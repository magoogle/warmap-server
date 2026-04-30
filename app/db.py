"""
SQLite index for the WarMap server.

Source of truth stays in files: NDJSON dumps in /data/dumps/, merged zone
JSONs in /data/data/zones/.  This DB is a derived, rebuildable index that
makes the per-uploader / per-session / per-actor queries fast.

Tables
------
keys      : minted API keys (replaces api_keys.json)
sessions  : one row per dump file we know about (header + last-sample summary)
actors    : aggregated actor catalog (zone, skin, rx, ry, floor) -> attrs
uploads   : append-only audit trail of upload events (who, when, what, bytes)

Concurrency
-----------
WAL mode + a single writer thread is sufficient for our workload (uploads
and merges happen serially per-key).  Reads can happen concurrently from
viewer endpoints without contention.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterable, Optional

_LOCK = threading.RLock()       # serialize all writes; reads are lock-free under WAL

SCHEMA = """
CREATE TABLE IF NOT EXISTS keys (
    name        TEXT PRIMARY KEY,
    key         TEXT NOT NULL UNIQUE,
    tier        TEXT NOT NULL,
    created_at  REAL NOT NULL,
    last_used   REAL NOT NULL DEFAULT 0,
    uploads     INTEGER NOT NULL DEFAULT 0,
    enabled     INTEGER NOT NULL DEFAULT 1,
    note        TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS sessions (
    name           TEXT PRIMARY KEY,
    client_id      TEXT NOT NULL,
    session_id     TEXT,
    zone           TEXT,
    world          TEXT,
    activity       TEXT,
    started_at     REAL,
    ended_at       REAL,
    last_sample_t  REAL,
    last_x         REAL,
    last_y         REAL,
    last_z         REAL,
    last_floor     INTEGER,
    sample_count   INTEGER NOT NULL DEFAULT 0,
    cell_count     INTEGER NOT NULL DEFAULT 0,
    actor_count    INTEGER NOT NULL DEFAULT 0,
    complete       INTEGER NOT NULL DEFAULT 0,
    size           INTEGER NOT NULL DEFAULT 0,
    mtime          REAL NOT NULL DEFAULT 0,
    received_at    REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS sessions_client ON sessions(client_id);
CREATE INDEX IF NOT EXISTS sessions_zone   ON sessions(zone);
CREATE INDEX IF NOT EXISTS sessions_mtime  ON sessions(mtime);

CREATE TABLE IF NOT EXISTS actors (
    zone               TEXT NOT NULL,
    skin               TEXT NOT NULL,
    rx                 INTEGER NOT NULL,
    ry                 INTEGER NOT NULL,
    floor              INTEGER NOT NULL,
    kind               TEXT,
    x                  REAL,
    y                  REAL,
    z                  REAL,
    type_id            INTEGER,
    sno_id             INTEGER,
    radius             REAL,
    is_boss            INTEGER,
    is_elite           INTEGER,
    sessions_seen      INTEGER NOT NULL DEFAULT 0,
    total_observations INTEGER NOT NULL DEFAULT 0,
    first_seen_at      REAL,
    last_seen_at       REAL,
    PRIMARY KEY (zone, skin, rx, ry, floor)
);
CREATE INDEX IF NOT EXISTS actors_zone ON actors(zone);
CREATE INDEX IF NOT EXISTS actors_kind ON actors(kind);

CREATE TABLE IF NOT EXISTS uploads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    client_id       TEXT NOT NULL,
    bytes           INTEGER,
    accepted_at     REAL NOT NULL,
    rejected_reason TEXT
);
CREATE INDEX IF NOT EXISTS uploads_client ON uploads(client_id);
CREATE INDEX IF NOT EXISTS uploads_time   ON uploads(accepted_at);
"""


class DB:
    """
    Thin wrapper around a single sqlite3 connection.  All public methods
    are safe to call from any thread (lock + WAL).
    """

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self.path),
            check_same_thread=False,
            timeout=10.0,
            isolation_level=None,    # autocommit; we manage transactions explicitly
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute('PRAGMA journal_mode=WAL')
        self._conn.execute('PRAGMA synchronous=NORMAL')
        self._conn.execute('PRAGMA foreign_keys=ON')
        self._conn.executescript(SCHEMA)

    # -------- low-level helpers --------
    @contextlib.contextmanager
    def write(self):
        with _LOCK:
            cur = self._conn.cursor()
            try:
                cur.execute('BEGIN IMMEDIATE')
                yield cur
                cur.execute('COMMIT')
            except Exception:
                cur.execute('ROLLBACK')
                raise
            finally:
                cur.close()

    def query(self, sql: str, params: Iterable = ()) -> list[sqlite3.Row]:
        with _LOCK:                                # ensure no torn reads during a write
            cur = self._conn.execute(sql, list(params))
            try:
                return cur.fetchall()
            finally:
                cur.close()

    def query_one(self, sql: str, params: Iterable = ()) -> Optional[sqlite3.Row]:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    # ============================================================
    # Keys
    # ============================================================
    def upsert_key(self, *, name: str, key: str, tier: str,
                   created_at: float, last_used: float = 0.0,
                   uploads: int = 0, enabled: bool = True, note: str = ''):
        with self.write() as c:
            c.execute("""
                INSERT INTO keys(name, key, tier, created_at, last_used,
                                 uploads, enabled, note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    key=excluded.key, tier=excluded.tier,
                    last_used=MAX(excluded.last_used, last_used),
                    uploads=MAX(excluded.uploads, uploads),
                    enabled=excluded.enabled, note=excluded.note
            """, (name, key, tier, created_at, last_used, uploads,
                  1 if enabled else 0, note))

    def find_key_by_secret(self, secret: str) -> Optional[sqlite3.Row]:
        if not secret:
            return None
        return self.query_one(
            'SELECT * FROM keys WHERE key = ? LIMIT 1', (secret,))

    def list_keys(self) -> list[sqlite3.Row]:
        return self.query('SELECT * FROM keys ORDER BY created_at DESC')

    def set_key_enabled(self, name_or_key: str, enabled: bool) -> Optional[sqlite3.Row]:
        with self.write() as c:
            c.execute("""
                UPDATE keys SET enabled = ?
                WHERE name = ? OR key = ?
            """, (1 if enabled else 0, name_or_key, name_or_key))
            if c.rowcount == 0:
                return None
        return self.query_one(
            'SELECT * FROM keys WHERE name = ? OR key = ?',
            (name_or_key, name_or_key))

    def remove_key(self, name_or_key: str) -> bool:
        with self.write() as c:
            c.execute('DELETE FROM keys WHERE name = ? OR key = ?',
                      (name_or_key, name_or_key))
            return c.rowcount > 0

    def bump_key_uploads(self, secret: str, n: int) -> None:
        if not secret or n <= 0:
            return
        with self.write() as c:
            c.execute("""
                UPDATE keys SET uploads = uploads + ?, last_used = ?
                WHERE key = ?
            """, (n, time.time(), secret))

    # ============================================================
    # Sessions (per-dump metadata)
    # ============================================================
    def upsert_session(self, **kw) -> None:
        kw.setdefault('received_at', time.time())
        cols = ['name','client_id','session_id','zone','world','activity',
                'started_at','ended_at','last_sample_t','last_x','last_y',
                'last_z','last_floor','sample_count','cell_count','actor_count',
                'complete','size','mtime','received_at']
        vals = [kw.get(c) for c in cols]
        # Convert booleans
        idx_complete = cols.index('complete')
        vals[idx_complete] = 1 if vals[idx_complete] else 0
        with self.write() as c:
            c.execute(f"""
                INSERT INTO sessions ({','.join(cols)})
                VALUES ({','.join(['?']*len(cols))})
                ON CONFLICT(name) DO UPDATE SET
                    client_id=excluded.client_id,
                    session_id=excluded.session_id,
                    zone=excluded.zone,
                    world=excluded.world,
                    activity=excluded.activity,
                    started_at=excluded.started_at,
                    ended_at=excluded.ended_at,
                    last_sample_t=excluded.last_sample_t,
                    last_x=excluded.last_x,
                    last_y=excluded.last_y,
                    last_z=excluded.last_z,
                    last_floor=excluded.last_floor,
                    sample_count=excluded.sample_count,
                    cell_count=excluded.cell_count,
                    actor_count=excluded.actor_count,
                    complete=excluded.complete,
                    size=excluded.size,
                    mtime=excluded.mtime
            """, vals)

    def remove_session(self, name: str) -> None:
        with self.write() as c:
            c.execute('DELETE FROM sessions WHERE name = ?', (name,))

    def list_sessions(self, *, client_id: Optional[str] = None,
                      zone: Optional[str] = None,
                      limit: int = 500) -> list[sqlite3.Row]:
        sql = 'SELECT * FROM sessions WHERE 1=1'
        params: list[Any] = []
        if client_id:
            sql += ' AND client_id = ?'
            params.append(client_id)
        if zone:
            sql += ' AND zone = ?'
            params.append(zone)
        sql += ' ORDER BY mtime DESC LIMIT ?'
        params.append(limit)
        return self.query(sql, params)

    def list_uploaders(self) -> list[sqlite3.Row]:
        """Aggregate sessions by client_id."""
        return self.query("""
            SELECT
                client_id,
                COUNT(*)                                 AS sessions,
                SUM(CASE WHEN complete=0 THEN 1 ELSE 0 END) AS in_progress,
                MAX(mtime)                               AS last_active,
                GROUP_CONCAT(DISTINCT zone)              AS zones_csv
            FROM sessions
            GROUP BY client_id
            ORDER BY last_active DESC
        """)

    # ============================================================
    # Uploads (audit trail)
    # ============================================================
    def record_upload(self, *, name: str, client_id: str, bytes_: int) -> None:
        with self.write() as c:
            c.execute("""
                INSERT INTO uploads(name, client_id, bytes, accepted_at)
                VALUES (?, ?, ?, ?)
            """, (name, client_id, bytes_, time.time()))

    def record_rejection(self, *, name: str, client_id: str, reason: str) -> None:
        with self.write() as c:
            c.execute("""
                INSERT INTO uploads(name, client_id, accepted_at, rejected_reason)
                VALUES (?, ?, ?, ?)
            """, (name, client_id, time.time(), reason))

    # ============================================================
    # Actors (curated catalog index, complementary to the zone JSONs)
    # ============================================================
    def replace_actors_for_zone(self, zone: str, rows: list[dict]) -> None:
        """Wipe + re-insert actors for one zone.  Called after each merge
        for that zone -- the JSON file is the source of truth, this is just
        a queryable mirror."""
        with self.write() as c:
            c.execute('DELETE FROM actors WHERE zone = ?', (zone,))
            for r in rows:
                c.execute("""
                    INSERT INTO actors(zone, skin, rx, ry, floor, kind, x, y, z,
                                       type_id, sno_id, radius, is_boss, is_elite,
                                       sessions_seen, total_observations,
                                       first_seen_at, last_seen_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    zone,
                    r.get('skin', ''),
                    int(round(r.get('x', 0))),
                    int(round(r.get('y', 0))),
                    int(r.get('floor', 1)),
                    r.get('kind'),
                    r.get('x'), r.get('y'), r.get('z'),
                    r.get('type_id'), r.get('sno_id'), r.get('radius'),
                    1 if r.get('is_boss') else (0 if r.get('is_boss') is False else None),
                    1 if r.get('is_elite') else (0 if r.get('is_elite') is False else None),
                    int(r.get('sessions_seen', 0)),
                    int(r.get('total_observations', 0)),
                    r.get('first_seen_at'),
                    r.get('last_seen_at'),
                ))
