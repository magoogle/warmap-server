"""
WarMap upload server.

A small FastAPI app that accepts NDJSON recorder dumps over HTTP from
distributed clients and runs the merger periodically to aggregate the
incoming data into curated per-zone files.

Endpoints
---------

POST  /upload                multipart/form-data; client uploads one or more
                             .ndjson session files.  Returns {accepted, rejected}.
GET   /status                summary of received dumps + last merge run.
GET   /coverage              proxy to data/zones/coverage.json (read-only).
GET   /saturated.json        proxy to saturated.json so clients can pull the
                             "skip probing for these zones" list.
GET   /zones/<key>           returns merged data/zones/<key>.json for a given
                             zone or pit-template key.
POST  /merge                 force an immediate merge run (admin).

Auth: every endpoint that mutates state (upload, merge) requires the
shared-secret header `X-WarMap-Key`. Read endpoints (`status`, `coverage`,
`saturated`, `zones/<key>`) are public so client-side recorders can pull
without needing the secret -- there's nothing private in the merged data.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.keys import KeyStore   # legacy fallback for migration only
from app.db   import DB

# ----- Paths (overridable via env for local dev) ---------------------------
ROOT      = Path(os.getenv('WARMAP_ROOT', '/data'))
DUMPS_DIR = ROOT / 'dumps'
DATA_DIR  = ROOT / 'data' / 'zones'
SIDECAR   = ROOT / 'sidecar'                # for saturated.json target
LOG_DIR   = ROOT / 'logs'

for d in (DUMPS_DIR, DATA_DIR, SIDECAR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ----- Auth ----------------------------------------------------------------
# Admin key (master) -- the operator's single key.  Mints/revokes per-friend
# uploader keys, can quarantine suspicious uploads.
ADMIN_KEY = os.getenv('WARMAP_API_KEY', '').strip()
if not ADMIN_KEY:
    print('WARNING: WARMAP_API_KEY not set; admin endpoints will be unusable.',
          file=sys.stderr)

DB_PATH = ROOT / 'warmap.db'
DBI = DB(DB_PATH)
QUARANTINE_DIR = ROOT / 'quarantine'
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)


# One-shot migration of the legacy api_keys.json into the keys table.
# Runs at every startup but is idempotent (UPSERT by name).
def _migrate_legacy_keys():
    legacy = ROOT / 'keys' / 'api_keys.json'
    if not legacy.exists():
        return
    try:
        ks = KeyStore(path=legacy, admin_key=ADMIN_KEY)
        for k in ks.list_uploader_keys():
            DBI.upsert_key(
                name=k.name, key=k.key, tier=k.tier,
                created_at=k.created_at, last_used=k.last_used,
                uploads=k.uploads, enabled=k.enabled, note=k.note,
            )
        legacy.rename(legacy.with_suffix('.json.migrated'))
        print(f'[startup] migrated keys from {legacy} -> SQLite')
    except Exception as e:
        print(f'[startup] key migration failed: {e}', file=sys.stderr)


_migrate_legacy_keys()


def _summarize_dump(path: Path) -> dict:
    """Header + last-sample + counts summary, shaped to fit DB.upsert_session.

    Walks the file once.  Cheap for our typical session sizes (KB-MB range).
    """
    name = path.name
    try:
        st = path.stat()
        size = st.st_size
        mtime = st.st_mtime
    except OSError:
        size, mtime = 0, 0.0
    info: dict = {
        'name':         name,
        'client_id':    name.split('__', 1)[0] if '__' in name else 'anon',
        'session_id':   None,
        'zone':         None, 'world': None, 'activity': None,
        'started_at':   None, 'ended_at': None, 'last_sample_t': None,
        'last_x':       None, 'last_y':   None, 'last_z':   None, 'last_floor': None,
        'sample_count': 0, 'cell_count': 0, 'actor_count': 0,
        'complete':     False, 'size': size, 'mtime': mtime,
    }
    try:
        with path.open('r', encoding='utf-8') as f:
            last_sample = None
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    o = json.loads(line)
                except json.JSONDecodeError:
                    continue
                t = o.get('type')
                if t == 'header':
                    info['session_id'] = o.get('session_id')
                    info['zone']       = o.get('zone')
                    info['world']      = o.get('world')
                    info['activity']   = o.get('activity_kind')
                    info['started_at'] = o.get('started_at')
                elif t == 'sample':
                    last_sample = o
                    info['sample_count'] += 1
                elif t == 'grid_cell':
                    info['cell_count'] += 1
                elif t == 'actor':
                    info['actor_count'] += 1
                elif t == 'footer':
                    info['complete'] = True
                    info['ended_at'] = o.get('ended_at')
            if last_sample:
                info['last_x']        = last_sample.get('x')
                info['last_y']        = last_sample.get('y')
                info['last_z']        = last_sample.get('z')
                info['last_floor']    = last_sample.get('floor')
                info['last_sample_t'] = last_sample.get('t')
    except OSError:
        pass
    return info


def _index_existing_dumps():
    n = 0
    for p in DUMPS_DIR.glob('*.ndjson'):
        try:
            DBI.upsert_session(**_summarize_dump(p))
            n += 1
        except Exception:
            pass
    if n:
        print(f'[startup] indexed {n} existing dumps')

_index_existing_dumps()

# ----- Merger import (the same module the local merger uses) ---------------
# We import lazily so the server starts even if the merger module has a
# small bug; you can hit `/health` and `/merge` reports the issue.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'merger'))


def _import_merger():
    """Import the bundled merge module (copied into the image at build)."""
    import merge  # type: ignore
    return merge


# ----- App ----------------------------------------------------------------
app = FastAPI(title='WarMap Upload Server', version='0.1')


# Global state for the background merger
_merge_lock = threading.Lock()
_last_merge: dict = {
    'started_at': None,
    'finished_at': None,
    'duration_s': None,
    'accepted': 0,
    'skipped': 0,
    'error': None,
}


# ----- Helpers -------------------------------------------------------------
class _AuthRec:
    """Minimal shape: name + tier + (raw) key, for endpoints to use."""
    __slots__ = ('name', 'tier', 'key')
    def __init__(self, name: str, tier: str, key: str):
        self.name, self.tier, self.key = name, tier, key


def _check_auth(provided: Optional[str]) -> _AuthRec:
    """Validate against the DB.  Master admin key (env) is recognized
    directly.  Otherwise look up in the keys table."""
    if not ADMIN_KEY:
        raise HTTPException(503, 'Server is not configured (no admin key set).')
    if provided == ADMIN_KEY:
        return _AuthRec(name='admin', tier='admin', key=provided)
    row = DBI.find_key_by_secret((provided or '').strip()) if provided else None
    if not row or not row['enabled']:
        raise HTTPException(401, 'Bad or missing X-WarMap-Key header.')
    return _AuthRec(name=row['name'], tier=row['tier'], key=row['key'])


def _check_admin(provided: Optional[str]) -> _AuthRec:
    rec = _check_auth(provided)
    if rec.tier != 'admin':
        raise HTTPException(403, 'Admin tier required.')
    return rec


def _safe_filename(name: str) -> str:
    """Allow only [A-Za-z0-9._-] and require .ndjson or .json suffix."""
    if '/' in name or '\\' in name or '..' in name:
        return ''
    if not (name.endswith('.ndjson') or name.endswith('.json')):
        return ''
    safe = ''.join(c for c in name if c.isalnum() or c in '._-')
    return safe if 8 <= len(safe) <= 200 else ''


def _run_merge() -> dict:
    """Run the merger against DUMPS_DIR -> DATA_DIR.  Also refreshes the
    SQLite index (sessions table + actors table) from the same scan."""
    if not _merge_lock.acquire(blocking=False):
        return {'skipped': True, 'reason': 'already running'}
    try:
        _last_merge['started_at'] = time.time()
        _last_merge['error'] = None
        try:
            merge = _import_merger()
            state = merge.merge_all(DUMPS_DIR, only_complete=False)
            merge.emit_all(state, DATA_DIR, SIDECAR)
            accepted = sum(len(agg.sessions) for agg in state.values())
            _last_merge['accepted'] = accepted
            _last_merge['skipped']  = 0

            # Refresh DB index off the same set of files the merger just read.
            # Cheap because we're only re-summarising headers + last samples.
            _refresh_db_from_disk(state)
        except Exception as e:
            _last_merge['error'] = str(e)
            return {'ok': False, 'error': str(e)}
        finally:
            _last_merge['finished_at'] = time.time()
            _last_merge['duration_s'] = (
                _last_merge['finished_at'] - _last_merge['started_at'])
        return {'ok': True, 'last_merge': _last_merge.copy()}
    finally:
        _merge_lock.release()


def _refresh_db_from_disk(merge_state: dict) -> None:
    """Update the SQLite index from the freshly merged state.

    sessions table : re-summarise every dump on disk (cheap).
    actors table   : per-zone, replace from merge_state's aggregated actors
                     (this is the canonical post-merge result).
    """
    # Sessions: walk dumps dir, upsert each
    seen = set()
    for p in DUMPS_DIR.glob('*.ndjson'):
        try:
            DBI.upsert_session(**_summarize_dump(p))
            seen.add(p.name)
        except Exception:
            pass
    # Drop sessions whose files were quarantined / deleted
    existing = {r['name'] for r in DBI.query('SELECT name FROM sessions')}
    for name in existing - seen:
        DBI.remove_session(name)

    # Actors: per-zone, replace from merge state
    for key, agg in merge_state.items():
        if not getattr(agg, 'actors', None):
            continue
        rows = []
        for actor_key, a in agg.actors.items():
            rows.append({
                'skin':    a.skin,
                'kind':    a.kind,
                'x':       a.x, 'y': a.y, 'z': a.z,
                'floor':   a.floor,
                'type_id': a.type_id,
                'sno_id':  a.sno_id,
                'radius':  a.radius,
                'is_boss': a.is_boss,
                'is_elite':a.is_elite,
                'sessions_seen':       len(a.sessions_seen) if hasattr(a, 'sessions_seen') else 0,
                'total_observations':  getattr(a, 'total_observations', 0),
                'first_seen_at':       getattr(a, 'first_seen_t', None),
                'last_seen_at':        None,
            })
        try:
            DBI.replace_actors_for_zone(key, rows)
        except Exception as e:
            print(f'[db] actor refresh for {key} failed: {e}', file=sys.stderr)


# ----- Background merger loop ---------------------------------------------
async def _merge_periodically(interval_s: int = 300):
    """Re-run the merger every interval_s seconds while the server is alive."""
    while True:
        try:
            await asyncio.sleep(interval_s)
            print(f'[scheduler] running periodic merge')
            _run_merge()
        except Exception as e:
            print(f'[scheduler] error: {e}', file=sys.stderr)


@app.on_event('startup')
async def on_startup():
    # Initial merge so clients can pull immediately if any data exists.
    threading.Thread(target=_run_merge, daemon=True).start()
    asyncio.create_task(_merge_periodically(interval_s=int(
        os.getenv('WARMAP_MERGE_INTERVAL_S', '300'))))


# ----- Routes --------------------------------------------------------------
# ----- Viewer (static) ----------------------------------------------------
# Tiny SPA that fetches /zones, /zones/<key>, /actor-index, /status and
# renders a live map.  Intended to be exposed only on a separate firewall-
# locked port (the public 30100 just keeps serving the existing endpoints
# without the viewer UI).
_VIEWER_DIR = Path(__file__).resolve().parent / 'viewer'
if _VIEWER_DIR.exists():
    app.mount('/viewer', StaticFiles(directory=str(_VIEWER_DIR), html=True),
              name='viewer')


@app.get('/')
def root():
    # Redirect bare hits to the viewer if it's available; otherwise show health.
    if _VIEWER_DIR.exists():
        return RedirectResponse('/viewer/')
    return {'status': 'ok', 'last_merge': _last_merge}


@app.get('/health')
def health():
    return {'status': 'ok', 'last_merge': _last_merge}


@app.get('/status')
def status():
    dumps = sorted(DUMPS_DIR.glob('*.ndjson')) + sorted(DUMPS_DIR.glob('*.json'))
    zones = sorted(DATA_DIR.glob('*.json'))
    return {
        'dumps_count':  len(dumps),
        'zones_count':  len([z for z in zones if not z.name.startswith('_')]),
        'last_merge':   _last_merge,
        'sample_zones': [z.stem for z in zones if not z.name.startswith('_')][:20],
    }


@app.get('/saturated.json')
def get_saturated():
    p = SIDECAR / 'saturated.json'
    if not p.exists():
        return JSONResponse({'updated_at': 0, 'zones': [], 'pit_worlds': []})
    return FileResponse(p, media_type='application/json')


@app.get('/coverage')
def get_coverage():
    p = DATA_DIR / 'coverage.json'
    if not p.exists():
        raise HTTPException(404, 'No merge has run yet.')
    return FileResponse(p, media_type='application/json')


@app.get('/actor-index')
def get_actor_index():
    p = DATA_DIR / '_actor_index.json'
    if not p.exists():
        raise HTTPException(404, 'No merge has run yet.')
    return FileResponse(p, media_type='application/json')


@app.get('/zones/{key}')
def get_zone(key: str):
    safe = _safe_filename(key + '.json')
    if not safe:
        raise HTTPException(400, 'Bad zone key.')
    p = DATA_DIR / safe
    if not p.exists():
        raise HTTPException(404, f'No data for zone {key}.')
    return FileResponse(p, media_type='application/json')


@app.get('/zones')
def list_zones():
    zones = sorted(DATA_DIR.glob('*.json'))
    return {'zones': [z.stem for z in zones if not z.name.startswith('_')]}


# ---------------------------------------------------------------------------
# Per-uploader / per-session visibility for the live viewer.
#
# All metadata queries go through the SQLite index now.  The dump files
# themselves remain the source of truth.
# ---------------------------------------------------------------------------

def _row_to_dict(r) -> dict:
    return {k: r[k] for k in r.keys()}


@app.get('/dumps')
def list_dumps(client_id: Optional[str] = None, zone: Optional[str] = None,
               limit: int = 500):
    rows = DBI.list_sessions(client_id=client_id, zone=zone, limit=limit)
    return {'dumps': [_row_to_dict(r) for r in rows], 'count': len(rows)}


@app.get('/dumps/{name}')
def get_dump(name: str):
    safe = _safe_filename(name)
    if not safe:
        raise HTTPException(400, 'Bad dump name.')
    p = DUMPS_DIR / safe
    if not p.exists():
        raise HTTPException(404, 'Not found.')
    return FileResponse(p, media_type='application/x-ndjson')


@app.get('/uploaders')
def list_uploaders():
    rows = DBI.list_uploaders()
    out = []
    for r in rows:
        zones_csv = r['zones_csv'] or ''
        zones = sorted({z for z in zones_csv.split(',') if z})
        out.append({
            'client_id':    r['client_id'],
            'sessions':     r['sessions'],
            'in_progress':  r['in_progress'],
            'last_active':  r['last_active'],
            'zones':        zones,
        })
    return {'uploaders': out, 'count': len(out)}


@app.post('/upload')
async def upload(
    files: list[UploadFile] = File(...),
    # client_id form field is now ignored (it was always spoofable). The
    # canonical uploader name comes from the authenticated key.  We accept
    # the field for backwards-compat with old clients.
    client_id: str = Form(default=''),
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    rec = _check_auth(x_warmap_key)
    canonical = ''.join(c for c in rec.name if c.isalnum() or c in '_-')[:32] or 'anon'
    accepted, rejected = [], []
    for f in files:
        name = _safe_filename(f.filename or '')
        if not name:
            rejected.append({'name': f.filename, 'reason': 'bad_name'})
            continue
        out_name = f'{canonical}__{name}'
        out_path = DUMPS_DIR / out_name
        size = 0
        try:
            with out_path.open('wb') as out:
                while True:
                    chunk = await f.read(64 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > 50 * 1024 * 1024:    # 50 MB cap per file
                        out_path.unlink(missing_ok=True)
                        raise HTTPException(413, f'{f.filename}: too large')
                    out.write(chunk)
            accepted.append({'name': out_name, 'bytes': size})
        except HTTPException:
            raise
        except Exception as e:
            rejected.append({'name': f.filename, 'reason': str(e)})

    # Record upload events + refresh session index for the new files
    for a in accepted:
        DBI.record_upload(name=a['name'], client_id=canonical, bytes_=a['bytes'])
        try:
            DBI.upsert_session(**_summarize_dump(DUMPS_DIR / a['name']))
        except Exception:
            pass
    if accepted:
        DBI.bump_key_uploads(rec.key, len(accepted))
    return {'accepted': accepted, 'rejected': rejected, 'as': canonical}


@app.post('/merge')
def merge_now(x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key')):
    _check_auth(x_warmap_key)
    return _run_merge()


# ---------------------------------------------------------------------------
# Admin: key management + quarantine.  All require the master ADMIN_KEY.
# ---------------------------------------------------------------------------

class MintRequest(BaseModel):
    name: str
    note: str = ''


@app.get('/admin/keys')
def admin_list_keys(x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key')):
    _check_admin(x_warmap_key)
    return {'keys': [{
        'name':       r['name'],
        'key':        r['key'],
        'tier':       r['tier'],
        'created_at': r['created_at'],
        'last_used':  r['last_used'],
        'uploads':    r['uploads'],
        'enabled':    bool(r['enabled']),
        'note':       r['note'],
    } for r in DBI.list_keys()]}


@app.post('/admin/keys')
def admin_mint_key(
    body: MintRequest,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    _check_admin(x_warmap_key)
    name = (body.name or '').strip()
    if not name:
        raise HTTPException(400, 'name required')
    # Idempotent on name: if a key already exists with this name, return it
    existing = DBI.query_one('SELECT * FROM keys WHERE name = ?', (name,))
    if existing:
        return {
            'name': existing['name'], 'key': existing['key'],
            'tier': existing['tier'], 'created_at': existing['created_at'],
            'note': existing['note'],
        }
    import secrets
    new_key = secrets.token_hex(32)
    DBI.upsert_key(
        name=name, key=new_key, tier='uploader',
        created_at=time.time(), note=body.note,
    )
    return {'name': name, 'key': new_key, 'tier': 'uploader',
            'created_at': time.time(), 'note': body.note}


@app.post('/admin/keys/{name}/disable')
def admin_disable_key(
    name: str,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    _check_admin(x_warmap_key)
    r = DBI.set_key_enabled(name, False)
    if not r: raise HTTPException(404, 'No such key.')
    return {'ok': True, 'name': r['name'], 'enabled': bool(r['enabled'])}


@app.post('/admin/keys/{name}/enable')
def admin_enable_key(
    name: str,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    _check_admin(x_warmap_key)
    r = DBI.set_key_enabled(name, True)
    if not r: raise HTTPException(404, 'No such key.')
    return {'ok': True, 'name': r['name'], 'enabled': bool(r['enabled'])}


@app.delete('/admin/keys/{name}')
def admin_delete_key(
    name: str,
    keep_data: bool = False,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    """Delete a key.  By default cascades: also wipes the uploader's
    sessions + dump files so they vanish from the Uploaders tab.  Pass
    ?keep_data=true to revoke the key but keep their historical
    contributions (rare; equivalent to old behavior)."""
    _check_admin(x_warmap_key)
    if not DBI.remove_key(name):
        raise HTTPException(404, 'No such key.')
    if keep_data:
        return {'ok': True, 'removed': name, 'cascade': False}

    # Cascade: remove their dump files + sessions rows.
    safe_name = ''.join(c for c in name if c.isalnum() or c in '_-')[:32]
    if safe_name:
        rows = DBI.list_sessions(client_id=safe_name, limit=10000)
        deleted_files = 0
        for r in rows:
            DBI.remove_session(r['name'])
            p = DUMPS_DIR / r['name']
            try:
                if p.exists():
                    p.unlink()
                    deleted_files += 1
            except OSError:
                pass
        # Re-merge so zone JSONs and the actors index drop their contributions.
        threading.Thread(target=_run_merge, daemon=True).start()
        return {
            'ok': True, 'removed': name, 'cascade': True,
            'sessions_removed': len(rows),
            'files_deleted':    deleted_files,
        }
    return {'ok': True, 'removed': name, 'cascade': True,
            'sessions_removed': 0, 'files_deleted': 0}


@app.post('/admin/quarantine/{name}')
def admin_quarantine_dump(
    name: str,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    """Move a single dump file out of dumps/ into quarantine/ so it stops
    being merged.  Useful when an uploader ships obvious junk."""
    _check_admin(x_warmap_key)
    safe = _safe_filename(name)
    if not safe:
        raise HTTPException(400, 'Bad name.')
    src = DUMPS_DIR / safe
    if not src.exists():
        raise HTTPException(404, 'Dump not found.')
    dst = QUARANTINE_DIR / safe
    shutil.move(str(src), str(dst))
    return {'ok': True, 'moved_to': str(dst)}


@app.post('/admin/quarantine_uploader/{name}')
def admin_quarantine_uploader(
    name: str,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    """Move EVERY dump from a given uploader to quarantine and disable
    their key.  Use when someone is uploading garbage at scale."""
    _check_admin(x_warmap_key)
    safe_name = ''.join(c for c in name if c.isalnum() or c in '_-')[:32]
    if not safe_name:
        raise HTTPException(400, 'Bad uploader name.')
    moved = []
    for p in list(DUMPS_DIR.glob(f'{safe_name}__*.ndjson')) + list(DUMPS_DIR.glob(f'{safe_name}__*.json')):
        dst = QUARANTINE_DIR / p.name
        shutil.move(str(p), str(dst))
        moved.append(p.name)
        DBI.remove_session(p.name)
    DBI.set_key_enabled(safe_name, False)
    return {'ok': True, 'uploader': safe_name, 'quarantined': moved, 'count': len(moved)}


@app.get('/admin/quarantine')
def admin_list_quarantine(
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    _check_admin(x_warmap_key)
    items = []
    for p in sorted(QUARANTINE_DIR.glob('*'), key=lambda x: x.stat().st_mtime, reverse=True):
        if p.is_file():
            items.append({
                'name':  p.name,
                'size':  p.stat().st_size,
                'mtime': p.stat().st_mtime,
            })
    return {'items': items, 'count': len(items)}
