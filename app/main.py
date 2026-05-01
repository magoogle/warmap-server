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
import contextlib
import json
import os
import shutil
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

# fcntl is unix-only; the deployment target is Docker on Linux so this is
# always available in production.  We fall back to a no-op shim on Windows
# so local dev imports still work (single-worker dev doesn't need cross-
# process locking anyway).
try:
    import fcntl
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False

from fastapi import FastAPI, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

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
    """Re-index any dumps the DB doesn't already have at their current
    mtime.  Critical that this is FAST: 12 uvicorn workers each run
    this on import, so a 1000-dump folder used to mean 12k full NDJSON
    parses on container start (each worker walking the entire dumps
    dir + parsing each file end-to-end via _summarize_dump).  With
    the upsert_session adding a SELECT-before-INSERT for the
    re-upload-mtime fix, the per-call cost grew further and pushed
    cold start past 1 minute -- workers were still indexing while the
    HTTP port stayed unbound, so the server appeared down.

    Fast path now: read all (name, mtime) pairs out of the DB up
    front in one query, then skip _summarize_dump for any dump whose
    file-system mtime matches what's already there.  Only newly-
    uploaded or modified dumps pay the parse cost on startup.
    """
    try:
        existing = DBI.list_session_mtimes()    # {name -> mtime}
    except Exception as e:
        print(f'[startup] could not preload session mtimes: {e}', file=sys.stderr)
        existing = {}
    parsed = 0
    skipped = 0
    for p in DUMPS_DIR.glob('*.ndjson'):
        try:
            mtime = p.stat().st_mtime
        except OSError:
            continue
        prev_mt = existing.get(p.name)
        # Tolerate sub-millisecond float jitter from filesystem stat.
        if prev_mt is not None and abs(prev_mt - mtime) < 0.01:
            skipped += 1
            continue
        try:
            DBI.upsert_session(**_summarize_dump(p))
            parsed += 1
        except Exception:
            pass
    if parsed or skipped:
        print(f'[startup] indexed {parsed} new/changed dumps '
              f'(skipped {skipped} unchanged)')

_index_existing_dumps()

# ----- Merger import (the same module the local merger uses) ---------------
# We import lazily so the server starts even if the merger module has a
# small bug; you can hit `/health` and `/merge` reports the issue.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'merger'))


def _import_merger():
    """Import the bundled merge module (copied into the image at build)."""
    import merge  # type: ignore
    return merge


# ----- Rate limiter --------------------------------------------------------
# Per-IP rate limiting via slowapi.  In-memory backend (per uvicorn worker),
# which means with 4 workers an attacker hitting at exactly the limit can
# distribute across workers and effectively get 4x through.  Acceptable
# for our threat model -- limits exist to deter casual scrapers and runaway
# friend scripts, not to mitigate DDoS.
#
# Admin requests share the same per-IP buckets as everyone else.  The
# limits are sized so the operator's own scripts never realistically
# trip them: 120/min reads, 60/min uploads, 30/min admin endpoints.
# slowapi's exempt_when callable doesn't get a Request handle, so a
# clean per-key bypass would require a contextvar shim -- not worth it
# for a self-hosted-by-friend server with this traffic shape.
#
# Limits chosen for typical use:
#   /upload                    60/min   (multipart, server cost)
#   /zones, /zones/{key}, etc  120/min  (~2/s sustained, plenty for friend syncs)
#   /admin/*                    30/min   (rare ops, tight ceiling)
def _real_client_ip(request: Request) -> str:
    """Return the actual remote client IP, even when sitting behind a
    proxy chain like Cloudflare -> pfSense HAProxy -> warmap-server.

    Header priority (highest to lowest trust):
      1. CF-Connecting-IP -- set by Cloudflare's edge.  Authoritative for
         requests proxied through CF (orange cloud).  We trust this only
         because origin is firewalled to CF's IP ranges; if you ever
         expose the origin directly, drop CF-Connecting-IP from the list
         since it can be spoofed.
      2. X-Real-IP -- set by pfSense HAProxy (`http-request add-header
         X-Real-IP %[src]`).  Reflects HAProxy's view of the source,
         which is CF's edge IP when going through CF.  Useful as a
         fallback when CF-Connecting-IP is missing (direct LAN test
         hits, etc.).
      3. X-Forwarded-For first hop -- HAProxy's `option forwardfor` adds
         this; same data as X-Real-IP in our setup.
      4. request.client.host -- raw TCP source.  Will be the docker
         bridge IP (172.20.0.1) for proxied requests, useless for
         per-client rate limiting.

    The chosen value is what slowapi keys per-IP rate limit buckets on.
    """
    h = request.headers
    cf = h.get('cf-connecting-ip')
    if cf:
        return cf.strip()
    xri = h.get('x-real-ip')
    if xri:
        return xri.strip()
    xff = h.get('x-forwarded-for')
    if xff:
        # First entry in the comma-separated list is the original client.
        return xff.split(',', 1)[0].strip()
    return get_remote_address(request)


_LIMITER = Limiter(
    key_func=_real_client_ip,
    default_limits=[],                  # apply per-route via decorators
    storage_uri='memory://',
)


# ----- App ----------------------------------------------------------------
app = FastAPI(title='WarMap Upload Server', version='0.2')
app.state.limiter = _LIMITER
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# (Was: GZipMiddleware compressing every response.  Replaced with
# pre-compressed companion files for /zones/{key} -- the merger writes
# both <key>.json and <key>.json.gz so the endpoint serves the gz
# directly with `Content-Encoding: gzip`.  Avoids paying gzip CPU on
# every request; under load -- multiple uploaders pulling the catalog
# in parallel -- this dropped the server CPU from 100% pegged to ~10%.)


# Global state for the background merger.
#
# In-process lock: prevents two threads in the same worker from racing
# (e.g. /merge POST during the periodic loop's tick).
# Cross-process lock: a flock on a marker file under ROOT, used so that
# multiple uvicorn workers can't fire concurrent merges over the same
# DUMPS_DIR / DATA_DIR.  See _run_merge below.
_merge_lock = threading.Lock()
_MERGE_LOCK_PATH = ROOT / 'merge.lock'
_SCHEDULER_LOCK_PATH = ROOT / 'merge_scheduler.lock'

_last_merge: dict = {
    'started_at': None,
    'finished_at': None,
    'duration_s': None,
    'accepted': 0,
    'skipped': 0,
    'error': None,
}


@contextlib.contextmanager
def _file_lock(path: Path, blocking: bool = False):
    """Cross-process advisory lock via fcntl.flock.  Yields the open file
    handle on acquire, or None if non-blocking and another process holds
    it.  No-op (always yields a sentinel) on platforms without fcntl --
    fine for local dev where only one process is running.
    """
    if not _HAS_FCNTL:
        yield True
        return
    f = open(path, 'a+')
    try:
        flags = fcntl.LOCK_EX
        if not blocking:
            flags |= fcntl.LOCK_NB
        try:
            fcntl.flock(f.fileno(), flags)
        except BlockingIOError:
            f.close()
            yield None
            return
        try:
            yield f
        finally:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
    finally:
        try:
            f.close()
        except Exception:
            pass


# ----- Helpers -------------------------------------------------------------
class _AuthRec:
    """Minimal shape: name + tier + (raw) key, for endpoints to use."""
    __slots__ = ('name', 'tier', 'key')
    def __init__(self, name: str, tier: str, key: str):
        self.name, self.tier, self.key = name, tier, key


_TIERS_ALL          = frozenset({'admin', 'uploader', 'reader'})
_TIERS_UPLOAD       = frozenset({'admin', 'uploader'})  # readers can't upload
_TIERS_READ         = _TIERS_ALL                         # any valid key reads


def _check_auth(provided: Optional[str],
                allowed_tiers: frozenset = _TIERS_ALL) -> _AuthRec:
    """Validate against the DB.  Master admin key (env) is recognized
    directly.  Otherwise look up in the keys table.  When `allowed_tiers`
    is restricted, also gate by tier -- e.g. /upload passes
    _TIERS_UPLOAD so reader-tier keys get a 403 instead of polluting
    the dump dir."""
    if not ADMIN_KEY:
        raise HTTPException(503, 'Server is not configured (no admin key set).')
    if provided == ADMIN_KEY:
        rec = _AuthRec(name='admin', tier='admin', key=provided)
    else:
        row = DBI.find_key_by_secret((provided or '').strip()) if provided else None
        if not row or not row['enabled']:
            raise HTTPException(401, 'Bad or missing X-WarMap-Key header.')
        rec = _AuthRec(name=row['name'], tier=row['tier'], key=row['key'])
    if rec.tier not in allowed_tiers:
        raise HTTPException(403,
            f"Tier '{rec.tier}' not permitted; need one of {sorted(allowed_tiers)}.")
    return rec


def _check_admin(provided: Optional[str]) -> _AuthRec:
    return _check_auth(provided, allowed_tiers=frozenset({'admin'}))


def _safe_filename(name: str) -> str:
    """Allow only [A-Za-z0-9._-] and require .ndjson or .json suffix."""
    if '/' in name or '\\' in name or '..' in name:
        return ''
    if not (name.endswith('.ndjson') or name.endswith('.json')):
        return ''
    safe = ''.join(c for c in name if c.isalnum() or c in '._-')
    return safe if 8 <= len(safe) <= 200 else ''


def _conditional_file_response(path: Path, request: Request, media_type: str,
                               content_encoding: Optional[str] = None):
    """FileResponse with proper conditional-GET handling.

    Starlette's FileResponse advertises Last-Modified + ETag but doesn't
    actually compare them against If-Modified-Since / If-None-Match -- so
    polling clients (StaticPather's fetcher, the viewer) re-download the
    full body even when nothing has changed.  This helper does the
    comparison and returns 304 with no body when the client already has
    the current version.

    `content_encoding` lets callers serve a pre-compressed companion file
    (e.g. zone.json.gz) directly with `Content-Encoding: gzip` -- avoids
    runtime gzip CPU cost.  ETag is derived from the file we actually
    serve so cache validation works correctly per-encoding.
    """
    import email.utils
    import hashlib

    st = path.stat()
    # Cheap, stable ETag derived from (mtime, size).  Same file -> same etag.
    # Including the encoding tag means a client that switches Accept-Encoding
    # gets a different ETag and won't see a stale cached body.
    enc_tag = ('-' + content_encoding) if content_encoding else ''
    etag = '"{}"'.format(
        hashlib.md5(f'{st.st_mtime_ns}-{st.st_size}{enc_tag}'.encode()).hexdigest())
    last_modified = email.utils.formatdate(st.st_mtime, usegmt=True)

    inm = request.headers.get('if-none-match')
    ims = request.headers.get('if-modified-since')

    not_modified = False
    if inm and inm.strip() == etag:
        not_modified = True
    elif ims:
        try:
            ims_ts = email.utils.parsedate_to_datetime(ims).timestamp()
            # Compare to second resolution -- HTTP-date is whole-second.
            if int(ims_ts) >= int(st.st_mtime):
                not_modified = True
        except (TypeError, ValueError):
            pass

    if not_modified:
        return Response(status_code=304, headers={
            'ETag': etag,
            'Last-Modified': last_modified,
        })

    headers = {'ETag': etag, 'Last-Modified': last_modified}
    if content_encoding:
        headers['Content-Encoding'] = content_encoding
    return FileResponse(path, media_type=media_type, headers=headers)


def _run_merge() -> dict:
    """Run the merger against DUMPS_DIR -> DATA_DIR.  Also refreshes the
    SQLite index (sessions table + actors table) from the same scan.

    Locking is two-layer:
      * In-process threading.Lock: stops two threads in the SAME worker
        from racing (e.g. POST /merge while the scheduler tick fires).
      * Cross-process fcntl flock: stops two uvicorn WORKERS from running
        the merger simultaneously over the same DUMPS_DIR / DATA_DIR.
    Both are non-blocking; if either is held we report skipped and return.
    """
    if not _merge_lock.acquire(blocking=False):
        return {'skipped': True, 'reason': 'already running (in-process)'}
    try:
        with _file_lock(_MERGE_LOCK_PATH, blocking=False) as lk:
            if lk is None:
                return {'skipped': True, 'reason': 'already running (other worker)'}
            _last_merge['started_at'] = time.time()
            _last_merge['error'] = None
            try:
                merge = _import_merger()
                # Refresh the merger's dynamic ignore-list from the DB so
                # admin-added patterns apply on this cycle.  Cheap (a
                # single SELECT) and safely no-ops when the list hasn't
                # changed.
                try:
                    merge.set_dynamic_ignore(DBI.list_ignore_pattern_strings())
                except Exception:
                    pass
                state = merge.merge_all(DUMPS_DIR, only_complete=False)
                # Selective emit: when the merger ran in incremental mode
                # it tells us which zone keys were touched this cycle, so
                # we only re-emit those .json files (instead of all 90+
                # every cycle).  On a full rebuild last_touched_keys
                # equals state.keys() so emit_all writes everything.
                touched = getattr(merge.merge_all, 'last_touched_keys', None)
                merge_mode = getattr(merge.merge_all, 'last_mode', '')
                merge.emit_all(state, DATA_DIR, SIDECAR, only_keys=touched)
                accepted = sum(len(agg.sessions) for agg in state.values())
                _last_merge['accepted'] = accepted
                _last_merge['skipped']  = 0
                _last_merge['mode']     = merge_mode
                _last_merge['touched']  = len(touched) if touched else 0

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
                # Persist to disk so the OTHER 11 uvicorn workers (who
                # never run the merge themselves -- only the scheduler-
                # lock holder does) can show the same fresh last-merge
                # info on /status.  Without this, /status was returning
                # null finished_at for any request that happened to hit
                # a non-scheduler worker.
                _persist_last_merge()
            return {'ok': True, 'last_merge': _last_merge.copy()}
    finally:
        _merge_lock.release()


_LAST_MERGE_FILE = ROOT / 'last_merge.json'


def _persist_last_merge() -> None:
    """Write _last_merge to disk so non-scheduler workers can read it
    on /status.  Atomic via tmp + rename.  Cheap (<1KB)."""
    try:
        tmp = _LAST_MERGE_FILE.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(_last_merge), encoding='utf-8')
        tmp.replace(_LAST_MERGE_FILE)
    except OSError:
        pass


def _read_last_merge() -> dict:
    """/status helper -- prefer the worker's own _last_merge if it has
    a finished_at (means THIS worker ran the merge), else fall back to
    the shared on-disk copy."""
    if _last_merge.get('finished_at'):
        return _last_merge
    try:
        return json.loads(_LAST_MERGE_FILE.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return _last_merge


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
    """Re-run the merger every interval_s seconds while the server is alive.

    Multi-worker safety: at most ONE worker runs this loop.  We try to grab
    an exclusive flock on _SCHEDULER_LOCK_PATH non-blocking; the worker
    that wins holds it for the lifetime of the process.  The losers
    return immediately.  If the winning worker crashes, its lock is
    released by the kernel and the next merge cycle a survivor will pick
    it up on the following startup (or whenever the supervisor restarts
    it).  Without this gate, N workers would each fire the merge every
    interval_s -> N concurrent merge attempts, all but one rejected by
    the per-merge flock but still spamming the logs.
    """
    if _HAS_FCNTL:
        f = open(_SCHEDULER_LOCK_PATH, 'a+')
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            f.close()
            print(f'[scheduler] another worker holds the scheduler lock; standing down (pid={os.getpid()})')
            return
        # Hold the file for the lifetime of this task.  Reference kept on
        # the function via a closure-attached attr so the GC doesn't close
        # the fd from under us.
        _merge_periodically._lock_handle = f  # type: ignore[attr-defined]
        print(f'[scheduler] running periodic merge every {interval_s}s (pid={os.getpid()})')

    while True:
        try:
            await asyncio.sleep(interval_s)
            _run_merge()
        except Exception as e:
            print(f'[scheduler] error: {e}', file=sys.stderr)


@app.on_event('startup')
async def on_startup():
    # Initial merge so clients can pull immediately if any data exists.
    # _run_merge is itself flock-gated, so multiple workers all firing this
    # at startup is safe -- only the first one through actually merges.
    threading.Thread(target=_run_merge, daemon=True).start()
    asyncio.create_task(_merge_periodically(interval_s=int(
        os.getenv('WARMAP_MERGE_INTERVAL_S', '300'))))


# ----- Routes --------------------------------------------------------------
# ----- Viewer (static + dynamic index) ------------------------------------
# Tiny SPA that fetches /zones, /zones/<key>, /actor-index, /status and
# renders a live map.
#
# Caching strategy
# ----------------
# index.html is served via a tiny dynamic route that injects cache-busting
# query strings (?v=<file_mtime>) on the css/js asset references it pulls
# in.  That way:
#   * the HTML itself is always no-cache (so the latest version-tags ride)
#   * the .css/.js are immutable for a given version-tag, can be cached
#     forever -- but a deploy bumps the file mtime which bumps the tag,
#     so the browser sees a different URL and fetches fresh.
#
# The static files themselves still ride a wrapped StaticFiles mount, but
# we tell CF + browsers to honor the etag (validate on each request)
# rather than rely on TTL.
class _NoCacheStaticFiles(StaticFiles):
    async def get_response(self, path, scope):
        resp = await super().get_response(path, scope)
        try:
            resp.headers['Cache-Control'] = 'no-cache, must-revalidate, max-age=0'
            resp.headers['Pragma']        = 'no-cache'
            resp.headers['Expires']       = '0'
        except Exception:
            pass
        return resp


_VIEWER_DIR  = Path(__file__).resolve().parent / 'viewer'
_VIEWER_TPL  = _VIEWER_DIR / 'index.html'

def _viewer_asset_version() -> str:
    """Compute a cache-busting tag from the css/js file mtimes.  Bump on
    every deploy that changes either file -> browser fetches fresh."""
    parts = []
    for fname in ('viewer.css', 'viewer.js'):
        p = _VIEWER_DIR / fname
        try:
            parts.append(str(int(p.stat().st_mtime)))
        except OSError:
            parts.append('0')
    return '-'.join(parts)


if _VIEWER_DIR.exists():
    # IMPORTANT: register the explicit /viewer + /viewer/ routes BEFORE
    # the catch-all mount.  Starlette resolves routes in registration
    # order; if the mount goes first, it intercepts /viewer/ and our
    # cache-busting handler never runs.
    @app.get('/viewer/', include_in_schema=False)
    @app.get('/viewer',  include_in_schema=False)
    def _viewer_index():
        try:
            html = _VIEWER_TPL.read_text(encoding='utf-8')
        except OSError:
            raise HTTPException(500, 'viewer template missing')
        v = _viewer_asset_version()
        # Inject ?v=<tag> on asset references.  Idempotent if the strings
        # are unique enough (they are -- bare "viewer.css" / "viewer.js"
        # only appear in the link/script tags).
        html = html.replace('viewer.css',     f'viewer.css?v={v}')
        html = html.replace('viewer.js',      f'viewer.js?v={v}')
        return Response(
            content=html, media_type='text/html; charset=utf-8',
            headers={
                'Cache-Control': 'no-cache, must-revalidate, max-age=0',
                'Pragma':        'no-cache',
                'Expires':       '0',
            },
        )

    # Static mount for everything under /viewer/* (the .css, .js, etc.).
    # html=False so the mount doesn't try to auto-resolve index.html --
    # the explicit route above handles that.
    app.mount('/viewer', _NoCacheStaticFiles(directory=str(_VIEWER_DIR), html=False),
              name='viewer')


@app.get('/')
def root():
    # Redirect bare hits to the viewer if it's available; otherwise show health.
    if _VIEWER_DIR.exists():
        return RedirectResponse('/viewer/')
    return {'status': 'ok', 'last_merge': _read_last_merge()}


@app.get('/health')
def health():
    return {'status': 'ok', 'last_merge': _read_last_merge()}


@app.get('/status')
def status():
    dumps = sorted(DUMPS_DIR.glob('*.ndjson')) + sorted(DUMPS_DIR.glob('*.json'))
    zones = sorted(DATA_DIR.glob('*.json'))
    return {
        'dumps_count':  len(dumps),
        'zones_count':  len([z for z in zones if not z.name.startswith('_')]),
        # Read from disk-shared copy so non-scheduler workers also have
        # a fresh last-merge timestamp -- otherwise /status flickered
        # between 'X s ago' (scheduler worker) and 'never' (everyone
        # else) depending on which worker handled the request.
        'last_merge':   _read_last_merge(),
        'sample_zones': [z.stem for z in zones if not z.name.startswith('_')][:20],
    }


# ---------------------------------------------------------------------------
# Read endpoints -- all gated behind a valid X-WarMap-Key (any tier:
# admin/uploader/reader) and rate-limited per IP.  /health is the only
# unauthenticated endpoint, so monitoring / load-balancer probes still work.
#
# Why gate reads:  the merged map data is shared with friends via reader-
# tier keys.  Public reads would let anyone scrape the full catalog without
# any way to revoke access if a key holder misbehaves.
#
# All requests (admin included) share the same per-IP rate buckets; the
# limits are loose enough that the operator's own tooling won't trip them.
# ---------------------------------------------------------------------------

@app.get('/saturated.json')
@_LIMITER.limit('120/minute')
def get_saturated(request: Request,
                  x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key')):
    _check_auth(x_warmap_key, allowed_tiers=_TIERS_READ)
    p = SIDECAR / 'saturated.json'
    if not p.exists():
        return JSONResponse({'updated_at': 0, 'zones': [], 'pit_worlds': []})
    return FileResponse(p, media_type='application/json')


@app.get('/whoami')
@_LIMITER.limit('120/minute')
def whoami(request: Request,
           x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key')):
    """Reports the caller's identity + tier so the viewer can gate UI
    (e.g. only show the Admin button to admin keys, only show the
    Uploaders tab to admin/uploader keys, etc.).

    Returns 401 if no/bad key, otherwise:
        { name: <key-name-or-'admin'>, tier: 'admin'|'uploader'|'reader' }
    """
    rec = _check_auth(x_warmap_key, allowed_tiers=_TIERS_READ)
    return {'name': rec.name, 'tier': rec.tier}


@app.get('/ignore-list')
@_LIMITER.limit('120/minute')
def get_ignore_list(request: Request,
                    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key')):
    """Public-to-authenticated-callers list of dynamic ignore patterns
    that the recorder + merger should use IN ADDITION to their hardcoded
    static lists.  The uploader fetches this periodically and writes it
    to the recorder's core/ignore_dynamic.lua so a freshly-loaded
    recorder picks up admin-added ignores without a code change."""
    _check_auth(x_warmap_key, allowed_tiers=_TIERS_READ)
    return {'patterns': DBI.list_ignore_pattern_strings()}


@app.get('/coverage')
@_LIMITER.limit('120/minute')
def get_coverage(request: Request,
                 x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key')):
    _check_auth(x_warmap_key, allowed_tiers=_TIERS_READ)
    p = DATA_DIR / 'coverage.json'
    if not p.exists():
        raise HTTPException(404, 'No merge has run yet.')
    return FileResponse(p, media_type='application/json')


@app.get('/actor-index')
@_LIMITER.limit('120/minute')
def get_actor_index(request: Request,
                    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key')):
    _check_auth(x_warmap_key, allowed_tiers=_TIERS_READ)
    p = DATA_DIR / '_actor_index.json'
    if not p.exists():
        raise HTTPException(404, 'No merge has run yet.')
    return FileResponse(p, media_type='application/json')


@app.get('/zones/{key}')
@_LIMITER.limit('120/minute')
def get_zone(key: str, request: Request,
             x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key')):
    _check_auth(x_warmap_key, allowed_tiers=_TIERS_READ)
    safe = _safe_filename(key + '.json')
    if not safe:
        raise HTTPException(400, 'Bad zone key.')
    p = DATA_DIR / safe
    if not p.exists():
        raise HTTPException(404, f'No data for zone {key}.')

    # Pre-compressed companion file (written by the merger).  When the
    # client accepts gzip AND a fresh .json.gz exists, serve that with
    # `Content-Encoding: gzip` -- avoids paying gzip CPU cost on every
    # request.  This is a 4-5x throughput win on a busy server with
    # multiple uploaders pulling the zone catalog in parallel.
    accept = (request.headers.get('accept-encoding') or '').lower()
    if 'gzip' in accept:
        gz = p.with_suffix('.json.gz')
        if gz.exists():
            try:
                gz_mtime = gz.stat().st_mtime
                json_mtime = p.stat().st_mtime
                # Only serve the pre-compressed copy if it's at least as
                # fresh as the source JSON.  If a manual JSON edit went
                # past the merger's gzip step, fall through to runtime.
                if gz_mtime >= json_mtime:
                    return _conditional_file_response(
                        gz, request,
                        media_type='application/json',
                        content_encoding='gzip',
                    )
            except OSError:
                pass

    return _conditional_file_response(p, request, media_type='application/json')


@app.get('/zones/{key}/nav')
@_LIMITER.limit('120/minute')
def get_zone_nav(key: str, request: Request,
                 x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key')):
    """Navigation-only variant of /zones/{key}, consumed by WarPath
    when it actually needs cell data (smooth_path / wall-distance
    BFS).  Strips:
      * non-walkable cells (only walkable kept)
      * per-cell vote metadata (conf, total)
      * leaving each cell as a 2-tuple [cx, cy]

    Result: ~40-60% smaller than the full /zones/{key} payload while
    carrying everything WarPath needs.  The viewer continues to use
    the full endpoint since it renders blocked cells too.
    """
    _check_auth(x_warmap_key, allowed_tiers=_TIERS_READ)
    safe = _safe_filename(key + '.nav.json')
    if not safe:
        raise HTTPException(400, 'Bad zone key.')
    p = DATA_DIR / safe
    if not p.exists():
        raise HTTPException(404, f'No nav data for zone {key}.')

    accept = (request.headers.get('accept-encoding') or '').lower()
    if 'gzip' in accept:
        gz = p.with_suffix('.json.gz')
        if gz.exists():
            try:
                if gz.stat().st_mtime >= p.stat().st_mtime:
                    return _conditional_file_response(
                        gz, request,
                        media_type='application/json',
                        content_encoding='gzip',
                    )
            except OSError:
                pass
    return _conditional_file_response(p, request, media_type='application/json')


@app.get('/zones/{key}/meta')
@_LIMITER.limit('120/minute')
def get_zone_meta(key: str, request: Request,
                  x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key')):
    """Slim variant of /zones/{key} -- everything except grid.floors
    cell data (which dominates file size for big zones).  Consumed by
    WarPath for fast zone-change loads:  parsing the full file in pure
    Lua takes ~500ms on big overworld zones (Hawe_Verge) and feels
    like the game crashed; the meta variant typically parses in
    <30ms.  WarPath lazy-loads the full file via the regular
    /zones/{key} endpoint only when wall-distance smoothing is
    actually needed (rare).

    Format: same JSON schema as /zones/{key} but with
    grid.floors[fid] = []  (empty per floor).  A `cells_omitted`:true
    flag is added at the top level so consumers can confirm they got
    the slim variant.  Per-floor cell counts are surfaced inside
    grid.floors_meta[fid].cell_count for status-display use.
    """
    _check_auth(x_warmap_key, allowed_tiers=_TIERS_READ)
    safe = _safe_filename(key + '.meta.json')
    if not safe:
        raise HTTPException(400, 'Bad zone key.')
    p = DATA_DIR / safe
    if not p.exists():
        # Old deploys with no meta files: 404 lets the caller fall
        # back to /zones/{key} for the full payload.
        raise HTTPException(404, f'No meta data for zone {key}.')

    accept = (request.headers.get('accept-encoding') or '').lower()
    if 'gzip' in accept:
        gz = p.with_suffix('.json.gz')
        if gz.exists():
            try:
                if gz.stat().st_mtime >= p.stat().st_mtime:
                    return _conditional_file_response(
                        gz, request,
                        media_type='application/json',
                        content_encoding='gzip',
                    )
            except OSError:
                pass
    return _conditional_file_response(p, request, media_type='application/json')


@app.get('/zones')
@_LIMITER.limit('120/minute')
def list_zones(request: Request,
               x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key')):
    _check_auth(x_warmap_key, allowed_tiers=_TIERS_READ)
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


# Zones currently receiving in-progress uploads.  The viewer polls this
# alongside /status so it can paint a "LIVE" indicator next to each
# active zone in the sidebar.  Cheap query (single GROUP BY on the
# sessions index, complete=0 filter), so we don't gate it behind a
# longer poll interval -- runs at the same 5s cadence as /status.
#
# Staleness:  a session is "in progress" until the recorder writes a
# footer line.  If the recorder crashes mid-record there's no footer,
# and the row stays complete=0 forever.  To keep the indicator from
# lying about a long-dead session, we apply a wall-clock cutoff on
# last_active here -- anything older than LIVE_STALE_SECONDS gets
# dropped from the response.
LIVE_STALE_SECONDS = 300       # 5 min: recorder uploads every ~60s

@app.get('/live-zones')
@_LIMITER.limit('120/minute')
def list_live_zones(request: Request,
                    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key')):
    _check_auth(x_warmap_key, allowed_tiers=_TIERS_READ)
    rows = DBI.list_live_zones()
    cutoff = time.time() - LIVE_STALE_SECONDS
    zones: dict[str, dict] = {}
    for r in rows:
        last = r['last_active'] or 0
        if last < cutoff:
            continue
        zones[r['zone']] = {
            'in_progress': r['in_progress'],
            'uploaders':   r['uploaders'],
            'last_active': last,
        }
    return {'zones': zones, 'count': len(zones), 'stale_after_s': LIVE_STALE_SECONDS}


@app.post('/upload')
@_LIMITER.limit('60/minute')
async def upload(
    request: Request,
    files: list[UploadFile] = File(...),
    # client_id form field is now ignored (it was always spoofable). The
    # canonical uploader name comes from the authenticated key.  We accept
    # the field for backwards-compat with old clients.
    client_id: str = Form(default=''),
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    # Reader-tier keys are explicitly NOT allowed to upload -- they're for
    # downloading only.  Admin + uploader pass.
    rec = _check_auth(x_warmap_key, allowed_tiers=_TIERS_UPLOAD)
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
    # Tier defaults to 'uploader' for backward compat with the existing
    # admin tooling that mints contributor keys.  Pass 'reader' to mint
    # a download-only key for a friend (cannot upload, can pull all read
    # endpoints).  'admin' is rejected -- the master admin key is the
    # only admin-tier credential, set via WARMAP_API_KEY env.
    tier: str = 'uploader'


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
    tier = (body.tier or 'uploader').strip().lower()
    if tier not in {'uploader', 'reader'}:
        raise HTTPException(400, "tier must be 'uploader' or 'reader'")
    # Idempotent on name: if a key already exists with this name, return it.
    # Note: an existing key keeps its original tier -- to retier, delete and
    # remint.  This avoids surprise privilege escalation if a name collides.
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
        name=name, key=new_key, tier=tier,
        created_at=time.time(), note=body.note,
    )
    return {'name': name, 'key': new_key, 'tier': tier,
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


@app.post('/admin/zone_reset/{key}')
def admin_zone_reset(
    key: str,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    """Quarantine every dump that contributed to a zone key, delete the
    merged JSON, and trigger a re-merge.  Use when a recorder bug has
    contaminated a zone -- e.g. the pre-fix undercity recordings that
    flattened all floors onto floor=1.  After the fix lands, call this
    so future uploads can rebuild the zone cleanly without the bad
    data leaking back through re-merge.

    A dump "belongs" to a zone when its header's `zone` field equals the
    requested key (overworld/town/nmd/undercity/etc) OR its header's
    `world` field equals the key (pit, where world is the per-template
    key used by the merger).
    """
    _check_admin(x_warmap_key)
    safe_key = _safe_filename(key + '.json')
    if not safe_key:
        raise HTTPException(400, 'Bad zone key.')

    # 1. Walk dumps, identify ones that belong to this zone, quarantine them.
    moved = []
    scan_errors = 0
    for p in DUMPS_DIR.glob('*.ndjson'):
        try:
            with open(p, encoding='utf-8') as f:
                first_line = f.readline()
            if not first_line:
                continue
            header = json.loads(first_line)
            # Recorder writes the header as { type: 'header', zone: ..., world: ..., ... }
            if header.get('type') != 'header':
                continue
            zone = header.get('zone')
            world = header.get('world')
            if zone == key or world == key:
                dst = QUARANTINE_DIR / p.name
                shutil.move(str(p), str(dst))
                moved.append(p.name)
                DBI.remove_session(p.name)
        except (OSError, ValueError, json.JSONDecodeError):
            scan_errors += 1

    # 2. Drop the merged zone JSON.
    zone_path = DATA_DIR / safe_key
    json_existed = zone_path.exists()
    if json_existed:
        try:
            zone_path.unlink()
        except OSError:
            pass

    # 3. Trigger a fresh merge so any remaining dumps re-aggregate (and the
    #    actor index regenerates without the dropped data).
    threading.Thread(target=_run_merge, daemon=True).start()

    return {
        'ok': True,
        'key': key,
        'dumps_quarantined': moved,
        'count': len(moved),
        'merged_json_deleted': json_existed,
        'scan_errors': scan_errors,
    }


@app.post('/admin/floor_reset/{key}/{floor}')
def admin_floor_reset(
    key: str,
    floor: int,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    """Quarantine every dump that contributed any data to a specific
    (zone, floor) pair, then trigger a re-merge.  Lighter alternative
    to /admin/zone_reset when only one floor is contaminated.

    Real-world trigger: cross-session floor-detection drift in
    multi-world zones (undercity in particular) where the recorder
    increments the floor counter on world_id change but different
    sessions enter the worlds in different orders.  Result: floor 2
    in zone X1_Undercity_SnakeTemple_01 ends up containing data from
    both world_02 (correct) AND world_03 (should have been floor 3),
    so a boss from world_03 appears on what the viewer renders as
    floor 2.

    What we do:
      1. Walk all dumps for this zone (header.zone == key).
      2. For each dump, scan body lines for ANY entry tagged with the
         target floor (samples, grid_cells, actors, or floor_change
         events crossing onto/off the target).
      3. Quarantine the matching dumps wholesale.  Yes, this also
         drops their other-floor contributions -- pragmatic tradeoff:
         a single dump's floor data can't be cleanly separated
         server-side (the floor counter is session-internal), and
         the user has to re-record the zone anyway to get clean
         floor labelling.
      4. Drop the merged JSON, trigger re-merge.

    Reversible: the dumps live in quarantine/ until manually deleted.
    Move them back to dumps/ + trigger re-merge to restore.
    """
    _check_admin(x_warmap_key)
    if floor < 1 or floor > 99:
        raise HTTPException(400, 'Bad floor (expected 1..99).')
    safe_key = _safe_filename(key + '.json')
    if not safe_key:
        raise HTTPException(400, 'Bad zone key.')

    # 1. Walk dumps, identify ones touching (zone=key, floor=floor).
    moved = []
    scan_errors = 0
    skipped_no_match = 0
    for p in DUMPS_DIR.glob('*.ndjson'):
        try:
            with open(p, encoding='utf-8') as f:
                first_line = f.readline()
                if not first_line:
                    continue
                header = json.loads(first_line)
                if header.get('type') != 'header':
                    continue
                zone = header.get('zone')
                world = header.get('world')
                if zone != key and world != key:
                    continue
                # Header matches the zone -- now scan the body for any
                # line tagged with the target floor.  We also count
                # floor_change events with from_floor == target or
                # to_floor == target so a session that merely crossed
                # the floor counts (its samples/actors get attributed
                # to whichever floor was active at the time, which
                # is what we're trying to invalidate).
                touches_floor = False
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line)
                    except (ValueError, json.JSONDecodeError):
                        continue
                    fl = obj.get('floor')
                    if fl == floor:
                        touches_floor = True
                        break
                    # floor_change carries the transition in metadata.
                    if obj.get('type') == 'event' and obj.get('kind') == 'floor_change':
                        meta = obj.get('metadata') or {}
                        if meta.get('from_floor') == floor or meta.get('to_floor') == floor:
                            touches_floor = True
                            break
                if not touches_floor:
                    skipped_no_match += 1
                    continue
            # Out of the with-open block before moving the file.
            dst = QUARANTINE_DIR / p.name
            shutil.move(str(p), str(dst))
            moved.append(p.name)
            DBI.remove_session(p.name)
        except (OSError, ValueError, json.JSONDecodeError):
            scan_errors += 1

    # 2. Drop the merged zone JSON so the next-served version comes
    # from a fresh merge run (without the quarantined dumps).
    zone_path = DATA_DIR / safe_key
    json_existed = zone_path.exists()
    if json_existed:
        try:
            zone_path.unlink()
        except OSError:
            pass
    gz_path = DATA_DIR / (safe_key + '.gz')
    if gz_path.exists():
        try:
            gz_path.unlink()
        except OSError:
            pass

    # 3. Trigger a fresh merge.
    threading.Thread(target=_run_merge, daemon=True).start()

    return {
        'ok': True,
        'key':                  key,
        'floor':                floor,
        'dumps_quarantined':    moved,
        'count':                len(moved),
        'skipped_no_match':     skipped_no_match,
        'merged_json_deleted':  json_existed,
        'scan_errors':          scan_errors,
    }


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


# ---------------------------------------------------------------------------
# Dynamic ignore-list management (admin only).
#
# These patterns are additive on top of the hardcoded _SKIN_IGNORE_SUBSTR in
# merger/merge.py.  When admin adds 'BurningAether' here, the merger drops
# any actor whose skin contains that substring on the next merge cycle, and
# the uploader pushes the same list to the recorder via core/ignore_dynamic.lua
# so subsequent recorder sessions also stop emitting it.
# ---------------------------------------------------------------------------

class IgnoreAddRequest(BaseModel):
    pattern: str
    note:    str = ''


@app.get('/admin/ignore')
def admin_list_ignore(
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    _check_admin(x_warmap_key)
    return {'patterns': DBI.list_ignore_patterns()}


@app.post('/admin/ignore')
def admin_add_ignore(
    body: IgnoreAddRequest,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    rec = _check_admin(x_warmap_key)
    pattern = (body.pattern or '').strip()
    if not pattern:
        raise HTTPException(400, 'pattern required')
    if len(pattern) > 200:
        raise HTTPException(400, 'pattern too long (max 200)')
    added = DBI.add_ignore_pattern(pattern, added_by=rec.name, note=body.note)
    # Reset the merger's cached state so the next merge cycle re-applies
    # the updated ignore list to existing dumps (otherwise an actor that
    # already merged into a zone aggregate stays there until eviction).
    try:
        merge = _import_merger()
        merge.reset_merge_state()
    except Exception:
        pass
    return {'ok': True, 'added': added, 'pattern': pattern}


@app.delete('/admin/ignore/{pattern}')
def admin_remove_ignore(
    pattern: str,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    _check_admin(x_warmap_key)
    removed = DBI.remove_ignore_pattern(pattern)
    if not removed:
        raise HTTPException(404, 'pattern not found')
    try:
        merge = _import_merger()
        merge.reset_merge_state()
    except Exception:
        pass
    return {'ok': True, 'removed': pattern}


# ---------------------------------------------------------------------------
# Actor labels (rename / reclassify per-actor).
#
# Lets an admin click an actor in the viewer and apply a custom label
# ("Whispers Dungeon Entrance") and / or kind override ("dungeon_entrance"
# even though it auto-classified as plain "portal").  Keyed by the same
# composite (zone, skin, rx, ry, floor) the merger uses, so the viewer
# does an O(1) lookup per actor on render and substitutes the user's
# label / kind.
# ---------------------------------------------------------------------------

class ActorLabelRequest(BaseModel):
    zone:          str
    skin:          str
    rx:            int
    ry:            int
    floor:         int = 1
    label:         Optional[str] = None
    kind_override: Optional[str] = None
    note:          str = ''


@app.get('/labels')
@_LIMITER.limit('120/minute')
def get_labels(request: Request,
               x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key')):
    """Public-to-authenticated-callers list of actor labels.  Viewer
    fetches this on signin + after every admin save and applies
    overrides during render."""
    _check_auth(x_warmap_key, allowed_tiers=_TIERS_READ)
    return {'labels': DBI.list_actor_labels()}


@app.post('/admin/labels')
def admin_set_label(
    body: ActorLabelRequest,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    """Insert-or-update a label.  Setting both label and kind_override
    to empty deletes the row (treat as 'clear this actor's overrides')."""
    rec = _check_admin(x_warmap_key)
    if not body.zone or not body.skin:
        raise HTTPException(400, 'zone and skin required')
    if (body.label or '').strip() == '' and (body.kind_override or '').strip() == '':
        DBI.remove_actor_label(zone=body.zone, skin=body.skin,
                               rx=body.rx, ry=body.ry, floor=body.floor)
        return {'ok': True, 'cleared': True}
    DBI.upsert_actor_label(
        zone=body.zone, skin=body.skin, rx=body.rx, ry=body.ry, floor=body.floor,
        label=body.label, kind_override=body.kind_override,
        note=body.note, set_by=rec.name,
    )
    return {'ok': True, 'cleared': False}


@app.delete('/admin/labels')
def admin_remove_label(
    zone:  str,
    skin:  str,
    rx:    int,
    ry:    int,
    floor: int = 1,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    """Explicit label removal.  Same effect as POSTing with empty
    label + kind_override but keeps the verb honest."""
    _check_admin(x_warmap_key)
    removed = DBI.remove_actor_label(zone=zone, skin=skin, rx=rx, ry=ry, floor=floor)
    if not removed:
        raise HTTPException(404, 'label not found')
    return {'ok': True}
