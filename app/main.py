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

from app.keys import KeyStore

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

KEYSTORE = KeyStore(path=ROOT / 'keys' / 'api_keys.json', admin_key=ADMIN_KEY)
QUARANTINE_DIR = ROOT / 'quarantine'
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

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
def _check_auth(provided: Optional[str]):
    """Validate against the keystore.  Returns the KeyRecord (admin or
    uploader) or raises 401."""
    if not ADMIN_KEY:
        raise HTTPException(503, 'Server is not configured (no admin key set).')
    rec = KEYSTORE.validate(provided)
    if not rec:
        raise HTTPException(401, 'Bad or missing X-WarMap-Key header.')
    return rec


def _check_admin(provided: Optional[str]):
    """Like _check_auth but requires the admin tier."""
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
    """Run the merger against DUMPS_DIR -> DATA_DIR. Thread-safe."""
    if not _merge_lock.acquire(blocking=False):
        return {'skipped': True, 'reason': 'already running'}
    try:
        _last_merge['started_at'] = time.time()
        _last_merge['error'] = None
        try:
            merge = _import_merger()
            # Include in-progress sessions so the live viewer reflects what
            # the player just walked through, not just what they've finished.
            state = merge.merge_all(DUMPS_DIR, only_complete=False)
            merge.emit_all(state, DATA_DIR, SIDECAR)
            accepted = sum(len(agg.sessions) for agg in state.values())
            _last_merge['accepted'] = accepted
            _last_merge['skipped']  = 0
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
# Filenames on disk are `<client_id>__<session_id>.ndjson` (the upload
# endpoint enforces this format).  Header line carries zone + activity
# metadata; the last sample line carries the latest player position so
# the viewer can plot live tracks.
# ---------------------------------------------------------------------------

def _parse_dump_summary(path) -> dict:
    """Read header + last sample of an NDJSON dump for a quick summary.

    Avoids loading the whole file: header is line 1; last sample is found
    by reading the tail and walking backward for a line with type=sample.
    """
    name = path.name
    info = {
        'name':         name,
        'client_id':    name.split('__', 1)[0] if '__' in name else 'anon',
        'size':         path.stat().st_size,
        'mtime':        path.stat().st_mtime,
        'zone':         None,
        'world':        None,
        'activity':     None,
        'session_id':   None,
        'started_at':   None,
        'last_x':       None,
        'last_y':       None,
        'last_z':       None,
        'last_floor':   None,
        'last_t':       None,
        'sample_count': 0,
        'complete':     False,
    }
    try:
        with path.open('r', encoding='utf-8') as f:
            first = f.readline()
            try:
                h = json.loads(first)
                if h.get('type') == 'header':
                    info['zone']       = h.get('zone')
                    info['world']      = h.get('world')
                    info['activity']   = h.get('activity_kind')
                    info['session_id'] = h.get('session_id')
                    info['started_at'] = h.get('started_at')
            except json.JSONDecodeError:
                pass
        # Tail-scan for last sample + footer detection.
        # File is small (typically <2 MB); just walk it once.
        last_sample = None
        sample_count = 0
        complete = False
        with path.open('r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    o = json.loads(line)
                except json.JSONDecodeError:
                    continue
                t = o.get('type')
                if t == 'sample':
                    last_sample = o
                    sample_count += 1
                elif t == 'footer':
                    complete = True
        if last_sample:
            info['last_x']     = last_sample.get('x')
            info['last_y']     = last_sample.get('y')
            info['last_z']     = last_sample.get('z')
            info['last_floor'] = last_sample.get('floor')
            info['last_t']     = last_sample.get('t')
        info['sample_count'] = sample_count
        info['complete']     = complete
    except OSError:
        pass
    return info


@app.get('/dumps')
def list_dumps():
    """List every dump file with header + last-sample summary.

    Sorted by mtime descending so newest is first.  Useful for the live
    viewer: shows who's currently uploading + where they are right now.
    """
    files = list(DUMPS_DIR.glob('*.ndjson')) + list(DUMPS_DIR.glob('*.json'))
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    summaries = [_parse_dump_summary(p) for p in files]
    return {'dumps': summaries, 'count': len(summaries)}


@app.get('/dumps/{name}')
def get_dump(name: str):
    """Return the raw NDJSON for a specific dump (live viewer's "trail" mode)."""
    safe = _safe_filename(name)
    if not safe:
        raise HTTPException(400, 'Bad dump name.')
    p = DUMPS_DIR / safe
    if not p.exists():
        raise HTTPException(404, 'Not found.')
    return FileResponse(p, media_type='application/x-ndjson')


@app.get('/uploaders')
def list_uploaders():
    """Aggregate dumps by `client_id` prefix.

    Returns one entry per uploader with: total session count, in-progress
    count, last-active timestamp (= mtime of the newest dump from them),
    distinct zones touched, latest position.
    """
    files = list(DUMPS_DIR.glob('*.ndjson')) + list(DUMPS_DIR.glob('*.json'))
    by_client: dict[str, dict] = {}
    for p in files:
        cid = p.name.split('__', 1)[0] if '__' in p.name else 'anon'
        slot = by_client.setdefault(cid, {
            'client_id':       cid,
            'sessions':        0,
            'in_progress':     0,
            'last_active':     0,
            'zones':           set(),
            'newest_dump':     None,
            'newest_summary':  None,
        })
        slot['sessions'] += 1
        info = _parse_dump_summary(p)
        if info['zone']:
            slot['zones'].add(info['zone'])
        if not info['complete']:
            slot['in_progress'] += 1
        if info['mtime'] > slot['last_active']:
            slot['last_active']    = info['mtime']
            slot['newest_dump']    = info['name']
            slot['newest_summary'] = info
    out = []
    for cid, s in by_client.items():
        s['zones'] = sorted(s['zones'])
        out.append(s)
    out.sort(key=lambda s: s['last_active'], reverse=True)
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

    if accepted:
        KEYSTORE.record_upload(rec.key, n_files=len(accepted))
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
    return {
        'keys': [{
            'name':       k.name,
            'key':        k.key,
            'tier':       k.tier,
            'created_at': k.created_at,
            'last_used':  k.last_used,
            'uploads':    k.uploads,
            'enabled':    k.enabled,
            'note':       k.note,
        } for k in KEYSTORE.list_uploader_keys()]
    }


@app.post('/admin/keys')
def admin_mint_key(
    body: MintRequest,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    _check_admin(x_warmap_key)
    try:
        rec = KEYSTORE.mint(name=body.name, note=body.note)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {
        'name':       rec.name,
        'key':        rec.key,
        'tier':       rec.tier,
        'created_at': rec.created_at,
        'note':       rec.note,
    }


@app.post('/admin/keys/{name}/disable')
def admin_disable_key(
    name: str,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    _check_admin(x_warmap_key)
    rec = KEYSTORE.set_enabled(name, False)
    if not rec:
        raise HTTPException(404, 'No such key.')
    return {'ok': True, 'name': rec.name, 'enabled': rec.enabled}


@app.post('/admin/keys/{name}/enable')
def admin_enable_key(
    name: str,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    _check_admin(x_warmap_key)
    rec = KEYSTORE.set_enabled(name, True)
    if not rec:
        raise HTTPException(404, 'No such key.')
    return {'ok': True, 'name': rec.name, 'enabled': rec.enabled}


@app.delete('/admin/keys/{name}')
def admin_delete_key(
    name: str,
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    _check_admin(x_warmap_key)
    if not KEYSTORE.remove(name):
        raise HTTPException(404, 'No such key.')
    return {'ok': True, 'removed': name}


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
    KEYSTORE.set_enabled(safe_name, False)
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
