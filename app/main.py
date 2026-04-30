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

# ----- Paths (overridable via env for local dev) ---------------------------
ROOT      = Path(os.getenv('WARMAP_ROOT', '/data'))
DUMPS_DIR = ROOT / 'dumps'
DATA_DIR  = ROOT / 'data' / 'zones'
SIDECAR   = ROOT / 'sidecar'                # for saturated.json target
LOG_DIR   = ROOT / 'logs'

for d in (DUMPS_DIR, DATA_DIR, SIDECAR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ----- Auth ----------------------------------------------------------------
API_KEY = os.getenv('WARMAP_API_KEY', '').strip()
if not API_KEY:
    print('WARNING: WARMAP_API_KEY not set; uploads will be rejected.',
          file=sys.stderr)

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
def _check_auth(provided: Optional[str]) -> None:
    if not API_KEY:
        raise HTTPException(503, 'Server is not configured (no API key set).')
    if provided != API_KEY:
        raise HTTPException(401, 'Bad or missing X-WarMap-Key header.')


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


@app.post('/upload')
async def upload(
    files: list[UploadFile] = File(...),
    client_id: str = Form(default='anon'),
    x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key'),
):
    _check_auth(x_warmap_key)
    accepted, rejected = [], []
    for f in files:
        name = _safe_filename(f.filename or '')
        if not name:
            rejected.append({'name': f.filename, 'reason': 'bad_name'})
            continue
        # Prefix with client_id so multiple users don't collide
        safe_client = ''.join(c for c in client_id if c.isalnum() or c in '_-')[:32] or 'anon'
        out_name = f'{safe_client}__{name}'
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

    return {'accepted': accepted, 'rejected': rejected}


@app.post('/merge')
def merge_now(x_warmap_key: Optional[str] = Header(default=None, alias='X-WarMap-Key')):
    _check_auth(x_warmap_key)
    return _run_merge()
