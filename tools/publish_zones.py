#!/usr/bin/env python3
"""
WarPath zone-data publisher.

Builds a single zip of every file the WarPath plugin needs at runtime
(`_meta_index.json` + `_actor_index.json` + `_links_index.json` +
all `<key>.{meta,nav,links}.json`) and uploads it as the asset on a
GitHub release tagged 'latest'.  Friends' Fetch.bat downloads that
zip directly via curl -- no Python on the client, no API key, just
GitHub's CDN.

Architecture:
  d4data merger writes /data/zones/*.json   (every 30s)
        |
        | cron */15 *
        v
  publish_zones.py packages + uploads
        |
        v
  GitHub Releases (magoogle/WarPath-data, tag='latest')
        |
        | curl -L (in client Fetch.bat)
        v
  WarPath/cache/ on the player's machine

Idempotency:
  * If the zone-files' aggregate SHA-256 hasn't changed since the
    previous publish, we skip the upload.  The hash is stored in a
    sidecar file at /data/zones/.publish_state so cross-cron
    invocations stay aware.
  * The 'latest' release tag is reused; the asset is overwritten in
    place.  We keep one GitHub release total -- no growing
    revision history (releases preserve old asset IDs in the API
    but they're invisible to the consumer).

Auth:
  GITHUB_DATA_TOKEN env var -- a PAT with `repo` scope on the data
  repo.  Loaded from /opt/warmap/.env or whatever .env path is
  given via --env-file.

Stdlib only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_ZONES_DIR = pathlib.Path('/data/zones')
DEFAULT_REPO = 'magoogle/WarPath-data'
DEFAULT_TAG = 'latest'
DEFAULT_ASSET_NAME = 'zones.zip'

# Files we include in the zip.  Per-zone variants are matched by
# suffix; the three index files are matched by exact name.
INDEX_FILES = ('_meta_index.json', '_actor_index.json', '_links_index.json')
ZONE_VARIANT_SUFFIXES = ('.meta.json', '.nav.json', '.links.json')


# ---------------------------------------------------------------------------
# .env loader (no python-dotenv dependency)
# ---------------------------------------------------------------------------

def load_env(path: pathlib.Path) -> None:
    """Merge KEY=VALUE lines from `path` into os.environ.  Only sets
    keys that aren't already in the environment so explicit env wins."""
    if not path.exists():
        return
    try:
        for line in path.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v
    except OSError as e:
        print(f'  warning: could not read {path}: {e}', file=sys.stderr)


# ---------------------------------------------------------------------------
# Zip building
# ---------------------------------------------------------------------------

def collect_files(zones_dir: pathlib.Path) -> list[pathlib.Path]:
    """Walk zones_dir and return the curated subset we publish."""
    out: list[pathlib.Path] = []
    for name in INDEX_FILES:
        p = zones_dir / name
        if p.exists():
            out.append(p)
    for p in sorted(zones_dir.iterdir()):
        if not p.is_file():
            continue
        for suffix in ZONE_VARIANT_SUFFIXES:
            if p.name.endswith(suffix):
                out.append(p)
                break
    return out


def file_sha256(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(64 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def aggregate_hash(files: list[pathlib.Path]) -> str:
    """Hash of (filename, file_hash) pairs sorted by filename.  Used to
    decide whether the bundle has actually changed since the last
    publish -- skips the upload if not."""
    h = hashlib.sha256()
    for p in sorted(files, key=lambda x: x.name):
        h.update(p.name.encode('utf-8'))
        h.update(b'\0')
        h.update(file_sha256(p).encode('utf-8'))
        h.update(b'\n')
    return h.hexdigest()


def build_zip(files: list[pathlib.Path], target: pathlib.Path,
              progress: bool) -> int:
    """Write a deflate-compressed zip of `files` to `target`.  Returns
    the resulting zip size in bytes.  Uses ZIP_DEFLATED level 6 (the
    default) -- JSON compresses ~10x; level 9 is marginally smaller
    but ~3x slower for negligible wire-size gain."""
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, 'w', compression=zipfile.ZIP_DEFLATED,
                         compresslevel=6) as zf:
        for i, p in enumerate(files):
            zf.write(p, arcname=p.name)
            if progress and (i + 1) % 50 == 0:
                print(f'  ... zipped {i + 1}/{len(files)} files')
    return target.stat().st_size


# ---------------------------------------------------------------------------
# GitHub Releases API
# ---------------------------------------------------------------------------

def _gh_request(method: str, url: str, token: str, *, data: bytes | None = None,
                content_type: str = 'application/json',
                timeout: float = 60.0) -> tuple[int, bytes]:
    """Tiny GitHub API helper.  Returns (status, body)."""
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept':        'application/vnd.github+json',
        'User-Agent':    'WarPath-publisher',
    }
    if data is not None:
        headers['Content-Type'] = content_type
        headers['Content-Length'] = str(len(data))
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def get_or_create_release(repo: str, tag: str, token: str) -> dict:
    """Return the JSON body of the release with the given tag.  Creates
    one (with a dummy commitish auto-resolved to the default branch)
    if it doesn't exist."""
    # GET /repos/{repo}/releases/tags/{tag}
    status, body = _gh_request(
        'GET',
        f'https://api.github.com/repos/{repo}/releases/tags/{urllib.parse.quote(tag)}',
        token,
    )
    if status == 200:
        return json.loads(body)
    if status != 404:
        raise RuntimeError(f'unexpected status {status} on release lookup: {body[:200]!r}')
    # Create it.
    payload = json.dumps({
        'tag_name': tag,
        'name':     'Latest zone data',
        'body':     'Auto-generated by publish_zones.py.  Refreshed on '
                    'every server merge cycle that produces new data.',
        'draft':    False,
        'prerelease': False,
    }).encode('utf-8')
    status, body = _gh_request(
        'POST',
        f'https://api.github.com/repos/{repo}/releases',
        token, data=payload,
    )
    if status not in (200, 201):
        raise RuntimeError(f'release create failed status={status}: {body[:200]!r}')
    return json.loads(body)


def delete_existing_asset(repo: str, release: dict, asset_name: str,
                          token: str) -> None:
    """If an asset with `asset_name` already exists on `release`, delete
    it.  GitHub doesn't support overwrite-in-place; we delete + re-upload."""
    for asset in release.get('assets', []):
        if asset.get('name') == asset_name:
            asset_id = asset.get('id')
            if asset_id is None:
                continue
            status, body = _gh_request(
                'DELETE',
                f'https://api.github.com/repos/{repo}/releases/assets/{asset_id}',
                token,
            )
            if status not in (200, 204):
                print(f'  warning: failed to delete old asset (status={status}): '
                      f'{body[:200]!r}', file=sys.stderr)
            return


def upload_asset(release: dict, zip_path: pathlib.Path,
                 asset_name: str, token: str) -> dict:
    """Upload zip_path as a new asset on `release`.  Returns the asset
    metadata dict."""
    upload_url = release.get('upload_url', '')
    # GitHub's upload_url is templated, e.g.
    #   https://uploads.github.com/repos/.../releases/12345/assets{?name,label}
    upload_url = upload_url.split('{', 1)[0]
    upload_url += '?' + urllib.parse.urlencode({'name': asset_name})
    data = zip_path.read_bytes()
    status, body = _gh_request(
        'POST', upload_url, token,
        data=data,
        content_type='application/zip',
        timeout=300.0,   # uploads can be slow
    )
    if status not in (200, 201):
        raise RuntimeError(f'asset upload failed status={status}: {body[:200]!r}')
    return json.loads(body)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

STATE_FILENAME = '.publish_state'


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--zones-dir', type=pathlib.Path, default=DEFAULT_ZONES_DIR,
                    help=f'zones directory (default: {DEFAULT_ZONES_DIR})')
    ap.add_argument('--repo',       default=os.environ.get('GITHUB_DATA_REPO', DEFAULT_REPO),
                    help=f'<owner>/<repo> for the data release (default: {DEFAULT_REPO})')
    ap.add_argument('--tag',        default=DEFAULT_TAG)
    ap.add_argument('--asset-name', default=DEFAULT_ASSET_NAME)
    ap.add_argument('--env-file',   type=pathlib.Path, default=pathlib.Path('/opt/warmap/.env'),
                    help='dotenv file to source GITHUB_DATA_TOKEN from')
    ap.add_argument('--force',      action='store_true',
                    help='upload even when the aggregate hash hasn\'t changed')
    ap.add_argument('--dry-run',    action='store_true',
                    help='build the zip but do not contact GitHub')
    ap.add_argument('--verbose',    action='store_true')
    args = ap.parse_args()

    load_env(args.env_file)

    zones_dir = args.zones_dir
    if not zones_dir.exists():
        print(f'ERROR: zones dir not found: {zones_dir}', file=sys.stderr)
        return 1

    files = collect_files(zones_dir)
    if not files:
        print(f'ERROR: no publishable files in {zones_dir}', file=sys.stderr)
        return 1
    if args.verbose:
        print(f'  collected {len(files)} files from {zones_dir}')

    # Idempotency check.
    state_path = zones_dir / STATE_FILENAME
    new_hash = aggregate_hash(files)
    if not args.force and state_path.exists():
        try:
            prev_hash = state_path.read_text(encoding='utf-8').strip()
        except OSError:
            prev_hash = ''
        if prev_hash == new_hash:
            print(f'  no changes since last publish (hash={new_hash[:12]}); skipping')
            return 0

    # Build the zip into a temp dir under the zones dir so we don't
    # cross filesystems on /tmp -> volume rename.
    tmp_dir = pathlib.Path(tempfile.mkdtemp(prefix='publish_', dir=str(zones_dir)))
    zip_path = tmp_dir / args.asset_name
    try:
        t0 = time.time()
        size = build_zip(files, zip_path, progress=args.verbose)
        elapsed = time.time() - t0
        size_mb = size / (1024 * 1024)
        print(f'  built {zip_path.name}: {size_mb:.1f} MB ({len(files)} files) '
              f'in {elapsed:.1f}s')

        if args.dry_run:
            print(f'  dry-run: skipping upload')
            return 0

        token = os.environ.get('GITHUB_DATA_TOKEN', '').strip()
        if not token:
            print('ERROR: GITHUB_DATA_TOKEN not set (env or .env file)',
                  file=sys.stderr)
            return 2

        # Locate / create the release.
        release = get_or_create_release(args.repo, args.tag, token)
        if args.verbose:
            print(f'  release id={release.get("id")} url={release.get("html_url")}')

        # Replace the existing asset (if any) and upload fresh.
        delete_existing_asset(args.repo, release, args.asset_name, token)
        asset = upload_asset(release, zip_path, args.asset_name, token)
        print(f'  uploaded {asset.get("name")} ({asset.get("size", 0)/(1024*1024):.1f} MB)')
        print(f'  download URL: {asset.get("browser_download_url")}')

        # Record the new hash so the next cron run can skip a no-op
        # publish.
        try:
            state_path.write_text(new_hash + '\n', encoding='utf-8')
        except OSError as e:
            print(f'  warning: could not write {state_path}: {e}', file=sys.stderr)
    finally:
        try:
            zip_path.unlink(missing_ok=True)
            tmp_dir.rmdir()
        except OSError:
            pass

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
