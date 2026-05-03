#!/bin/bash
# ---------------------------------------------------------------------------
# publish_zones.sh -- cron-friendly wrapper for tools/publish_zones.py.
#
# Runs the publisher directly on the host (stdlib-only Python, no
# docker exec required).  The script reads the merger's emitted
# files from the docker-managed volume mountpoint; .env at
# /opt/warmap/.env supplies GITHUB_DATA_TOKEN.
#
# Suggested cron entry (root, every 15 min):
#   */15 * * * * /opt/warmap/tools/publish_zones.sh >> /var/log/warmap-publish.log 2>&1
#
# First-run setup:
#   1. Create a GitHub Personal Access Token (classic) with `repo`
#      scope.  A dedicated bot account is cleanest.
#   2. echo 'GITHUB_DATA_TOKEN=ghp_xxxx' >> /opt/warmap/.env
#      (Optionally also: GITHUB_DATA_REPO=magoogle/WarPath-data)
#   3. chmod 600 /opt/warmap/.env  (matches existing perms)
#   4. Create the empty target repo on GitHub.  The publisher auto-
#      creates the 'latest' release on first run.
#   5. Install the cron entry above.  (Test once first by running
#      this script by hand: sudo /opt/warmap/tools/publish_zones.sh)
# ---------------------------------------------------------------------------

set -euo pipefail

# Default volume mountpoint per docker-compose.yml's `warmap-data`
# named volume.  Override via $WARMAP_VOLUME if the deploy is
# different.
ZONES_DIR="${WARMAP_VOLUME:-/var/lib/docker/volumes/warmap-data/_data}/data/zones"
PUBLISHER="$(dirname "$0")/publish_zones.py"
ENV_FILE="${WARMAP_ENV_FILE:-/opt/warmap/.env}"

if [[ ! -d "$ZONES_DIR" ]]; then
    # Most common cause: running as a non-root user.  Docker's volume
    # path traversal requires root because intermediate dirs under
    # /var/lib/docker/volumes are mode 701 (no group/other read,
    # only execute) -- a non-root caller can't see anything past
    # /var/lib/docker even though the leaf zones/ dir is 755.
    if [[ $EUID -ne 0 ]] && [[ "$ZONES_DIR" == /var/lib/docker/* ]]; then
        echo "ERROR: zones dir not visible to user '$(id -un)'." >&2
        echo "  Path: $ZONES_DIR" >&2
        echo "  Cause: docker volume mountpoint is root-only (mode 701)." >&2
        echo "  Fix:  sudo $0 $*" >&2
        exit 1
    fi
    echo "ERROR: zones dir not found: $ZONES_DIR" >&2
    echo "  If your docker volume lives elsewhere, override:" >&2
    echo "    WARMAP_VOLUME=/your/path $0" >&2
    exit 1
fi
if [[ ! -f "$PUBLISHER" ]]; then
    echo "ERROR: publisher script not found: $PUBLISHER" >&2
    exit 1
fi

exec python3 "$PUBLISHER" \
    --zones-dir "$ZONES_DIR" \
    --env-file "$ENV_FILE" \
    "$@"
