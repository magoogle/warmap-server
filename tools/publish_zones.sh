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
    echo "ERROR: zones dir not found: $ZONES_DIR" >&2
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
