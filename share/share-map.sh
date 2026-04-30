#!/usr/bin/env bash
# share-map.sh -- pull WarMap zone data using a reader-tier API key.
#
# Usage:
#   WARMAP_KEY=<your-key> ./share-map.sh <zone-key> [<zone-key>...]
#   WARMAP_KEY=<your-key> ./share-map.sh --list
#   WARMAP_KEY=<your-key> ./share-map.sh --all
#
# Output goes to ./zones/<key>.json (creates ./zones if needed).
#
# Set WARMAP_SERVER if you're not pointing at the default host.

set -euo pipefail

SERVER="${WARMAP_SERVER:-http://87.99.138.184:30100}"
KEY="${WARMAP_KEY:-}"
OUT="${WARMAP_OUT:-./zones}"

if [[ -z "$KEY" ]]; then
    echo "ERROR: set WARMAP_KEY (your reader-tier API key)" >&2
    exit 2
fi

mkdir -p "$OUT"

curl_get() {
    # --compressed asks for gzip and decodes; the server pre-compresses
    # zone JSONs so this is the fast path.  -f makes 4xx/5xx exit non-zero.
    curl -sS -f --compressed -H "X-WarMap-Key: $KEY" "$@"
}

case "${1:-}" in
    --list)
        curl_get "$SERVER/zones" | python -c "import sys,json;[print(z) for z in json.load(sys.stdin)['zones']]"
        ;;
    --all)
        ZONES=$(curl_get "$SERVER/zones" | python -c "import sys,json;[print(z) for z in json.load(sys.stdin)['zones']]")
        for z in $ZONES; do
            echo "+ $z"
            curl_get -o "$OUT/$z.json" "$SERVER/zones/$z" || echo "  (failed)" >&2
        done
        ;;
    "")
        echo "usage: $0 [--list | --all | <zone-key> ...]" >&2
        exit 2
        ;;
    *)
        for z in "$@"; do
            echo "+ $z"
            curl_get -o "$OUT/$z.json" "$SERVER/zones/$z" || echo "  (failed)" >&2
        done
        ;;
esac

echo "done -> $OUT/"
