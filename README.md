# WarMap upload server

Lightweight FastAPI service that accepts recorder NDJSON dumps from
distributed clients, runs the merger periodically, and serves the
aggregated data back as static JSON.

```
client recorder    --upload-->     warmap-server     --merge--> data/zones/*.json
                  X-WarMap-Key                                  data/zones/_actor_index.json
                                                                sidecar/saturated.json
client recorder    <--pull--                                    (publicly readable)
```

## Endpoints

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET  | `/health`            | -   | liveness probe |
| GET  | `/status`            | -   | dump count, zone count, last merge stats |
| GET  | `/zones`             | -   | list of curated zone keys |
| GET  | `/zones/<key>`       | -   | one merged zone JSON file |
| GET  | `/saturated.json`    | -   | recorder pulls this on session start |
| GET  | `/coverage`          | -   | per-zone coverage report |
| GET  | `/actor-index`       | -   | universal actor index |
| POST | `/upload`            | API key | multi-file NDJSON upload |
| POST | `/merge`             | API key | trigger an immediate merge run (admin) |

Public reads + authed writes is intentional: the merged data isn't
private, and we want client recorders to pull `saturated.json` without
needing the secret.

## Server install (Ubuntu + Docker)

The server is shipped as a docker-compose stack.  It's intentionally
isolated -- doesn't share networks, volumes, or ports with anything else
on the host.

```bash
# On the server
sudo mkdir -p /opt/warmap
cd /opt/warmap

# Drop in the WarMap repo (or just the server/ + tools/merger/ subset)
# git clone <your repo> .

# Generate a strong API key for client uploads
echo "WARMAP_API_KEY=$(openssl rand -hex 32)" | sudo tee /opt/warmap/.env

# Build + run
sudo docker compose -f server/docker-compose.yml --env-file .env up -d --build

# Check it's listening
curl http://127.0.0.1:30100/health
```

The compose file binds `30100` (host) to the container's `8000`. Open
TCP 30100 in your cloud firewall to expose to clients.

Logs:

```bash
sudo docker logs -f warmap-server
```

Data + dumps live in the `warmap-data` named volume.  To inspect:

```bash
sudo docker exec warmap-server ls -la /data
sudo docker exec warmap-server ls /data/data/zones
```

To wipe and rebuild:

```bash
sudo docker compose -f server/docker-compose.yml down -v
sudo docker compose -f server/docker-compose.yml --env-file .env up -d --build
```

## Client setup

After deploying, distribute the API key to trusted players.  The client
uploader is a small Python script (see `tools/uploader/`) that watches
`scripts/WarMapRecorder/dumps/` and posts new files to your server.

```powershell
# Per-player one-time setup
cd C:\New folder\Microsoft\WarMap\tools\uploader
pip install -r requirements.txt
copy .env.example .env
# Edit .env: WARMAP_SERVER, WARMAP_API_KEY, WARMAP_CLIENT_ID
python upload.py --watch
```

The client uploader is optional. Players can still use WarMap fully
locally (recorder + local merger + viewer) without uploading.  When the
uploader runs, their recorded data also benefits everyone else once
merged.

## Scaling notes

- The merge step is O(total cells across all sessions). With ~25 sessions
  totaling 700k cells the local merger ran in ~1s. The server should
  comfortably handle thousands of sessions before merge time becomes
  noticeable.
- Disk usage scales with the unmerged dump backlog. Each session is
  ~50-200 KB; a thousand stored dumps is ~100 MB. The container could
  also auto-prune merged sessions older than N days -- not enabled by
  default since the local copies are small and aggregating more sessions
  improves saturation confidence.

## What the server does NOT do (yet)

- No client identity / quota / abuse limiting beyond the shared API key.
- No content validation beyond "is it NDJSON we can parse?". A malicious
  client could submit forged data that pollutes the merge. For a small
  trusted-friends deployment this is fine; for a public deployment we'd
  add per-client trust scoring or moderator review.
- No HTTPS termination. If you need it, point Caddy or another reverse
  proxy at `127.0.0.1:30100` and don't expose 30100 publicly.
