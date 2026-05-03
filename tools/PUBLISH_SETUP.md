# WarPath data publisher — operator setup

This is the one-time setup the operator (you) does on `d4data` so the
zone-data zip auto-publishes to GitHub Releases.  Friends' Fetch.bat
then downloads from GitHub directly — no API key, no Python needed
on their end.

## 1. Create the data repo

On GitHub, create a new repo (any visibility — public is fine; the
data is map geometry, no PII).  Suggested name: **`WarPath-data`**
under your account or org.

Leave it empty.  The publisher creates the `latest` release on
first run.

## 2. Mint a Personal Access Token

GitHub → Settings → Developer settings → Personal access tokens →
**Tokens (classic)** → Generate new token → classic.

- **Name**: `warpath-publisher`
- **Expiration**: never (or as long as you're comfortable)
- **Scopes**: just `repo` (full control of private repositories — the
  granular `Contents: Read and write` scope on the data repo also
  works if you prefer fine-grained tokens)

Copy the token (`ghp_...`) immediately — GitHub only shows it once.

For belt-and-braces: a dedicated bot account with push access to
just the data repo is cleaner than your personal token, but a PAT
on your account is fine for a friends-only setup.

## 3. Drop the token into `.env`

```bash
ssh root@d4data
echo 'GITHUB_DATA_TOKEN=ghp_yourTokenHere' >> /opt/warmap/.env
# Optional: override the default repo path
# echo 'GITHUB_DATA_REPO=youraccount/WarPath-data' >> /opt/warmap/.env
chmod 600 /opt/warmap/.env
```

## 4. Test the publisher by hand

```bash
sudo /opt/warmap/tools/publish_zones.sh --verbose
```

Expected output (first run):

```
  collected 411 files from /var/lib/docker/volumes/warmap-data/_data/data/zones
  built zones.zip: 5.8 MB (411 files) in 2.3s
  release id=12345 url=https://github.com/.../releases/tag/latest
  uploaded zones.zip (5.8 MB)
  download URL: https://github.com/.../releases/latest/download/zones.zip
```

Visit the release URL in a browser — the zip should be there.

## 5. Install the cron entry

```bash
sudo crontab -e
```

Add:

```
*/15 * * * * /opt/warmap/tools/publish_zones.sh >> /var/log/warmap-publish.log 2>&1
```

The publisher hashes the inputs and skips the upload when nothing
changed since the last run, so a 15-min cadence costs ~nothing on
a quiet hour and ~5MB upload every 15 min on a busy one.

## 6. Verify the loop

After ~20 minutes, check the log:

```bash
sudo tail -n 50 /var/log/warmap-publish.log
```

Most lines should be `no changes since last publish (hash=...) ; skipping`.

When the merger emits new data, you'll see:
```
  collected 411 files from /var/lib/docker/volumes/warmap-data/_data/data/zones
  built zones.zip: 5.8 MB ...
  uploaded zones.zip ...
```

## Client side

Friends just double-click `<scripts>\WarPath\bin\Fetch.bat` (or a
desktop shortcut).  No Python, no API key — `curl` pulls the zip
from GitHub's CDN, `tar` extracts it.  Both ship with Windows 10+.

If they want to override the repo (e.g. forking the data publisher
to their own account):
```bat
set WARPATH_DATA_REPO=youraccount/WarPath-data
```

## Rollback

If the publisher breaks something, friends with the older
Python-based fetcher in their bundle can keep using it
(`pythonw bin\fetch_all.py`).  That path goes directly to the
WarMap API and is unaffected by GitHub-side issues.

## Troubleshooting

**`Bad credentials` from GitHub API:** token expired / revoked.
Mint a new one, update `/opt/warmap/.env`, re-run.

**`Asset upload failed (status=422)`:** the previous asset wasn't
fully deleted before the upload retried.  Delete it manually via
`gh release delete-asset latest zones.zip --repo <repo>` and re-run.

**Friend reports "release not found":** they're probably hitting
`magoogle/WarPath-data` but you published to a different repo.
Have them set `WARPATH_DATA_REPO=youraccount/WarPath-data` in their
environment, or tell them to update the bundle's `Fetch.bat` to
hard-code the right URL.

**Cron isn't firing:** check `systemctl status cron` and
`/var/log/syslog` for cron errors.  Permission issues usually
trace back to the script not being executable
(`chmod +x /opt/warmap/tools/publish_zones.sh`).
