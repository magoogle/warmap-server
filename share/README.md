# Share WarMap data with friends

The server gates all read endpoints behind `X-WarMap-Key` and rate-limits
each IP to 120 requests/minute.  To give a friend access:

## 1. Mint a reader-tier key (operator runs this)

```bash
curl -sS -X POST -H "X-WarMap-Key: $ADMIN_KEY" \
     -H 'Content-Type: application/json' \
     -d '{"name":"alice","tier":"reader","note":"shared via discord"}' \
     https://your-server:30100/admin/keys
```

The response includes the new key.  Send it to the friend over a private
channel.  Reader-tier keys can pull every read endpoint but cannot upload
sessions (so they can't pollute your dump dir).

To list / disable / delete keys:

```bash
curl -H "X-WarMap-Key: $ADMIN_KEY" https://your-server:30100/admin/keys
curl -X POST -H "X-WarMap-Key: $ADMIN_KEY" https://your-server:30100/admin/keys/alice/disable
curl -X DELETE -H "X-WarMap-Key: $ADMIN_KEY" https://your-server:30100/admin/keys/alice
```

## 2. Friend pulls data

### Windows (PowerShell)

```powershell
$env:WARMAP_KEY = '<their-reader-key>'
.\share-map.ps1 -List                          # see what's available
.\share-map.ps1 Skov_Cerrigar Step_South       # download specific zones
.\share-map.ps1 -All                           # download everything
```

### Unix

```bash
WARMAP_KEY=<their-reader-key> ./share-map.sh --list
WARMAP_KEY=<their-reader-key> ./share-map.sh Skov_Cerrigar Step_South
WARMAP_KEY=<their-reader-key> ./share-map.sh --all
```

Output goes to `./zones/<key>.json` by default.  Override with
`$env:WARMAP_OUT` / `WARMAP_OUT=`.

## 3. Rate limits

Per IP, per uvicorn worker (4 workers means effective ceiling ~4x):

| Endpoint        | Limit       |
|-----------------|-------------|
| Read endpoints  | 120/minute  |
| `/upload`       | 60/minute   |
| `/admin/*`      | 30/minute   |

The master `WARMAP_API_KEY` is exempt -- the operator can hammer their own
server during maintenance without 429s.  A 429 response carries
`Retry-After`; wait that long and retry.
