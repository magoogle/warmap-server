# Deploying

Server runs at `/opt/warmap` on the host.  After the initial `git clone`,
deploys are:

```bash
cd /opt/warmap
git pull
sudo docker compose --env-file .env up -d --build
```

The `.env` file is **not** in the repo (intentionally) -- it contains the
shared API key.  On a fresh server:

```bash
sudo mkdir -p /opt/warmap
sudo chown $USER /opt/warmap
git clone https://github.com/magoogle/warmap-server.git /opt/warmap
echo "WARMAP_API_KEY=$(openssl rand -hex 32)" > /opt/warmap/.env
chmod 600 /opt/warmap/.env
cd /opt/warmap
sudo docker compose --env-file .env up -d --build
```

## Rotating the API key

```bash
echo "WARMAP_API_KEY=$(openssl rand -hex 32)" | sudo tee /opt/warmap/.env
sudo docker compose --env-file .env up -d
```

Old clients (and the warmap-recorder bundle on disk) will start getting
401 on uploads until rebundled with the new key.

## Ports

| Port | Use | Firewall |
|------|-----|----------|
| 30100 | Public upload + read API | Open to all |
| 30101 | Live viewer at `/viewer/` | UFW + cloud-FW: locked to operator IP |
