# Deployment Guide

This guide documents a production deployment for the Nexio IMDb Internal API Platform on a single Linux host serving:

- `https://api.nexioapp.org/` for the Nuxt portal
- `https://api.nexioapp.org/docs` for API docs
- `https://api.nexioapp.org/v1/...` for the API
- `https://api.nexioapp.org/healthz` and `https://api.nexioapp.org/readyz` for health checks

It assumes:

- Debian 12 or Ubuntu 24.04
- PostgreSQL 16+
- systemd for process management
- Caddy for TLS and reverse proxy
- one host running three services:
  - Go API
  - Go worker
  - Nuxt Nitro server

This platform is intended for internal, non-commercial use against the public IMDb dataset snapshots.

If you want containerized deployment instead, use the Docker Compose guide:

- [`docs/docker-compose-deployment.md`](/Users/jneerdael/Scripts/imdb-scrape/docs/docker-compose-deployment.md)

## Architecture

Production layout:

- `apps/api/cmd/api`
  - Go HTTP API on `127.0.0.1:8080`
- `apps/api/cmd/worker`
  - background worker for:
    - IMDb dataset sync
    - async bulk job execution
- `apps/web`
  - Nuxt Nitro server on `127.0.0.1:3000`
  - proxies `/v1`, `/healthz`, and `/readyz` to the Go API through `API_BASE_URL`
- PostgreSQL
  - canonical application database

Request flow:

1. Client connects to `api.nexioapp.org`.
2. Caddy terminates TLS and forwards all traffic to Nuxt.
3. Nuxt serves the portal and docs directly.
4. Nuxt proxies API and health routes to the Go API on localhost.
5. The worker imports IMDb snapshots and processes queued bulk jobs independently.

Proxy routing rule:

- `/v1/*`, `/healthz`, and `/readyz` must reach the Go API
- everything else should reach the Nuxt app

## Required DNS And OAuth Setup

Before deployment:

- Point `api.nexioapp.org` to the server IP.
- In Google Cloud Console, create an OAuth client for the web portal.
- Add this authorized redirect URI:
  - `https://api.nexioapp.org/auth/callback`
- Add this authorized JavaScript origin if needed by your Google project policy:
  - `https://api.nexioapp.org`

## Server Packages

Install base packages:

```bash
sudo apt update
sudo apt install -y \
  build-essential \
  curl \
  git \
  unzip \
  postgresql-16 \
  postgresql-client-16 \
  caddy
```

Install Go 1.26.x:

```bash
curl -LO https://go.dev/dl/go1.26.1.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.26.1.linux-amd64.tar.gz
echo 'export PATH=/usr/local/go/bin:$PATH' | sudo tee /etc/profile.d/go.sh
source /etc/profile.d/go.sh
go version
```

Install Node.js 22:

```bash
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
sudo apt install -y nodejs
node -v
npm -v
```

## System Users And Directories

Create an application user and directories:

```bash
sudo useradd --system --create-home --shell /bin/bash nexio
sudo mkdir -p /opt/nexio-imdb
sudo mkdir -p /var/log/nexio-imdb
sudo chown -R nexio:nexio /opt/nexio-imdb /var/log/nexio-imdb
```

Clone the repository:

```bash
sudo -u nexio git clone <YOUR_REPO_URL> /opt/nexio-imdb/current
cd /opt/nexio-imdb/current
```

## PostgreSQL Setup

Create the database and user:

```bash
sudo -u postgres psql <<'SQL'
CREATE USER nexio_imdb WITH PASSWORD 'CHANGE_ME';
CREATE DATABASE nexio_imdb OWNER nexio_imdb;
GRANT ALL PRIVILEGES ON DATABASE nexio_imdb TO nexio_imdb;
SQL
```

Apply the schema:

```bash
psql "postgres://nexio_imdb:CHANGE_ME@127.0.0.1:5432/nexio_imdb?sslmode=disable" \
  -f infra/postgres/migrations/0001_init.sql
```

If you use managed Postgres instead of local Postgres:

- create the same database and role remotely
- update `DATABASE_URL`
- ensure the host allows the app server to connect

## Environment Variables

This project currently uses a shared environment file model. In production, use one env file for all three processes.

Create:

- `/etc/nexio-imdb.env`

Example:

```dotenv
API_ADDRESS=127.0.0.1:8080
API_BASE_URL=http://127.0.0.1:8080
APP_BASE_URL=https://api.nexioapp.org

POSTGRES_DB=nexio_imdb
POSTGRES_USER=nexio_imdb
POSTGRES_PASSWORD=CHANGE_ME

DATABASE_URL=postgres://nexio_imdb:CHANGE_ME@127.0.0.1:5432/nexio_imdb?sslmode=disable

GOOGLE_CLIENT_ID=YOUR_GOOGLE_CLIENT_ID
GOOGLE_CLIENT_SECRET=YOUR_GOOGLE_CLIENT_SECRET
GOOGLE_REDIRECT_URL=https://api.nexioapp.org/auth/callback
ALLOWED_GOOGLE_EMAILS=user1@nexioapp.org,user2@nexioapp.org

SESSION_COOKIE_SECRET=CHANGE_ME_TO_64_PLUS_RANDOM_BYTES
SESSION_COOKIE_NAME=nexio_imdb_session
API_KEY_PEPPER=CHANGE_ME_TO_LONG_RANDOM_SECRET

IMDB_DATASET_BASE_URL=https://datasets.imdbws.com
IMDB_SYNC_INTERVAL_HOURS=12
IMDB_RUN_ON_STARTUP=true
BULK_JOB_POLL_INTERVAL_SECONDS=15
HTTP_TIMEOUT_MINUTES=30
```

Important notes:

- `API_BASE_URL` is required by the Nuxt build and Nitro route proxy.
- `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD` are useful if you also bootstrap Postgres or reuse the same values in Docker Compose.
- `DATABASE_URL` is required by:
  - Go API
  - Go worker
  - Nuxt server-side routes
- `SESSION_COOKIE_SECRET` protects encrypted OAuth verifier cookies.
- `API_KEY_PEPPER` must remain stable across deployments or existing API keys will stop validating.
- `ALLOWED_GOOGLE_EMAILS` is a strict allowlist.

Lock down permissions:

```bash
sudo chown root:nexio /etc/nexio-imdb.env
sudo chmod 640 /etc/nexio-imdb.env
```

## Build And Release

From `/opt/nexio-imdb/current`:

```bash
npm ci
npm run build:docs
npm run build:web
npm run test:api
```

Build the Go binaries:

```bash
mkdir -p bin
go build -o bin/nexio-api ./apps/api/cmd/api
go build -o bin/nexio-worker ./apps/api/cmd/worker
```

The Nuxt production server entrypoint will be:

- `/opt/nexio-imdb/current/apps/web/.output/server/index.mjs`

## systemd Services

Create the API service:

- `/etc/systemd/system/nexio-imdb-api.service`

```ini
[Unit]
Description=Nexio IMDb API
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=nexio
Group=nexio
WorkingDirectory=/opt/nexio-imdb/current
EnvironmentFile=/etc/nexio-imdb.env
ExecStart=/opt/nexio-imdb/current/bin/nexio-api
Restart=always
RestartSec=5
StandardOutput=append:/var/log/nexio-imdb/api.log
StandardError=append:/var/log/nexio-imdb/api.log

[Install]
WantedBy=multi-user.target
```

Create the worker service:

- `/etc/systemd/system/nexio-imdb-worker.service`

```ini
[Unit]
Description=Nexio IMDb Worker
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=nexio
Group=nexio
WorkingDirectory=/opt/nexio-imdb/current
EnvironmentFile=/etc/nexio-imdb.env
ExecStart=/opt/nexio-imdb/current/bin/nexio-worker
Restart=always
RestartSec=5
StandardOutput=append:/var/log/nexio-imdb/worker.log
StandardError=append:/var/log/nexio-imdb/worker.log

[Install]
WantedBy=multi-user.target
```

Create the Nuxt service:

- `/etc/systemd/system/nexio-imdb-web.service`

```ini
[Unit]
Description=Nexio IMDb Web Portal
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=nexio
Group=nexio
WorkingDirectory=/opt/nexio-imdb/current/apps/web
EnvironmentFile=/etc/nexio-imdb.env
Environment=NODE_ENV=production
ExecStart=/usr/bin/node /opt/nexio-imdb/current/apps/web/.output/server/index.mjs
Restart=always
RestartSec=5
StandardOutput=append:/var/log/nexio-imdb/web.log
StandardError=append:/var/log/nexio-imdb/web.log

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now nexio-imdb-api
sudo systemctl enable --now nexio-imdb-worker
sudo systemctl enable --now nexio-imdb-web
```

## Reverse Proxy And TLS

Create a Caddy config:

- `/etc/caddy/Caddyfile`

```caddy
api.nexioapp.org {
    encode zstd gzip

    handle /v1/* {
        reverse_proxy 127.0.0.1:8080
    }

    handle /healthz {
        reverse_proxy 127.0.0.1:8080
    }

    handle /readyz {
        reverse_proxy 127.0.0.1:8080
    }

    handle {
        reverse_proxy 127.0.0.1:3000
    }
}
```

Reload Caddy:

```bash
sudo systemctl reload caddy
```

This example routes the API paths directly at the proxy layer so the traffic split is explicit and easy to reason about.

## First Deployment Checklist

Run this in order:

1. Provision the server and DNS.
2. Configure Google OAuth redirect URI.
3. Install Go, Node, Postgres, and Caddy.
4. Create the Postgres database.
5. Clone the repo to `/opt/nexio-imdb/current`.
6. Create `/etc/nexio-imdb.env`.
7. Apply [`infra/postgres/migrations/0001_init.sql`](/Users/jneerdael/Scripts/imdb-scrape/infra/postgres/migrations/0001_init.sql).
8. Run `npm ci`.
9. Run `npm run build:docs`.
10. Run `npm run build:web`.
11. Run `npm run test:api`.
12. Build the Go binaries.
13. Install the three systemd unit files.
14. Start the services.
15. Configure Caddy and verify TLS.
16. Sign in through Google with one approved email.
17. Generate an API key from the portal.
18. Verify `/v1/meta/stats` with that key.

## Health Checks

Validate the stack:

```bash
curl -I https://api.nexioapp.org/
curl https://api.nexioapp.org/healthz
curl https://api.nexioapp.org/readyz
```

Check services:

```bash
sudo systemctl status nexio-imdb-api
sudo systemctl status nexio-imdb-worker
sudo systemctl status nexio-imdb-web
```

Tail logs:

```bash
sudo tail -f /var/log/nexio-imdb/api.log
sudo tail -f /var/log/nexio-imdb/worker.log
sudo tail -f /var/log/nexio-imdb/web.log
```

Validate dataset ingestion:

```sql
SELECT id, status, is_active, imported_at, completed_at, title_count, name_count, rating_count
FROM imdb_snapshots
ORDER BY id DESC
LIMIT 5;
```

Validate queued bulk jobs:

```sql
SELECT id, job_type, status, created_at, updated_at, completed_at
FROM bulk_jobs
ORDER BY created_at DESC
LIMIT 20;
```

## Upgrade Procedure

Deploy a new version:

```bash
cd /opt/nexio-imdb/current
sudo -u nexio git fetch --all
sudo -u nexio git checkout <target-commit-or-branch>
npm ci
psql "$DATABASE_URL" -f infra/postgres/migrations/0001_init.sql
npm run build:docs
npm run build:web
npm run test:api
go build -o bin/nexio-api ./apps/api/cmd/api
go build -o bin/nexio-worker ./apps/api/cmd/worker
sudo systemctl restart nexio-imdb-api
sudo systemctl restart nexio-imdb-worker
sudo systemctl restart nexio-imdb-web
```

Then verify:

```bash
curl https://api.nexioapp.org/healthz
curl https://api.nexioapp.org/readyz
```

## Rollback Procedure

If a release fails:

1. Checkout the previous known-good commit.
2. Rebuild:
   - Go binaries
   - Nuxt production bundle
   - docs HTML
3. Restart the three services.

Example:

```bash
cd /opt/nexio-imdb/current
sudo -u nexio git checkout <previous-good-commit>
npm ci
npm run build:docs
npm run build:web
go build -o bin/nexio-api ./apps/api/cmd/api
go build -o bin/nexio-worker ./apps/api/cmd/worker
sudo systemctl restart nexio-imdb-api nexio-imdb-worker nexio-imdb-web
```

Note:

- do not rotate `SESSION_COOKIE_SECRET` or `API_KEY_PEPPER` during a normal rollback
- if you change `API_KEY_PEPPER`, existing API keys become invalid
- if you change `SESSION_COOKIE_SECRET`, current OAuth verifier cookies become invalid and in-flight logins will fail

## Backups

At minimum, back up:

- the PostgreSQL database
- `/etc/nexio-imdb.env`
- the deployed git revision or release artifact

Example database backup:

```bash
pg_dump "postgres://nexio_imdb:CHANGE_ME@127.0.0.1:5432/nexio_imdb?sslmode=disable" \
  --format=custom \
  --file=/var/backups/nexio_imdb_$(date +%F_%H%M%S).dump
```

Recommended:

- nightly Postgres backups
- off-host backup retention
- monitoring on disk usage, because IMDb snapshot imports are large

## Operational Notes

- The worker imports snapshots only when IMDb `ETag` or `Last-Modified` values change.
- The worker performs bulk-job processing continuously and no longer blocks on snapshot sync windows.
- Async bulk jobs are stored in `bulk_jobs` and results remain queryable until expiry.
- The importer currently relies on IMDb dataset endpoints supporting `HEAD`, `ETag`, and `Last-Modified`.
- The initial import can take substantial time and bandwidth because the full dataset set is downloaded and normalized.

## Recommended Next Steps

After the first successful deployment, add:

- real migration tooling instead of replaying one SQL file
- log shipping
- uptime monitoring on `/readyz`
- database backup automation
- service metrics
- staged release directories like `/opt/nexio-imdb/releases/<timestamp>` with a `current` symlink
