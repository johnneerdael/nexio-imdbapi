# Docker Compose Deployment Guide

This guide deploys the full stack with Docker Compose:

- PostgreSQL 16
- automatic schema patch container
- Go API
- Go worker
- Nuxt web portal
- your choice of reverse proxy:
  - Caddy
  - Nginx
  - Traefik

It is designed for a host serving `https://api.nexioapp.org`.

## Compose Files

Shared application stack:

- [`docker-compose.deploy.yml`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.deploy.yml)

Proxy overlays:

- [`docker-compose.caddy.yml`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.caddy.yml)
- [`docker-compose.nginx.yml`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.nginx.yml)
- [`docker-compose.traefik.yml`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.traefik.yml)

Proxy configs:

- [`infra/caddy/Caddyfile`](/Users/jneerdael/Scripts/imdb-scrape/infra/caddy/Caddyfile)
- [`infra/nginx/default.conf`](/Users/jneerdael/Scripts/imdb-scrape/infra/nginx/default.conf)

Optional host-managed proxy override:

- [`docker-compose.host-proxy.override.yml.example`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.host-proxy.override.yml.example)

## How The Stack Is Split

The base Compose file contains only the application services:

- `postgres`
- `migrate`
- `api`
- `worker`
- `web`

The proxy overlay adds the edge service and public ports.

Required routing split for every proxy:

- `/v1/*` -> Go API
- `/healthz` -> Go API
- `/readyz` -> Go API
- everything else -> Nuxt web app

## Environment File

Copy the example:

```bash
cp .env.compose.example .env.compose
```

Edit `.env.compose`:

```dotenv
APP_DOMAIN=api.nexioapp.org
TRAEFIK_ACME_EMAIL=infra@nexioapp.org

POSTGRES_DB=nexio_imdb
POSTGRES_USER=nexio_imdb
POSTGRES_PASSWORD=CHANGE_ME

GOOGLE_CLIENT_ID=YOUR_GOOGLE_CLIENT_ID
GOOGLE_CLIENT_SECRET=YOUR_GOOGLE_CLIENT_SECRET
ALLOWED_GOOGLE_EMAILS=user1@nexioapp.org,user2@nexioapp.org

SESSION_COOKIE_SECRET=CHANGE_ME_TO_A_LONG_RANDOM_SECRET
SESSION_COOKIE_NAME=nexio_imdb_session
API_KEY_PEPPER=CHANGE_ME_TO_A_LONG_RANDOM_SECRET

IMDB_DATASET_BASE_URL=https://datasets.imdbws.com
IMDB_SYNC_INTERVAL_HOURS=12
IMDB_RUN_ON_STARTUP=true
BULK_JOB_POLL_INTERVAL_SECONDS=15
HTTP_TIMEOUT_MINUTES=30
```

Important:

- `SESSION_COOKIE_SECRET` must stay stable between deployments.
- `API_KEY_PEPPER` must stay stable between deployments.
- if `API_KEY_PEPPER` changes, all existing API keys become invalid.
- `TRAEFIK_ACME_EMAIL` is only used by the Traefik overlay.

## Start With Caddy

Use this when you want Docker Compose to handle the full public edge with automatic TLS:

```bash
docker compose \
  --env-file .env.compose \
  -f docker-compose.deploy.yml \
  -f docker-compose.caddy.yml \
  up -d --build
```

This publishes:

- `80/tcp`
- `443/tcp`

Caddy terminates TLS and routes:

- `/v1/*`, `/healthz`, `/readyz` -> `api:8080`
- everything else -> `web:3000`

## Start With Traefik

Use this when you want a Compose-managed proxy with automatic Let's Encrypt support and Docker-native routing:

```bash
docker compose \
  --env-file .env.compose \
  -f docker-compose.deploy.yml \
  -f docker-compose.traefik.yml \
  up -d --build
```

This publishes:

- `80/tcp`
- `443/tcp`

Notes:

- Traefik redirects HTTP to HTTPS automatically.
- The ACME account email comes from `TRAEFIK_ACME_EMAIL`.
- The API router is given higher priority than the web router so `/v1/*`, `/healthz`, and `/readyz` always reach the Go API.

## Start With Nginx

Use this when you want a simple Compose-managed HTTP reverse proxy:

```bash
docker compose \
  --env-file .env.compose \
  -f docker-compose.deploy.yml \
  -f docker-compose.nginx.yml \
  up -d --build
```

This publishes:

- `80/tcp`

Notes:

- The bundled Nginx overlay is HTTP-only.
- If you need public HTTPS with Nginx, terminate TLS in front of it or extend the overlay with your own certificate mount and `listen 443 ssl` server block.

## Use A Custom Host-Level Caddy Installation

If you already run Caddy on the host and only want Compose to run the app stack, expose the internal services on loopback:

```bash
cp docker-compose.host-proxy.override.yml.example docker-compose.host-proxy.override.yml
docker compose \
  --env-file .env.compose \
  -f docker-compose.deploy.yml \
  -f docker-compose.host-proxy.override.yml \
  up -d --build
```

That publishes:

- `127.0.0.1:8080` -> Go API
- `127.0.0.1:3000` -> Nuxt web

Example host-level Caddy config:

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

## Changing API Or Web Ports

There are two different port layers:

1. internal container ports
2. host-exposed ports

Internal container ports:

- the API listens on `8080`
- the Nuxt app listens on `3000`

Those values are baked into the proxy configs and service wiring:

- [`docker-compose.deploy.yml`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.deploy.yml)
- [`infra/caddy/Caddyfile`](/Users/jneerdael/Scripts/imdb-scrape/infra/caddy/Caddyfile)
- [`infra/nginx/default.conf`](/Users/jneerdael/Scripts/imdb-scrape/infra/nginx/default.conf)
- [`docker-compose.traefik.yml`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.traefik.yml)

If you change the API port:

- update `API_ADDRESS` in the `api` service
- update `API_BASE_URL` in the `web` build arg and env
- update every proxy target that points to `api:8080`

If you change the web port:

- update the container's listening port in the web image/runtime
- update every proxy target that points to `web:3000`

If you only need different host-exposed ports for a host-level proxy, change the `ports` section in your override file instead of changing the internal service ports.

## Health Verification

Check public endpoints:

```bash
curl https://api.nexioapp.org/healthz
curl https://api.nexioapp.org/readyz
```

For the HTTP-only Nginx overlay:

```bash
curl http://api.nexioapp.org/healthz
curl http://api.nexioapp.org/readyz
```

Check service state:

```bash
docker compose --env-file .env.compose -f docker-compose.deploy.yml ps
```

Inspect logs by service:

```bash
docker compose --env-file .env.compose -f docker-compose.deploy.yml logs -f api
docker compose --env-file .env.compose -f docker-compose.deploy.yml logs -f worker
docker compose --env-file .env.compose -f docker-compose.deploy.yml logs -f web
```

Add the active proxy overlay file to the command when you want to inspect that proxy service too.

## Database Migration Behavior

The automated SQL patch is handled by the `migrate` service:

- it waits for Postgres readiness
- it runs `psql -f /migrations/0001_init.sql`
- it must complete successfully before the app services start

To rerun only the migration:

```bash
docker compose --env-file .env.compose -f docker-compose.deploy.yml run --rm migrate
```

## Updating The Deployment

After pulling new code:

```bash
git pull
docker compose \
  --env-file .env.compose \
  -f docker-compose.deploy.yml \
  -f docker-compose.caddy.yml \
  up -d --build
```

Replace the last file with the proxy overlay you actually use.

## Rolling Back

Return to a previous git revision and rebuild:

```bash
git checkout <previous-good-commit>
docker compose \
  --env-file .env.compose \
  -f docker-compose.deploy.yml \
  -f docker-compose.caddy.yml \
  up -d --build
```

Do not rotate these during rollback unless intended:

- `SESSION_COOKIE_SECRET`
- `API_KEY_PEPPER`

## Stopping The Stack

Stop containers:

```bash
docker compose \
  --env-file .env.compose \
  -f docker-compose.deploy.yml \
  -f docker-compose.caddy.yml \
  down
```

Stop and remove volumes too:

```bash
docker compose \
  --env-file .env.compose \
  -f docker-compose.deploy.yml \
  -f docker-compose.caddy.yml \
  down -v
```

Replace the last file with the proxy overlay you actually use.

Warning:

- `down -v` deletes the Postgres data volume.

## Data Persistence

The base stack always uses:

- `postgres_data`

The proxy overlays add their own volumes:

- Caddy: `caddy_data`, `caddy_config`
- Traefik: `traefik_letsencrypt`

Back up the database before destructive maintenance.
