# Docker Compose Deployment Guide

This guide deploys the full stack with Docker Compose:

- PostgreSQL 16
- automatic schema patch container
- Go API
- Go worker
- Nuxt web portal
- Caddy reverse proxy with TLS

It is designed for a host serving `https://api.nexioapp.org`.

## Files Added For This Flow

- [`docker-compose.deploy.yml`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.deploy.yml)
- [`.env.compose.example`](/Users/jneerdael/Scripts/imdb-scrape/.env.compose.example)
- [`apps/api/Dockerfile`](/Users/jneerdael/Scripts/imdb-scrape/apps/api/Dockerfile)
- [`apps/web/Dockerfile`](/Users/jneerdael/Scripts/imdb-scrape/apps/web/Dockerfile)
- [`infra/caddy/Caddyfile`](/Users/jneerdael/Scripts/imdb-scrape/infra/caddy/Caddyfile)

## How It Works

Service order:

1. `postgres` starts and becomes healthy.
2. `migrate` waits for Postgres and runs:
   - [`infra/postgres/migrations/0001_init.sql`](/Users/jneerdael/Scripts/imdb-scrape/infra/postgres/migrations/0001_init.sql)
3. `api`, `worker`, and `web` start only after the migration container exits successfully.
4. `caddy` exposes ports `80` and `443`.

The migration step is safe to rerun because the schema SQL is written with `IF NOT EXISTS` and `CREATE OR REPLACE VIEW`.

## Prerequisites

- Docker Engine with Compose plugin installed
- DNS for `api.nexioapp.org` pointing to the server
- Google OAuth client configured with:
  - redirect URI: `https://api.nexioapp.org/auth/callback`

## Environment File

Copy the example:

```bash
cp .env.compose.example .env.compose
```

Edit `.env.compose`:

```dotenv
APP_DOMAIN=api.nexioapp.org

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

## Start The Stack

Build and launch:

```bash
docker compose --env-file .env.compose -f docker-compose.deploy.yml up -d --build
```

Watch startup:

```bash
docker compose --env-file .env.compose -f docker-compose.deploy.yml logs -f
```

## Health Verification

Check public endpoints:

```bash
curl https://api.nexioapp.org/healthz
curl https://api.nexioapp.org/readyz
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
docker compose --env-file .env.compose -f docker-compose.deploy.yml logs -f caddy
docker compose --env-file .env.compose -f docker-compose.deploy.yml logs -f migrate
```

## First Login

After the stack is live:

1. Open `https://api.nexioapp.org/`
2. Sign in with an approved Google account
3. Generate an API key from the portal
4. Use it against:
   - `https://api.nexioapp.org/v1/meta/stats`

Example:

```bash
curl -H "X-API-Key: YOUR_KEY" https://api.nexioapp.org/v1/meta/stats
```

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
docker compose --env-file .env.compose -f docker-compose.deploy.yml up -d --build
```

This rebuilds images, reruns the migration service, and recreates changed containers.

## Rolling Back

Return to a previous git revision and rebuild:

```bash
git checkout <previous-good-commit>
docker compose --env-file .env.compose -f docker-compose.deploy.yml up -d --build
```

Do not rotate these during rollback unless intended:

- `SESSION_COOKIE_SECRET`
- `API_KEY_PEPPER`

## Stopping The Stack

Stop containers:

```bash
docker compose --env-file .env.compose -f docker-compose.deploy.yml down
```

Stop and remove volumes too:

```bash
docker compose --env-file .env.compose -f docker-compose.deploy.yml down -v
```

Warning:

- `down -v` deletes the Postgres data volume.

## Data Persistence

Compose volumes used:

- `postgres_data`
- `caddy_data`
- `caddy_config`

`postgres_data` holds the actual application database. Back it up before destructive maintenance.

## Recommended Production Notes

- Keep the repo on a stable tagged release or known commit.
- Put `.env.compose` outside version control.
- Restrict server firewall to:
  - `80/tcp`
  - `443/tcp`
- Do not expose Postgres publicly unless required.
- Expect the first IMDb import to take time and bandwidth.

## Optional Direct Service Access

This Compose file does not expose:

- API port `8080`
- web port `3000`
- Postgres port `5432`

That is intentional for a cleaner production shape behind Caddy. If you want local direct access for debugging, add temporary `ports` mappings to the relevant services.
