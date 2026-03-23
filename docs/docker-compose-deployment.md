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
- [`docker-compose.host-ports.override.yml.example`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.host-ports.override.yml.example)

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

- `APP_DOMAIN` must be the bare host name only, for example `api.nexioapp.org`.
- Do not include `http://` or `https://` in `APP_DOMAIN`.
- Compose uses `APP_DOMAIN` to derive `APP_BASE_URL=https://${APP_DOMAIN}` and `GOOGLE_REDIRECT_URL=https://${APP_DOMAIN}/auth/callback`, and the bundled Caddy config also expects a host name instead of a full URL.
- the `web` service maps these values to `NUXT_*` container env vars internally because Nuxt runtime config expects `NUXT_`-prefixed overrides at production runtime
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

## Use A Separate Caddy Stack On A Shared Docker Network

This is often cleaner than loopback port publishing when Caddy already runs in its own Compose project.

Create a shared external Docker network once:

```bash
docker network create caddy_net
```

Then connect only the API and web containers to that shared network in your app stack:

```yaml
services:
  api:
    networks:
      - default
      - caddy_net

  web:
    networks:
      - default
      - caddy_net

networks:
  caddy_net:
    external: true
```

The full app stack shape looks like this:

```yaml
services:
  postgres:
    image: postgres:16-alpine
    restart: unless-stopped
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 10s
      timeout: 5s
      retries: 12

  migrate:
    image: postgres:16-alpine
    restart: "no"
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      PGPASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
    volumes:
      - ./infra/postgres/migrations:/migrations:ro
    command:
      - /bin/sh
      - -lc
      - >
        until pg_isready -h postgres -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"; do
          sleep 2;
        done &&
        psql -v ON_ERROR_STOP=1 -h postgres -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"
        -f /migrations/0001_init.sql

  api:
    build:
      context: .
      dockerfile: apps/api/Dockerfile
      target: api-runtime
    restart: unless-stopped
    depends_on:
      migrate:
        condition: service_completed_successfully
    environment:
      API_ADDRESS: :8080
      DATABASE_URL: postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}?sslmode=disable
      API_KEY_PEPPER: ${API_KEY_PEPPER}
      IMDB_DATASET_BASE_URL: ${IMDB_DATASET_BASE_URL}
      IMDB_SYNC_INTERVAL_HOURS: ${IMDB_SYNC_INTERVAL_HOURS}
      IMDB_RUN_ON_STARTUP: ${IMDB_RUN_ON_STARTUP}
      BULK_JOB_POLL_INTERVAL_SECONDS: ${BULK_JOB_POLL_INTERVAL_SECONDS}
      HTTP_TIMEOUT_MINUTES: ${HTTP_TIMEOUT_MINUTES}
    networks:
      - default
      - caddy_net

  worker:
    build:
      context: .
      dockerfile: apps/api/Dockerfile
      target: worker-runtime
    restart: unless-stopped
    depends_on:
      migrate:
        condition: service_completed_successfully
    environment:
      DATABASE_URL: postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}?sslmode=disable
      API_KEY_PEPPER: ${API_KEY_PEPPER}
      IMDB_DATASET_BASE_URL: ${IMDB_DATASET_BASE_URL}
      IMDB_SYNC_INTERVAL_HOURS: ${IMDB_SYNC_INTERVAL_HOURS}
      IMDB_RUN_ON_STARTUP: ${IMDB_RUN_ON_STARTUP}
      BULK_JOB_POLL_INTERVAL_SECONDS: ${BULK_JOB_POLL_INTERVAL_SECONDS}
      HTTP_TIMEOUT_MINUTES: ${HTTP_TIMEOUT_MINUTES}

  web:
    build:
      context: .
      dockerfile: apps/web/Dockerfile
      args:
        API_BASE_URL: http://api:8080
    restart: unless-stopped
    depends_on:
      migrate:
        condition: service_completed_successfully
    environment:
      NODE_ENV: production
      API_BASE_URL: http://api:8080
      APP_BASE_URL: https://${APP_DOMAIN}
      DATABASE_URL: postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}?sslmode=disable
      NUXT_API_BASE_URL: http://api:8080
      NUXT_PUBLIC_API_BASE_URL: http://api:8080
      NUXT_APP_BASE_URL: https://${APP_DOMAIN}
      NUXT_DATABASE_URL: postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}?sslmode=disable
      GOOGLE_CLIENT_ID: ${GOOGLE_CLIENT_ID}
      GOOGLE_CLIENT_SECRET: ${GOOGLE_CLIENT_SECRET}
      GOOGLE_REDIRECT_URL: https://${APP_DOMAIN}/auth/callback
      NUXT_GOOGLE_CLIENT_ID: ${GOOGLE_CLIENT_ID}
      NUXT_GOOGLE_CLIENT_SECRET: ${GOOGLE_CLIENT_SECRET}
      NUXT_GOOGLE_REDIRECT_URL: https://${APP_DOMAIN}/auth/callback
      ALLOWED_GOOGLE_EMAILS: ${ALLOWED_GOOGLE_EMAILS}
      NUXT_ALLOWED_GOOGLE_EMAILS: ${ALLOWED_GOOGLE_EMAILS}
      SESSION_COOKIE_SECRET: ${SESSION_COOKIE_SECRET}
      SESSION_COOKIE_NAME: ${SESSION_COOKIE_NAME}
      NUXT_SESSION_COOKIE_SECRET: ${SESSION_COOKIE_SECRET}
      NUXT_SESSION_COOKIE_NAME: ${SESSION_COOKIE_NAME}
      API_KEY_PEPPER: ${API_KEY_PEPPER}
      NUXT_API_KEY_PEPPER: ${API_KEY_PEPPER}
    networks:
      - default
      - caddy_net

volumes:
  postgres_data:

networks:
  caddy_net:
    external: true
```

Then your separate Caddy stack joins the same external network:

```yaml
services:
  caddy:
    image: caddy:2.10-alpine
    cap_add:
      - NET_BIND_SERVICE
    restart: unless-stopped
    ports:
      - "80:80"
      - "80:80/udp"
      - "443:443"
      - "443:443/udp"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy_data:/data
      - caddy_config:/config
    environment:
      CF_API_TOKEN: ${CF_API_TOKEN}
    networks:
      - default
      - caddy_net

volumes:
  caddy_data:
  caddy_config:

networks:
  caddy_net:
    external: true
```

That translates directly to a Caddyfile like this:

```caddy
api.nexioapp.org {
    encode zstd gzip

    handle /v1/* {
        reverse_proxy api:8080
    }

    handle /healthz {
        reverse_proxy api:8080
    }

    handle /readyz {
        reverse_proxy api:8080
    }

    handle {
        reverse_proxy web:3000
    }
}
```

Why this works:

- both Compose projects join the same external Docker network, `caddy_net`
- Docker DNS resolves the service names `api` and `web` on that shared network
- Caddy can therefore reach `api:8080` and `web:3000` directly without published loopback ports

This is cleaner because:

- the API and portal do not need host `ports:` mappings
- Caddy stays isolated in its own stack
- routing remains explicit and easy to debug

Important:

- do not hardcode DNS provider or TLS API credentials directly in the Caddyfile
- pass them through environment variables or Docker secrets instead
- if you use OVH or Cloudflare DNS challenge, keep those credentials in the Caddy container env, not in Git-tracked config

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

### Option 1: Change Only The Host-Published Ports

This is the common case.

Use it when:

- you want direct browser or curl access to the API and web services
- you use a host-level Caddy or Nginx install
- you do not want to touch the internal service wiring

Example override:

- [`docker-compose.host-ports.override.yml.example`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.host-ports.override.yml.example)

```yaml
services:
  api:
    ports:
      - "18080:8080"

  web:
    ports:
      - "13000:3000"
```

Start with it:

```bash
cp docker-compose.host-ports.override.yml.example docker-compose.host-ports.override.yml
docker compose \
  --env-file .env.compose \
  -f docker-compose.deploy.yml \
  -f docker-compose.host-ports.override.yml \
  up -d --build
```

Result:

- the API is available on host port `18080`
- the web app is available on host port `13000`
- inside Docker, the API still listens on `api:8080`
- inside Docker, the web app still listens on `web:3000`

No proxy config changes are required for this option.

### Option 2: Change The Internal API Port

Use this only if you really want the API container itself to listen on a different port.

Example: move the API from `8080` to `18080`.

Update [`docker-compose.deploy.yml`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.deploy.yml):

```yaml
api:
  environment:
    API_ADDRESS: :18080

web:
  build:
    args:
      API_BASE_URL: http://api:18080
  environment:
    API_BASE_URL: http://api:18080
```

Then update every proxy target:

- [`infra/caddy/Caddyfile`](/Users/jneerdael/Scripts/imdb-scrape/infra/caddy/Caddyfile): change `api:8080` to `api:18080`
- [`infra/nginx/default.conf`](/Users/jneerdael/Scripts/imdb-scrape/infra/nginx/default.conf): change `http://api:8080` to `http://api:18080`
- [`docker-compose.traefik.yml`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.traefik.yml): change `traefik.http.services.nexio-api.loadbalancer.server.port` from `8080` to `18080`

If you also expose the API directly on the host, update the host mapping too:

```yaml
services:
  api:
    ports:
      - "18080:18080"
```

### Option 3: Change The Internal Web Port

Use this only if you really want the Nuxt container itself to listen on a different port.

Example: move the web app from `3000` to `3100`.

Update [`docker-compose.deploy.yml`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.deploy.yml):

```yaml
web:
  environment:
    NODE_ENV: production
    PORT: 3100
```

Then update every proxy target:

- [`infra/caddy/Caddyfile`](/Users/jneerdael/Scripts/imdb-scrape/infra/caddy/Caddyfile): change `web:3000` to `web:3100`
- [`infra/nginx/default.conf`](/Users/jneerdael/Scripts/imdb-scrape/infra/nginx/default.conf): change `http://web:3000` to `http://web:3100`
- [`docker-compose.traefik.yml`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.traefik.yml): change `traefik.http.services.nexio-web.loadbalancer.server.port` from `3000` to `3100`

If you also expose the web app directly on the host, update the host mapping too:

```yaml
services:
  web:
    ports:
      - "13000:3100"
```

### Option 4: Host-Level Proxy On Custom Ports

If you use your own host-level Caddy or Nginx and want the containers exposed on different loopback ports, change the override file only.

Example:

```yaml
services:
  api:
    ports:
      - "127.0.0.1:18080:8080"

  web:
    ports:
      - "127.0.0.1:13000:3000"
```

Then point your host proxy to those ports:

```caddy
api.nexioapp.org {
    handle /v1/* {
        reverse_proxy 127.0.0.1:18080
    }

    handle /healthz {
        reverse_proxy 127.0.0.1:18080
    }

    handle /readyz {
        reverse_proxy 127.0.0.1:18080
    }

    handle {
        reverse_proxy 127.0.0.1:13000
    }
}
```

Summary:

- change only host-published ports when possible
- change internal ports only if you also update `API_BASE_URL` and every proxy target
- the proxy overlays assume `api:8080` and `web:3000` until you edit them

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
