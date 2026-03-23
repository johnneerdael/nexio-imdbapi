# Nexio IMDb Internal API Platform

Internal IMDb dataset ingestion and query platform for `api.nexioapp.org`.

The platform is intended for internal, non-commercial use against the public IMDb dataset snapshots.

## Monorepo Layout

- `apps/api`: Go API, queued bulk-job worker, dataset ingestion pipeline, and migrations
- `apps/web`: Nuxt 4 portal with Google OIDC auth and internal docs
- `docs`: API Blueprint contract and generated documentation
- `infra/postgres`: local Postgres bootstrap assets

## Local Development

1. Copy `.env.example` to your local environment file and fill in Google OAuth and secret values.
2. Start Postgres with `docker compose up -d postgres`.
3. Apply [`infra/postgres/migrations/0001_init.sql`](/Users/jneerdael/Scripts/imdb-scrape/infra/postgres/migrations/0001_init.sql) to the database.
4. Run `npm run dev:api` for the Go query API.
5. Run `npm run dev:worker` for scheduled IMDb imports and queued bulk-job processing.
6. Run `npm run dev:web` for the Nuxt portal.
7. Run `npm run build:docs` to render the API Blueprint HTML docs consumed by the portal.

## Runtime Notes

- The worker checks IMDb dataset metadata and imports only when upstream `ETag` or `Last-Modified` values change.
- Imports stream gzip TSV snapshots directly into temporary Postgres staging tables, then normalize inside a transaction before promoting the snapshot.
- Bulk endpoints under `/v1/*/bulk` are synchronous up to 250 identifiers. `/v1/bulk/jobs` queues async bulk work up to 10,000 identifiers for the worker.
- The portal uses direct Google OIDC in Nuxt and stores users, sessions, and API keys in Postgres.

## Deployment

- Production deployment guide: [`docs/deployment.md`](/Users/jneerdael/Scripts/imdb-scrape/docs/deployment.md)
- Docker Compose deployment guide: [`docs/docker-compose-deployment.md`](/Users/jneerdael/Scripts/imdb-scrape/docs/docker-compose-deployment.md)
- Proxy overlays:
  - [`docker-compose.caddy.yml`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.caddy.yml)
  - [`docker-compose.nginx.yml`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.nginx.yml)
  - [`docker-compose.traefik.yml`](/Users/jneerdael/Scripts/imdb-scrape/docker-compose.traefik.yml)
