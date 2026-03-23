# Nexio IMDb Internal API Platform

Internal IMDb dataset ingestion and query platform for `api.nexioapp.org`.

## Monorepo Layout

- `apps/api`: Go API, ingestion pipeline, worker, and database migrations
- `apps/web`: Nuxt 4 portal with Google OIDC auth and internal docs
- `docs`: API Blueprint contract and generated documentation
- `infra/postgres`: local Postgres bootstrap assets
- `scripts`: helper scripts for local development and dataset operations

## Local Development

1. Copy the relevant env examples.
2. Start Postgres with Docker Compose.
3. Run the Go API and Nuxt web app.
4. Generate docs from `docs/api.apib`.

The implementation is intended for internal, non-commercial use only.
