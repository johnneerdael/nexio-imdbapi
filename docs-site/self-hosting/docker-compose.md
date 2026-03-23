---
title: Docker Compose
description: Compose files, startup commands, env files, and port layout for self-hosting.
---

# Docker Compose

The repository includes a Compose-based deployment for the full stack. It is the fastest way to get the API, worker, portal, and a proxy running together.

## Compose Files

Use these files together:

- `docker-compose.deploy.yml` for the shared application services
- `docker-compose.caddy.yml` for Caddy
- `docker-compose.nginx.yml` for Nginx
- `docker-compose.traefik.yml` for Traefik
- `docker-compose.host-proxy.override.yml.example` for host-managed proxy setups
- `docker-compose.host-ports.override.yml.example` for loopback port exposure

The app stack itself includes:

- `postgres`
- `migrate`
- `api`
- `worker`
- `web`

## Environment File

Copy the example env file and fill in the values:

```bash
cp .env.compose.example .env.compose
```

The important fields are:

- `APP_DOMAIN`
- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `ALLOWED_GOOGLE_EMAILS`
- `SESSION_COOKIE_SECRET`
- `API_KEY_PEPPER`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

`APP_DOMAIN` must be a bare host name without `http://` or `https://`.

## Start Commands

Run the stack with the proxy you want:

```bash
docker compose --env-file .env.compose -f docker-compose.deploy.yml -f docker-compose.caddy.yml up -d --build
```

```bash
docker compose --env-file .env.compose -f docker-compose.deploy.yml -f docker-compose.nginx.yml up -d --build
```

```bash
docker compose --env-file .env.compose -f docker-compose.deploy.yml -f docker-compose.traefik.yml up -d --build
```

If you want a host-managed proxy, use the loopback override and terminate TLS outside Compose.

## Port Layout

The internal service ports are fixed in the stack:

- API: `8080`
- Portal: `3000`

The proxy overlay or host proxy decides whether those services are exposed directly or only through the edge proxy.

## Upgrade Flow

For an in-place refresh:

1. update the checkout
2. update the env file if needed
3. rebuild the images
4. restart the stack
5. check the health endpoints

Use [Operations runbook](../operations/runbook.md) for the exact verification sequence after deployment.

