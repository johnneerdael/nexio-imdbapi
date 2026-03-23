---
title: Getting Started
description: Quick orientation for the Nexio IMDb API platform, portal, and docs site.
---

# Getting Started

This platform has three layers:

- the Go API under `/v1/*`
- the Nuxt portal for authenticated humans
- the deployment layer for self-hosting and edge routing

Start here if you want the shortest path from a fresh checkout to a working mental model.

## What This Platform Does

The API exposes IMDb dataset snapshots, title and name lookups, series episode views, alternate-title search, and queued bulk jobs. The portal sits in front of that API and adds Google sign-in, session management, and API key lifecycle controls.

The docs are split the same way:

- [API overview](../api/overview.md)
- [API authentication](../api/authentication.md)
- [Bulk jobs](../api/bulk-jobs.md)
- [Portal architecture](../frontend/portal.md)
- [Google auth setup](../frontend/google-auth.md)
- [Self-hosting overview](../self-hosting/overview.md)
- [Docker Compose](../self-hosting/docker-compose.md)
- [Proxy choices](../self-hosting/proxies.md)
- [Secrets](../security/secrets.md)
- [Operations runbook](../operations/runbook.md)

## Core Terms

Snapshot:
: A loaded IMDb dataset version in Postgres. The portal shows the latest snapshot metadata and aggregate counts.

Session:
: A browser session created after Google OIDC login. The session cookie is opaque and stored server-side with a hash.

API key:
: A one-time-visible credential minted from the portal. It is used by non-browser clients to call `/v1/*`.

Bulk job:
: An async execution record for large or long-running bulk operations. The API persists the job and the worker completes it.

## Typical Workflows

If you are consuming the API from a script or another service:

1. Read [API authentication](../api/authentication.md).
2. Pick the endpoint family you need from [API overview](../api/overview.md).
3. Use the synchronous bulk endpoints for small batches and [bulk jobs](../api/bulk-jobs.md) for larger batches.

If you are operating the portal:

1. Read [Portal architecture](../frontend/portal.md).
2. Read [Google auth setup](../frontend/google-auth.md).
3. Read [Secrets](../security/secrets.md) before setting environment variables.

If you are deploying the stack yourself:

1. Read [Self-hosting overview](../self-hosting/overview.md).
2. Choose a deployment shape in [Docker Compose](../self-hosting/docker-compose.md).
3. Pick a reverse proxy in [Proxy choices](../self-hosting/proxies.md).
4. Use [Operations](../operations/runbook.md) as the day-two reference.

## Local Development

The repo currently keeps the application source in `apps/api` and `apps/web`, with deployment material in `docs/` and the new documentation site content in `docs-site/`.

For local work you normally need:

- PostgreSQL
- the Go API
- the Go worker
- the Nuxt portal

The portal reads its own runtime config for Google auth and database access, then proxies API calls through the same origin. That means browser requests usually stay on the portal host while server-side requests go to the Go API through `API_BASE_URL`.

## Reading Order

If you want the detailed path from zero to production, read in this order:

1. [API overview](../api/overview.md)
2. [API authentication](../api/authentication.md)
3. [Bulk jobs](../api/bulk-jobs.md)
4. [Portal architecture](../frontend/portal.md)
5. [Google auth setup](../frontend/google-auth.md)
6. [Secrets](../security/secrets.md)
7. [Self-hosting overview](../self-hosting/overview.md)
8. [Docker Compose](../self-hosting/docker-compose.md)
9. [Proxy choices](../self-hosting/proxies.md)
10. [Operations runbook](../operations/runbook.md)

