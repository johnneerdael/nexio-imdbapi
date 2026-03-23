---
title: API Overview
description: Endpoints, authentication, response conventions, and bulk-job behavior for the IMDb API.
---

# API Overview

The Go API serves the public contract for the platform. It is mounted under `/v1/*` and shares a host with the portal and health checks.

## Endpoint Families

The API is organized by resource:

- `GET /healthz` and `GET /readyz` for health checks
- `GET /v1/meta/*` for dataset metadata and aggregate counts
- `GET /v1/titles/*` for title detail, search, resolution, credits, principals, crew, and alternate titles
- `GET /v1/ratings/*` for ratings lookup
- `GET /v1/series/*` for episode and episode-rating views
- `GET /v1/names/*` for name lookup and filmography-style links
- `GET /v1/akas/*` for alternate-title search
- `POST /v1/*/bulk` for synchronous small-batch lookups
- `POST /v1/bulk/jobs` for async bulk execution

Every `/v1/*` route requires an API key. The health routes do not.

## Authentication Model

The API accepts the key in either of these headers:

- `X-API-Key: prefix.secret`
- `Authorization: Bearer prefix.secret`

The server validates the prefix first, loads the stored record, rejects revoked or expired keys, and then checks the full secret with the configured pepper. See [API authentication](authentication.md) for the exact flow.

## Response Shapes

The API uses a small number of response conventions:

- single-resource endpoints return the resource directly
- list-like endpoints usually return `{ "items": [...] }`
- bulk endpoints usually return `{ "results": [...], "missing": [...] }`
- async bulk job creation returns a job document with `202 Accepted`

Some list responses are intentionally strict. For example, search endpoints reject empty queries, and bulk endpoints reject empty identifier lists.

## Synchronous Bulk vs Async Jobs

Use the synchronous bulk endpoints when the batch is small enough to fit comfortably inside a normal request. The router limits those payloads to 250 identifiers.

Use async bulk jobs when you need higher throughput or want to queue a large operation without holding an HTTP connection open. The async job validator allows up to 10,000 identifiers.

The bulk worker executes the same service logic and stores the result back in Postgres. See [Bulk jobs](bulk-jobs.md) for the operational behavior.

## Health Checks

- `GET /healthz` returns `200` with `{"status":"ok"}`
- `GET /readyz` returns `200` with `{"status":"ready"}` only after the service can answer through its repository layer

These endpoints are designed for proxy and orchestration probes, not for authenticated client use.

## Practical Usage

For most clients the sequence is:

1. Mint an API key in the portal.
2. Store the key in your client secret manager.
3. Send requests to `/v1/*` with `X-API-Key` or a bearer token.
4. Move from synchronous bulk endpoints to async bulk jobs when payload sizes grow.

If you are deploying the platform yourself, read [Self-hosting overview](../self-hosting/overview.md) after this page so the endpoint behavior lines up with your proxy routing.

