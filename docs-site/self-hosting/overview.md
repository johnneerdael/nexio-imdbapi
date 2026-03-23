---
title: Self-Hosting Overview
description: Deployment models, service split, and edge routing choices for the stack.
---

# Self-Hosting Overview

The platform is designed to run on a single host or in a Compose-based deployment. The stack is intentionally small:

- PostgreSQL 16
- Go API
- Go worker
- Nuxt web portal
- one reverse proxy choice at the edge

## Recommended Split

The deployment logic is split into two concerns:

- application services
- edge routing

The application stack owns the database, migrations, API, worker, and portal. The edge routing layer decides how requests reach the API or the portal.

## Request Routing

Every deployment must preserve this split:

- `/v1/*`, `/healthz`, and `/readyz` go to the Go API
- everything else goes to the Nuxt portal

This split matters because the portal serves browser pages and embeds docs, while the API serves the JSON contract and health checks.

## Deployment Options

You have three broad choices:

- use Docker Compose for the full stack and the edge
- use Docker Compose for the app stack and a host-managed proxy
- run the app stack under Compose and terminate TLS elsewhere

The Compose guide covers the concrete files and commands. See [Docker Compose](docker-compose.md).

## What To Read Next

- [Docker Compose](docker-compose.md) for the exact startup commands
- [Proxy choices](proxies.md) for Caddy, Nginx, Traefik, and host-managed Caddy trade-offs
- [Secrets](../security/secrets.md) for the variables that must be stable
- [Operations runbook](../operations/runbook.md) for checks and troubleshooting

