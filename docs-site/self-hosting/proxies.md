---
title: Proxy Choices
description: Trade-offs between Caddy, Nginx, Traefik, and host-managed edge routing.
---

# Proxy Choices

The stack supports multiple proxy strategies because not every operator wants the same edge behavior.

## Caddy

Caddy is the simplest default when you want automatic TLS and a small configuration surface.

Use it when:

- you want a Docker-managed proxy with minimal moving parts
- you prefer a direct file-based routing model
- you want the stack to manage certificates for you

Relevant files:

- `docker-compose.caddy.yml`
- `infra/caddy/Caddyfile`

Routing is explicit:

- `/v1/*` goes to the API
- `/healthz` goes to the API
- `/readyz` goes to the API
- everything else goes to the portal

## Nginx

Nginx is the most familiar option if you already operate it elsewhere.

Use it when:

- you want a plain HTTP reverse proxy in Compose
- you already terminate TLS in front of the stack
- you want explicit server-block behavior without Docker label routing

Relevant files:

- `docker-compose.nginx.yml`
- `infra/nginx/default.conf`

The bundled Nginx setup is HTTP-only. If you need HTTPS, terminate TLS before traffic reaches the container or extend the config yourself.

## Traefik

Traefik is a good fit when you want Docker-native routing and automatic Let’s Encrypt handling.

Use it when:

- you prefer label-driven routing
- you want a Compose-managed HTTPS edge
- you already like Traefik's ACME workflow

Relevant files:

- `docker-compose.traefik.yml`

Traefik gives the API router higher priority than the portal router so the `/v1/*`, `/healthz`, and `/readyz` paths always hit the API first.

## Host-Managed Caddy

This is the cleanest choice if you already run Caddy on the host and only want Compose to manage the app stack.

Use it when:

- you want the proxy outside Compose
- you want loopback exposure only
- you prefer to keep certificate management in the host configuration

Relevant files:

- `docker-compose.host-proxy.override.yml.example`
- `docker-compose.caddy-net.override.yml`

If your Caddy runs in Docker rather than directly on the host, the cleaner pattern is usually a shared external Docker network instead of loopback port publishing. In that model:

- the app stack attaches `api` and `web` to `caddy_net`
- the separate Caddy stack also joins `caddy_net`
- the Caddyfile can `reverse_proxy api:8080` and `reverse_proxy web:3000` directly

This gives you the same clean route split without exposing the app containers on host ports.

## Recommendation

If you do not have a strong preference, start with Caddy.

It is the smallest configuration surface, the route split is obvious, and the resulting behavior is easiest to debug.
