---
title: Secrets
description: Secret classification, rotation, and storage guidance for the portal and API.
---

# Secrets

This platform uses several secrets that must be treated as operational credentials, not configuration noise.

## Secret Inventory

The important secrets are:

- `GOOGLE_CLIENT_SECRET`
- `SESSION_COOKIE_SECRET`
- `API_KEY_PEPPER`
- `DATABASE_URL`
- `POSTGRES_PASSWORD`

Depending on your deployment, you may also treat the OAuth client ID and the Google allowlist as sensitive operational config even though they are not cryptographic secrets.

## What Each Secret Does

`GOOGLE_CLIENT_SECRET`
: Completes the Google OAuth code exchange in the portal.

`SESSION_COOKIE_SECRET`
: Protects the sealed OAuth state cookies and other session-adjacent cookie material.

`API_KEY_PEPPER`
: Is mixed into the API key hash before storage and validation. Rotating it invalidates all existing API keys.

`DATABASE_URL`
: Gives the API, worker, and portal access to Postgres.

`POSTGRES_PASSWORD`
: Bootstraps the database user in the Compose deployment.

## Rotation Rules

- rotate `GOOGLE_CLIENT_SECRET` through the Google Console and the deployment env file together
- rotate `SESSION_COOKIE_SECRET` only when you are willing to force new login sessions
- rotate `API_KEY_PEPPER` only when you are willing to invalidate every issued API key
- rotate `DATABASE_URL` credentials in lockstep with the database user or secret manager entry

## Storage Guidance

Use a secret manager or locked-down environment file, not Git history.

If you use a plain env file:

- keep the file out of Git
- restrict ownership and permissions
- avoid copying it between hosts without an audit trail

The deployment docs already recommend a single env file for the Compose stack. The same principle applies to the systemd deployment.

## Practical Checks

Before deploying, verify that:

- the Google redirect URL matches the configured host
- the session cookie secret is at least high entropy
- the API key pepper is stable
- the allowlist contains only the intended Google accounts

For how these values are consumed at runtime, see [Google auth setup](../frontend/google-auth.md) and [Portal architecture](../frontend/portal.md).

