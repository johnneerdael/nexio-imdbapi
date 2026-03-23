---
title: API Authentication
description: How API keys are generated, stored, validated, and revoked.
---

# API Authentication

The API uses opaque API keys for machine-to-machine access. The portal is the place where humans mint and revoke keys.

## Key Format

Keys use a `prefix.secret` structure.

- `prefix` identifies the stored record quickly
- `secret` is the high-entropy value shown only once at creation time

The API key creation flow in the portal generates the key, stores only a hash, and returns the raw value to the browser once.

## Where Keys Are Accepted

The API accepts either of these headers on `/v1/*`:

- `X-API-Key`
- `Authorization: Bearer ...`

Both paths end up in the same authentication code path. If no key is provided the server returns `401` with a `missing_api_key` error. If the key fails validation the server returns `401` with `invalid_api_key`.

## How Validation Works

Validation is layered:

1. Parse the key and extract the prefix.
2. Load the stored record for that prefix.
3. Reject keys that are revoked or expired.
4. Hash the presented key with the configured pepper.
5. Compare the hash to the stored hash using constant-time comparison.

The stored hash is not the raw key. If the pepper changes, every existing key becomes invalid.

## Portal-Minted Keys

The portal creates keys through the `/api/portal/keys/create` route. The API key is tied to a portal user and can be listed or revoked from the portal UI.

The portal returns the key prefix in the database record, but only the raw secret is visible at creation time. Treat the displayed secret as a one-time recovery window.

## Operational Guidance

- Keep the `API_KEY_PEPPER` stable across deployments.
- Revoke unused keys rather than deleting portal users.
- Use one key per integration so rotation is isolated.
- Prefer a secret manager, not environment files in application code, for the actual API key material.

## Example

```bash
curl \
  -H "X-API-Key: abcdef12.00112233445566778899aabbccddeeff0011223344556677" \
  https://api.example.com/v1/meta/stats
```

If you need the browser auth story rather than API key auth, read [Portal architecture](../frontend/portal.md) and [Google auth setup](../frontend/google-auth.md).

