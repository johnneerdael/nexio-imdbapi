---
title: Portal Architecture
description: How the Nuxt portal handles authentication, API keys, and embedded docs.
---

# Portal Architecture

The portal is the human-facing surface of the platform. It is built in Nuxt 4 and runs on the same origin as the API in production.

## Major Responsibilities

The portal handles three jobs:

1. Google login for approved users
2. API key creation and revocation
3. embedded access to the API Blueprint docs

The portal does not replace the API. It sits in front of it and manages the human workflow around access.

## Page Structure

The main routes are:

- `/` for the primary dashboard
- `/docs` for the docs-focused view
- `/auth/google` for login start
- `/auth/callback` for OAuth completion
- `/auth/session` for session checks
- `/auth/logout` for logout

The dashboard renders the latest snapshot metadata, aggregate counts, and the user's API keys after the browser session is established.

## Session Flow

The browser session is separate from API key auth.

1. The user clicks Google sign-in.
2. The portal starts an OAuth flow with PKCE, state, and nonce protection.
3. The callback verifies the Google ID token and the email allowlist.
4. The portal stores a Postgres-backed session and sets a secure session cookie.
5. The dashboard loads session data and portal bootstrap data.

Session reads are done through `/auth/session`. If the cookie is missing or expired, the portal shows the login card instead of the dashboard.

## Data Loaded On The Dashboard

The bootstrap endpoint returns:

- the current portal user
- the latest snapshot metadata
- dataset counts
- the user's API keys

That data comes from `imdb_snapshots`, `titles`, `title_ratings`, `title_episodes`, `names`, and `api_keys`.

## Embedded Docs

The portal includes an embedded docs panel that renders the API Blueprint HTML at `/api/docs`. If the generated HTML is unavailable, the server falls back to serving the raw blueprint contract.

That means the portal stays useful even when the generated docs artifact is missing.

## Operational Notes

- portal sessions are not the same as API keys
- API keys are generated one time and stored as hashes
- the portal only shows the raw API key at creation time
- users can revoke old keys without invalidating their browser session

For the auth configuration behind this flow, see [Google auth setup](google-auth.md). For the secret values that make this work, see [Secrets](../security/secrets.md).

