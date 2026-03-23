---
title: Google Auth Setup
description: OAuth configuration, callback URLs, and runtime variables for portal login.
---

# Google Auth Setup

The portal uses Google OIDC for human sign-in. Authentication is handled in the Nuxt server layer with PKCE, nonce validation, and a Postgres-backed session record.

## Google Cloud Setup

Create an OAuth client in Google Cloud Console for the web portal.

Use these values in the Google configuration:

- authorized redirect URI: `https://<your-host>/auth/callback`
- authorized JavaScript origin: `https://<your-host>`

The exact host depends on your deployment, but the redirect path must stay `/auth/callback`.

## Required Environment Variables

The portal needs these runtime values:

- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_REDIRECT_URL`
- `ALLOWED_GOOGLE_EMAILS`

The Compose deployment also derives `APP_BASE_URL` from `APP_DOMAIN`. In the existing env examples, the Google redirect URL is `https://api.nexioapp.org/auth/callback` and the allowlist is a comma-separated list of approved email addresses.

## Login Flow

The login route is `/auth/google?next=/`.

This is a runtime route on the Nuxt portal application, not a static GitHub Pages route.

The server then:

1. creates state, nonce, and PKCE verifier cookies
2. redirects the browser to Google
3. exchanges the callback code for tokens
4. verifies the ID token audience and nonce
5. checks that the email address is verified and allowlisted
6. creates the portal session and redirects back to the requested page

If the email is not allowlisted, the callback fails with `401`.

## Allowlist Behavior

`ALLOWED_GOOGLE_EMAILS` is a strict allowlist, not a soft hint.

Important consequences:

- a verified Google account still cannot enter the portal unless its email is listed
- the list is compared case-insensitively after trimming whitespace
- changing the list takes effect on the next login attempt

## Session Details

After successful login, the portal stores a session cookie named `nexio_imdb_session` by default. The cookie points to a server-side record that includes the user, expiry, and last-seen data.

That session controls access to:

- the dashboard
- the docs view
- API key management

For the secrets that protect the session cookie and OAuth exchange, read [Secrets](../security/secrets.md).
