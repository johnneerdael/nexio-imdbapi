import { OAuth2Client } from 'google-auth-library'
import { createHash, randomBytes } from 'node:crypto'
import { createError, getCookie, setCookie, type H3Event } from 'h3'
import { useRuntimeConfig } from '#imports'
import { openCookieValue, sealCookieValue } from './secureCookie'

const OAUTH_COOKIE_TTL_SECONDS = 600

function cfg() {
  const config = useRuntimeConfig()
  return {
    clientId: String(config.googleClientId || ''),
    clientSecret: String(config.googleClientSecret || ''),
    redirectUrl: String(config.googleRedirectUrl || ''),
    appBaseUrl: String(config.appBaseUrl || '')
  }
}

function cookieOptions() {
  return {
    httpOnly: true,
    sameSite: 'lax' as const,
    secure: process.env.NODE_ENV === 'production',
    path: '/',
    maxAge: OAUTH_COOKIE_TTL_SECONDS
  }
}

function base64Url(bytes = 32) {
  return randomBytes(bytes).toString('base64url')
}

function codeChallenge(verifier: string) {
  return createHash('sha256').update(verifier).digest('base64url')
}

export function googleClient() {
  const config = cfg()
  if (!config.clientId || !config.clientSecret || !config.redirectUrl) {
    throw createError({ statusCode: 503, statusMessage: 'Google OAuth runtime config is missing.' })
  }

  return new OAuth2Client({
    clientId: config.clientId,
    clientSecret: config.clientSecret,
    redirectUri: config.redirectUrl
  })
}

export function beginGoogleFlow(event: H3Event, nextPath = '/') {
  const client = googleClient()
  const state = base64Url(24)
  const nonce = base64Url(24)
  const verifier = base64Url(48)

  setCookie(event, 'oauth_state', sealCookieValue(state), cookieOptions())
  setCookie(event, 'oauth_nonce', sealCookieValue(nonce), cookieOptions())
  setCookie(event, 'oauth_code_verifier', sealCookieValue(verifier), cookieOptions())
  setCookie(event, 'oauth_next', sealCookieValue(nextPath), {
    ...cookieOptions(),
    httpOnly: true
  })

  return client.generateAuthUrl({
    scope: ['openid', 'email', 'profile'],
    response_type: 'code',
    access_type: 'offline',
    prompt: 'select_account',
    state,
    nonce,
    code_challenge_method: 'S256',
    code_challenge: codeChallenge(verifier)
  })
}

export async function finishGoogleFlow(event: H3Event, code: string, state: string) {
  const storedState = openCookieValue(getCookie(event, 'oauth_state') || '')
  const storedNonce = openCookieValue(getCookie(event, 'oauth_nonce') || '')
  const storedVerifier = openCookieValue(getCookie(event, 'oauth_code_verifier') || '')
  const nextPath = openCookieValue(getCookie(event, 'oauth_next') || '') || '/'
  if (!code || !state || state !== storedState || !storedVerifier || !storedNonce) {
    throw createError({ statusCode: 401, statusMessage: 'Invalid OAuth state.' })
  }

  const client = googleClient()
  const { tokens } = await client.getToken({
    code,
    codeVerifier: storedVerifier,
    redirect_uri: cfg().redirectUrl
  })

  if (!tokens.id_token) {
    throw createError({ statusCode: 401, statusMessage: 'Google OAuth did not return an ID token.' })
  }

  const ticket = await client.verifyIdToken({
    idToken: tokens.id_token,
    audience: cfg().clientId
  })
  const payload = ticket.getPayload()
  if (!payload || payload.nonce !== storedNonce || !payload.sub) {
    throw createError({ statusCode: 401, statusMessage: 'Google ID token verification failed.' })
  }

  return {
    payload,
    nextPath
  }
}

export function allowedEmail(email: string) {
  const config = useRuntimeConfig()
  const allowlist = String(config.allowedGoogleEmails || '')
    .split(',')
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean)
  return allowlist.includes(email.trim().toLowerCase())
}
