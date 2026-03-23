import { randomBytes, randomUUID, createHash, timingSafeEqual } from 'node:crypto'
import { createError, getCookie, getHeader, setCookie, deleteCookie, type H3Event } from 'h3'
import { useRuntimeConfig } from '#imports'
import { useDb } from './db'

type SessionRow = {
  session_id: string
  user_id: string
  email: string
  display_name: string | null
  avatar_url: string | null
  expires_at: string
  revoked_at: string | null
}

type Config = {
  cookieName: string
  sessionHours: number
}

export type PortalUser = {
  id: string
  email: string
  displayName: string | null
  avatarUrl: string | null
}

export type SessionUser = {
  sessionId: string
  user: PortalUser
}

function sessionConfig(): Config {
  const config = useRuntimeConfig()
  return {
    cookieName: String(config.sessionCookieName || 'nexio_imdb_session'),
    sessionHours: Number(config.sessionDurationHours || 336)
  }
}

export function hashOpaqueToken(token: string) {
  return createHash('sha256').update(token).digest('hex')
}

export function newOpaqueToken(byteLength = 32) {
  return randomBytes(byteLength).toString('base64url')
}

function hashedIp(event: H3Event) {
  const value = getHeader(event, 'x-forwarded-for') || getHeader(event, 'x-real-ip') || event.node.req.socket.remoteAddress || ''
  return value ? hashOpaqueToken(String(value)) : null
}

export async function createSession(event: H3Event, user: PortalUser) {
  const db = useDb()
  const token = newOpaqueToken(32)
  const sessionId = randomUUID()
  const tokenHash = hashOpaqueToken(token)
  const cfg = sessionConfig()
  const expiresAt = new Date(Date.now() + cfg.sessionHours * 60 * 60 * 1000)
  const userAgent = getHeader(event, 'user-agent') || ''

  await db.query(
    `
      insert into web_sessions (
        id,
        user_id,
        session_secret_hash,
        expires_at,
        created_at,
        last_seen_at,
        ip_hash,
        user_agent
      )
      values ($1, $2, $3, $4, now(), now(), $5, $6)
    `,
    [sessionId, user.id, tokenHash, expiresAt.toISOString(), hashedIp(event), userAgent]
  )

  setCookie(event, cfg.cookieName, `${sessionId}.${token}`, {
    httpOnly: true,
    sameSite: 'lax',
    secure: process.env.NODE_ENV === 'production',
    path: '/',
    expires: expiresAt
  })
}

export async function destroySession(event: H3Event) {
  const db = useDb()
  const token = getSessionCookie(event)
  if (token) {
    const [sessionId] = token.split('.', 2)
    if (sessionId) {
      await db.query(`update web_sessions set revoked_at = now() where id = $1`, [sessionId])
    }
  }

  const cfg = sessionConfig()
  deleteCookie(event, cfg.cookieName, { path: '/' })
}

function getSessionCookie(event: H3Event) {
  const cfg = sessionConfig()
  return getCookie(event, cfg.cookieName) || ''
}

export async function requireSessionUser(event: H3Event): Promise<SessionUser> {
  const session = await getSessionUser(event)
  if (!session) {
    throw createError({ statusCode: 401, statusMessage: 'Authentication required.' })
  }
  return session
}

export async function getSessionUser(event: H3Event): Promise<SessionUser | null> {
  const raw = getSessionCookie(event)
  if (!raw) {
    return null
  }

  const [sessionId, token] = raw.split('.', 2)
  if (!sessionId || !token) {
    return null
  }

  const db = useDb()
  const result = await db.query<SessionRow>(
    `
      select
        ws.id as session_id,
        u.id as user_id,
        u.email,
        u.display_name,
        u.avatar_url,
        ws.expires_at,
        ws.revoked_at
      from web_sessions ws
      join users u on u.id = ws.user_id
      where ws.id = $1
        and ws.revoked_at is null
        and ws.expires_at > now()
        and u.disabled_at is null
      limit 1
    `,
    [sessionId]
  )

  const row = result.rows[0]
  if (!row) {
    return null
  }

  const supplied = Buffer.from(hashOpaqueToken(token), 'hex')
  const storedHash = await db.query<{ session_secret_hash: string }>(
    `select session_secret_hash from web_sessions where id = $1 limit 1`,
    [sessionId]
  )
  const stored = storedHash.rows[0]
  if (!stored) {
    return null
  }

  const expected = Buffer.from(stored.session_secret_hash, 'hex')
  if (supplied.length !== expected.length || !timingSafeEqual(supplied, expected)) {
    return null
  }

  await db.query(`update web_sessions set last_seen_at = now(), expires_at = now() + interval '14 days' where id = $1`, [sessionId])

  return {
    sessionId,
    user: {
      id: row.user_id,
      email: row.email,
      displayName: row.display_name,
      avatarUrl: row.avatar_url
    }
  }
}
