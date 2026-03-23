import { deleteCookie, getQuery, sendRedirect } from 'h3'
import { finishGoogleFlow, allowedEmail } from '~/server/utils/google'
import { createSession } from '~/server/utils/session'
import { useDb } from '~/server/utils/db'

type UserRow = {
  id: string
}

export default defineEventHandler(async (event) => {
  const query = getQuery(event)
  const code = String(query.code || '')
  const state = String(query.state || '')
  const { payload, nextPath } = await finishGoogleFlow(event, code, state)

  const email = String(payload.email || '').trim().toLowerCase()
  if (!email || payload.email_verified !== true || !allowedEmail(email)) {
    throw createError({ statusCode: 401, statusMessage: 'This Google account is not allowed.' })
  }

  const db = useDb()
  const upsert = await db.query<UserRow>(
    `
      insert into users (google_sub, email, display_name, avatar_url, created_at, updated_at, last_login_at)
      values ($1, $2, $3, $4, now(), now(), now())
      on conflict (google_sub)
      do update set
        email = excluded.email,
        display_name = excluded.display_name,
        avatar_url = excluded.avatar_url,
        updated_at = now(),
        last_login_at = now()
      returning id
    `,
    [payload.sub, email, payload.name || null, payload.picture || null]
  )

  await createSession(event, {
    id: upsert.rows[0].id,
    email,
    displayName: typeof payload.name === 'string' ? payload.name : null,
    avatarUrl: typeof payload.picture === 'string' ? payload.picture : null
  })

  deleteCookie(event, 'oauth_state', { path: '/' })
  deleteCookie(event, 'oauth_nonce', { path: '/' })
  deleteCookie(event, 'oauth_code_verifier', { path: '/' })
  deleteCookie(event, 'oauth_next', { path: '/' })

  return sendRedirect(event, nextPath || '/', 302)
})
