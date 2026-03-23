import { createError, deleteCookie, getQuery, sendRedirect } from 'h3'
import { finishGoogleFlow, allowedEmail } from '~/server/utils/google'
import { createSession } from '~/server/utils/session'
import { useDb } from '~/server/utils/db'

type UserRow = {
  id: string
}

export default defineEventHandler(async (event) => {
  try {
    const query = getQuery(event)
    const code = String(query.code || '')
    const state = String(query.state || '')
    console.info('[auth] callback start', {
      hasCode: Boolean(code),
      hasState: Boolean(state)
    })

    const { payload, nextPath } = await finishGoogleFlow(event, code, state)
    const email = String(payload.email || '').trim().toLowerCase()
    console.info('[auth] callback token verified', {
      email,
      nextPath
    })

    if (!email || payload.email_verified !== true || !allowedEmail(email)) {
      throw createError({ statusCode: 401, statusMessage: 'This Google account is not allowed.' })
    }

    const db = useDb()
    const displayName = typeof payload.name === 'string' ? payload.name : null
    const avatarUrl = typeof payload.picture === 'string' ? payload.picture : null

    const existingBySub = await db.query<UserRow>(
      `
        select id
        from users
        where google_sub = $1
        limit 1
      `,
      [payload.sub]
    )

    let userId = existingBySub.rows[0]?.id
    if (userId) {
      await db.query(
        `
          update users
          set
            email = $2,
            display_name = $3,
            avatar_url = $4,
            updated_at = now(),
            last_login_at = now()
          where id = $1
        `,
        [userId, email, displayName, avatarUrl]
      )
    } else {
      const existingByEmail = await db.query<{ id: string; google_sub: string | null }>(
        `
          select id, google_sub
          from users
          where email = $1
          limit 1
        `,
        [email]
      )

      const emailRow = existingByEmail.rows[0]
      if (emailRow) {
        if (emailRow.google_sub && emailRow.google_sub !== payload.sub) {
          throw createError({ statusCode: 409, statusMessage: 'This email is already linked to another Google account.' })
        }

        userId = emailRow.id
        await db.query(
          `
            update users
            set
              google_sub = $2,
              display_name = $3,
              avatar_url = $4,
              updated_at = now(),
              last_login_at = now()
            where id = $1
          `,
          [userId, payload.sub, displayName, avatarUrl]
        )
      } else {
        const insert = await db.query<UserRow>(
          `
            insert into users (google_sub, email, display_name, avatar_url, created_at, updated_at, last_login_at)
            values ($1, $2, $3, $4, now(), now(), now())
            returning id
          `,
          [payload.sub, email, displayName, avatarUrl]
        )
        userId = insert.rows[0].id
      }
    }

    console.info('[auth] callback user resolved', { userId, email })

    await createSession(event, {
      id: userId,
      email,
      displayName,
      avatarUrl
    })

    console.info('[auth] callback session created', { userId, nextPath })

    deleteCookie(event, 'oauth_state', { path: '/' })
    deleteCookie(event, 'oauth_nonce', { path: '/' })
    deleteCookie(event, 'oauth_code_verifier', { path: '/' })
    deleteCookie(event, 'oauth_next', { path: '/' })

    return sendRedirect(event, nextPath || '/', 302)
  } catch (error) {
    console.error('[auth] callback failed', error)
    throw error
  }
})
