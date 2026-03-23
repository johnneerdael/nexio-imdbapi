import { requireSessionUser } from '~/server/utils/session'
import { useDb } from '~/server/utils/db'

export default defineEventHandler(async (event) => {
  const session = await requireSessionUser(event)
  const db = useDb()
  const result = await db.query(
    `
      select id, key_prefix, name as label, created_at, last_used_at, revoked_at
      from api_keys
      where user_id = $1
      order by created_at desc
    `,
    [session.user.id]
  )

  return {
    items: result.rows
  }
})
