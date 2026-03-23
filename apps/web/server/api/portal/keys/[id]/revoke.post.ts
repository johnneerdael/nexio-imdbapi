import { requireSessionUser } from '~/server/utils/session'
import { useDb } from '~/server/utils/db'

export default defineEventHandler(async (event) => {
  const session = await requireSessionUser(event)
  const id = getRouterParam(event, 'id')
  if (!id) {
    throw createError({ statusCode: 400, statusMessage: 'Key id is required.' })
  }

  await useDb().query(
    `
      update api_keys
      set revoked_at = now()
      where id = $1 and created_by_user_id = $2
    `,
    [id, session.user.id]
  )

  return {
    revoked: true
  }
})

