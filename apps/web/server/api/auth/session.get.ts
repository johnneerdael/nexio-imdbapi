import { getSessionUser } from '~/server/utils/session'

export default defineEventHandler(async (event) => {
  const session = await getSessionUser(event)
  if (!session) {
    throw createError({ statusCode: 401, statusMessage: 'Not authenticated.' })
  }

  return {
    authenticated: true,
    user: session.user
  }
})

