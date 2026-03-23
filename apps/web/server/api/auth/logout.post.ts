import { destroySession } from '~/server/utils/session'

export default defineEventHandler(async (event) => {
  await destroySession(event)
  return new Response(null, { status: 204 })
})

