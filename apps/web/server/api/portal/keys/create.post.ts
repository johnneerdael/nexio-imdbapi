import { createHash, randomBytes } from 'node:crypto'
import { readBody } from 'h3'
import { requireSessionUser } from '~/server/utils/session'
import { useDb } from '~/server/utils/db'

type Body = {
  label?: string
}

function hashApiKey(secret: string, pepper: string) {
  return createHash('sha256').update(`${secret}.${pepper}`).digest('hex')
}

export default defineEventHandler(async (event) => {
  const session = await requireSessionUser(event)
  const body = await readBody<Body>(event)
  const config = useRuntimeConfig()
  const pepper = String(config.apiKeyPepper || '')
  if (!pepper) {
    throw createError({ statusCode: 503, statusMessage: 'API key pepper is missing.' })
  }

  const secret = randomBytes(32).toString('base64url')
  const apiKey = `nexio_${secret}`
  const prefix = apiKey.slice(0, 12)
  const id = crypto.randomUUID()

  await useDb().query(
    `
      insert into api_keys (id, created_by_user_id, key_prefix, key_hash, label, created_at)
      values ($1, $2, $3, $4, $5, now())
    `,
    [id, session.user.id, prefix, hashApiKey(apiKey, pepper), body.label?.trim() || 'Portal key']
  )

  return {
    id,
    apiKey,
    keyPrefix: prefix
  }
})

