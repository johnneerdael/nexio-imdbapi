import { createHash, randomBytes } from 'node:crypto'
import { readBody } from 'h3'
import { requireSessionUser } from '~/server/utils/session'
import { useDb } from '~/server/utils/db'

type Body = {
  label?: string
}

function hashApiKey(secret: string, pepper: string) {
  return createHash('sha256').update(`${pepper}:${secret}`).digest('hex')
}

export default defineEventHandler(async (event) => {
  const session = await requireSessionUser(event)
  const body = await readBody<Body>(event)
  const config = useRuntimeConfig()
  const pepper = String(config.apiKeyPepper || '')
  if (!pepper) {
    throw createError({ statusCode: 503, statusMessage: 'API key pepper is missing.' })
  }

  const prefix = randomBytes(4).toString('hex')
  const secret = randomBytes(24).toString('hex')
  const apiKey = `${prefix}.${secret}`
  const result = await useDb().query<{ id: string }>(
      `
      insert into api_keys (user_id, key_prefix, key_hash, name, created_at)
      values ($1, $2, $3, $4, now())
      returning id
    `,
    [session.user.id, prefix, hashApiKey(apiKey, pepper), body.label?.trim() || 'Portal key']
  )

  return {
    id: result.rows[0].id,
    apiKey,
    keyPrefix: prefix
  }
})
