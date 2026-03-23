import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto'
import { createError } from 'h3'
import { useRuntimeConfig } from '#imports'
import { runtimeValue } from './runtimeValue'

const IV_LENGTH = 12
const TAG_LENGTH = 16

function cookieSecretKey() {
  const config = useRuntimeConfig()
  const secret = runtimeValue(config.sessionCookieSecret, ['NUXT_SESSION_COOKIE_SECRET', 'SESSION_COOKIE_SECRET'])
  if (!secret) {
    throw createError({ statusCode: 503, statusMessage: 'SESSION_COOKIE_SECRET is missing.' })
  }
  return createHash('sha256').update(secret).digest()
}

export function sealCookieValue(value: string) {
  const iv = randomBytes(IV_LENGTH)
  const cipher = createCipheriv('aes-256-gcm', cookieSecretKey(), iv)
  const encrypted = Buffer.concat([cipher.update(value, 'utf8'), cipher.final()])
  const tag = cipher.getAuthTag()
  return Buffer.concat([iv, tag, encrypted]).toString('base64url')
}

export function openCookieValue(value: string) {
  if (!value) {
    return ''
  }

  const payload = Buffer.from(value, 'base64url')
  if (payload.length <= IV_LENGTH+TAG_LENGTH) {
    throw createError({ statusCode: 401, statusMessage: 'Invalid encrypted cookie.' })
  }

  const iv = payload.subarray(0, IV_LENGTH)
  const tag = payload.subarray(IV_LENGTH, IV_LENGTH+TAG_LENGTH)
  const ciphertext = payload.subarray(IV_LENGTH + TAG_LENGTH)

  try {
    const decipher = createDecipheriv('aes-256-gcm', cookieSecretKey(), iv)
    decipher.setAuthTag(tag)
    return Buffer.concat([decipher.update(ciphertext), decipher.final()]).toString('utf8')
  } catch {
    throw createError({ statusCode: 401, statusMessage: 'Encrypted cookie verification failed.' })
  }
}
