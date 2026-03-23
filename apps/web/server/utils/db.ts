import { createError } from 'h3'
import { Pool } from 'pg'
import { useRuntimeConfig } from '#imports'
import { runtimeValue } from './runtimeValue'

let pool: Pool | null = null

export function useDb() {
  if (pool) {
    return pool
  }

  const config = useRuntimeConfig()
  const connectionString = runtimeValue(config.databaseUrl, ['NUXT_DATABASE_URL', 'DATABASE_URL'])
  if (!connectionString) {
    throw createError({ statusCode: 503, statusMessage: 'DATABASE_URL is missing.' })
  }

  pool = new Pool({
    connectionString,
    max: 10
  })

  return pool
}
