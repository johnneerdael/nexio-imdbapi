import { createError } from 'h3'
import { Pool } from 'pg'
import { useRuntimeConfig } from '#imports'

let pool: Pool | null = null

export function useDb() {
  if (pool) {
    return pool
  }

  const config = useRuntimeConfig()
  const connectionString = String(config.databaseUrl || '')
  if (!connectionString) {
    throw createError({ statusCode: 503, statusMessage: 'DATABASE_URL is missing.' })
  }

  pool = new Pool({
    connectionString,
    max: 10
  })

  return pool
}
