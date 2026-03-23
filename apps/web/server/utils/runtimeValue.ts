export function runtimeValue(value: unknown, envKeys: string[], fallback = '') {
  const direct = typeof value === 'string' ? value.trim() : String(value || '').trim()
  if (direct) {
    return direct
  }

  for (const key of envKeys) {
    const env = process.env[key]?.trim()
    if (env) {
      return env
    }
  }

  return fallback
}
