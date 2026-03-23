import { getQuery, sendRedirect } from 'h3'
import { beginGoogleFlow } from '~/server/utils/google'

function safeNextPath(value: unknown) {
  if (typeof value !== 'string') {
    return '/'
  }

  const trimmed = value.trim()
  if (!trimmed.startsWith('/')) {
    return '/'
  }

  return trimmed
}

export default defineEventHandler((event) => {
  const nextPath = safeNextPath(getQuery(event).next)
  const url = beginGoogleFlow(event, nextPath)
  return sendRedirect(event, url, 302)
})

