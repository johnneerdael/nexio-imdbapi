import { constants } from 'node:fs'
import { access, readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { createError } from 'h3'

async function firstReadable(paths: string[]) {
  for (const path of paths) {
    try {
      await access(path, constants.R_OK)
      return path
    } catch {
      continue
    }
  }

  return null
}

export default defineEventHandler(async () => {
  const here = dirname(fileURLToPath(import.meta.url))
  const repoRoot = resolve(here, '../../../../..')
  const cwd = process.cwd()
  const generated = await firstReadable([
    resolve(repoRoot, 'docs/public/index.html'),
    resolve(cwd, 'docs/public/index.html'),
    resolve(cwd, '../../docs/public/index.html')
  ])
  const blueprint = await firstReadable([
    resolve(repoRoot, 'docs/api.apib'),
    resolve(cwd, 'docs/api.apib'),
    resolve(cwd, '../../docs/api.apib')
  ])

  try {
    if (generated) {
      const html = await readFile(generated, 'utf8')
      return new Response(html, {
        headers: {
          'content-type': 'text/html; charset=utf-8'
        }
      })
    }

    if (!blueprint) {
      throw new Error('API Blueprint file not found.')
    }

    const source = await readFile(blueprint, 'utf8')
    return {
      generated: false,
      blueprint: source
    }
  } catch (error) {
    throw createError({
      statusCode: 500,
      statusMessage: error instanceof Error ? error.message : 'Failed to load API docs.'
    })
  }
})
