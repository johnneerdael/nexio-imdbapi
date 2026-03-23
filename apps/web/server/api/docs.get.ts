import { access, readFile } from 'node:fs/promises'
import { constants } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

export default defineEventHandler(async () => {
  const here = dirname(fileURLToPath(import.meta.url))
  const repoRoot = resolve(here, '../../../../..')
  const generated = resolve(repoRoot, 'docs/public/index.html')
  const blueprint = resolve(repoRoot, 'docs/api.apib')

  try {
    await access(generated, constants.R_OK)
    const html = await readFile(generated, 'utf8')
    return new Response(html, {
      headers: {
        'content-type': 'text/html; charset=utf-8'
      }
    })
  } catch {
    const source = await readFile(blueprint, 'utf8')
    return {
      generated: false,
      blueprint: source
    }
  }
})
