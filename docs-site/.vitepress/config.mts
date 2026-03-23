import { defineConfig } from 'vitepress'

const repoName = process.env.GITHUB_REPOSITORY?.split('/')[1]
const isUserPagesRepo = repoName?.endsWith('.github.io') ?? false
const repoSlug = process.env.GITHUB_REPOSITORY || 'johnneerdael/nexio-imdbapi'
const repoUrl = `https://github.com/${repoSlug}`

function resolveBase() {
  if (!repoName || isUserPagesRepo) {
    return '/'
  }

  return `/${repoName}/`
}

export default defineConfig({
  lang: 'en-US',
  title: 'Nexio IMDb Docs',
  description: 'Internal API, portal, and self-hosting documentation for the IMDb platform.',
  base: resolveBase(),
  cleanUrls: true,
  lastUpdated: true,
  themeConfig: {
    logo: {
      alt: 'Nexio IMDb Docs',
      text: 'Nexio IMDb Docs'
    },
    nav: [
      { text: 'Start', link: '/guide/getting-started' },
      { text: 'API', link: '/api/overview' },
      { text: 'Frontend', link: '/frontend/portal' },
      { text: 'Self-Hosting', link: '/self-hosting/overview' },
      { text: 'Operations', link: '/operations/runbook' },
      { text: 'Source', link: repoUrl }
    ],
    search: {
      provider: 'local'
    },
    sidebar: [
      {
        text: 'Guide',
        items: [{ text: 'Getting Started', link: '/guide/getting-started' }]
      },
      {
        text: 'API',
        items: [
          { text: 'Overview', link: '/api/overview' },
          { text: 'Authentication', link: '/api/authentication' },
          { text: 'Bulk Jobs', link: '/api/bulk-jobs' }
        ]
      },
      {
        text: 'Frontend',
        items: [
          { text: 'Portal Architecture', link: '/frontend/portal' },
          { text: 'Google Auth Setup', link: '/frontend/google-auth' }
        ]
      },
      {
        text: 'Self-Hosting',
        items: [
          { text: 'Overview', link: '/self-hosting/overview' },
          { text: 'Docker Compose', link: '/self-hosting/docker-compose' },
          { text: 'Proxy Choices', link: '/self-hosting/proxies' }
        ]
      },
      {
        text: 'Security',
        items: [{ text: 'Secrets', link: '/security/secrets' }]
      },
      {
        text: 'Operations',
        items: [{ text: 'Runbook', link: '/operations/runbook' }]
      }
    ],
    socialLinks: [{ icon: 'github', link: repoUrl }],
    outline: {
      level: [2, 3],
      label: 'On This Page'
    },
    docFooter: {
      prev: 'Previous',
      next: 'Next'
    },
    footer: {
      message: 'Internal documentation for the Nexio IMDb platform.',
      copyright: 'Copyright © 2026'
    }
  }
})
