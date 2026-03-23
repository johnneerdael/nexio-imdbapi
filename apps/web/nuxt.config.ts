const apiBaseUrl = process.env.API_BASE_URL || 'http://localhost:8080'

export default defineNuxtConfig({
  modules: ['@nuxtjs/tailwindcss'],
  css: ['~/assets/css/main.css'],
  app: {
    head: {
      title: 'Nexio IMDb Portal',
      meta: [
        { name: 'viewport', content: 'width=device-width, initial-scale=1' },
        {
          name: 'description',
          content: 'Internal IMDb dataset API portal, documentation, and API key management for Nexio.'
        }
      ]
    }
  },
  runtimeConfig: {
    googleClientId: '',
    googleClientSecret: '',
    googleRedirectUrl: '',
    allowedGoogleEmails: '',
    sessionCookieSecret: '',
    sessionCookieName: 'nexio_imdb_session',
    sessionDurationHours: 336,
    appBaseUrl: 'http://localhost:3000',
    apiBaseUrl: 'http://localhost:8080',
    databaseUrl: '',
    apiKeyPepper: '',
    public: {
      apiBaseUrl: 'http://localhost:8080'
    }
  },
  nitro: {
    routeRules: {
      '/healthz': { proxy: `${apiBaseUrl}/healthz` },
      '/readyz': { proxy: `${apiBaseUrl}/readyz` },
      '/v1/**': { proxy: `${apiBaseUrl}/v1/**` }
    }
  },
  future: {
    compatibilityVersion: 4
  },
  compatibilityDate: '2026-03-23'
})
