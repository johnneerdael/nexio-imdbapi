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
    googleClientId: process.env.GOOGLE_CLIENT_ID || '',
    googleClientSecret: process.env.GOOGLE_CLIENT_SECRET || '',
    googleRedirectUrl: process.env.GOOGLE_REDIRECT_URL || '',
    allowedGoogleEmails: process.env.ALLOWED_GOOGLE_EMAILS || '',
    sessionCookieSecret: process.env.SESSION_COOKIE_SECRET || '',
    sessionCookieName: process.env.SESSION_COOKIE_NAME || 'nexio_imdb_session',
    sessionDurationHours: 336,
    apiBaseUrl: process.env.API_BASE_URL || 'http://localhost:8080',
    databaseUrl: process.env.DATABASE_URL || '',
    apiKeyPepper: process.env.API_KEY_PEPPER || '',
    public: {
      apiBaseUrl: process.env.API_BASE_URL || 'http://localhost:8080'
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
