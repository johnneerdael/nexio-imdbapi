import type { Config } from 'tailwindcss'

export default <Partial<Config>>{
  content: [
    './components/**/*.{vue,js,ts}',
    './layouts/**/*.vue',
    './pages/**/*.vue',
    './composables/**/*.{js,ts}',
    './app.vue'
  ],
  theme: {
    extend: {
      colors: {
        surface: '#0e0e0e',
        'surface-lowest': '#000000',
        'surface-low': '#131313',
        'surface-card': '#1a1a1a',
        primary: '#ba9eff',
        'primary-dim': '#8455ef',
        secondary: '#53ddfc',
        tertiary: '#ff97b5',
        soft: '#adaaaa'
      },
      boxShadow: {
        glow: '0 0 40px rgba(186, 158, 255, 0.08)'
      },
      borderRadius: {
        xl2: '1rem',
        xl3: '1.5rem'
      },
      fontFamily: {
        display: ['Manrope', 'sans-serif'],
        body: ['Inter', 'sans-serif']
      }
    }
  }
}
