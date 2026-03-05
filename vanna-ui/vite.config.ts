import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],

  // Use relative asset paths so built files work when served from any path
  base: './',

  build: {
    // Output directory for production build — FastAPI serves from vanna-ui/dist/
    outDir: 'dist',
    // Clean output directory before each build
    emptyOutDir: true,
  },

  server: {
    // Proxy /api requests to the FastAPI backend during development
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})
