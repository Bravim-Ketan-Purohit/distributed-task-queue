import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 7200,
    proxy: {
      '/v1': {
        target: 'http://localhost:7201',
        changeOrigin: true,
      },
      '/metrics': {
        target: 'http://localhost:7201',
        changeOrigin: true,
      },
    },
  },
})
