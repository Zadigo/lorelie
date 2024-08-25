import { defineConfig, loadEnv } from 'vite'
import { resolve } from 'path'
import vue from '@vitejs/plugin-vue'

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const root = process.cwd()
  const env = loadEnv(mode, root)
  process.env = { ...process.env, ...env }
  
  return {
    root,
    base: '/REPONAME/',
    plugins: [
      vue()
    ],
    resolve: {
      alias: {
        '@': resolve(__dirname, './src'),
        'src': resolve(__dirname, './src'),
        'components': resolve(__dirname, './src/components'),
        'layouts': resolve(__dirname, './src/layouts'),
        'pages': resolve(__dirname, './src/pages'),
        'stores': resolve(__dirname, './src/stores'),
        'data': resolve(__dirname, './src/data'),
        'composables': resolve(__dirname, './src/composables'),
        'assets': resolve(__dirname, './src/assets')
      }
    },
    test: {
      alias: {
        '@/': resolve(__dirname, './src'),
        'src': resolve(__dirname, './src')
      },
      browser: {
        enabled: true,
        name: 'chrome'
      }
    }
  }
})
