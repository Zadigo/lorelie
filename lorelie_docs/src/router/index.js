import { createRouter, createWebHistory } from "vue-router";

export default createRouter({
  history: createWebHistory(),
  scrollBehavior: async () => ({ top: 0 }),
  routes: [
    {
      path: '/REPONAME/',
      children: [
        {
          path: '',
          component: async () => import('src/pages/HomePage.vue'),
          name: 'home'
        },
        {
          path: 'database',
          component: async () => import('src/pages/DatabasePage.vue'),
          name: 'database'
        }
      ]
    }
  ]
})
