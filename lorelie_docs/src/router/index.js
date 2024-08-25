import { createRouter, createWebHistory } from "vue-router";

export default createRouter({
  history: createWebHistory(),
  scrollBehavior: async () => ({ top: 0 }),
  routes: [
    {
      path: '/',
      component: async () => import('src/pages/HomePage.vue'),
      name: 'home'
    }
  ]
})
