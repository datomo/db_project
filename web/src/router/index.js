import Vue from 'vue'
import VueRouter from 'vue-router'
import Base from '../views/Base.vue'

Vue.use(VueRouter)

const routes = [
  {
    path: '/',
    name: 'home',
    component: Base
  },
  {
    path: '/combined',
    name: 'combined',
    component: () => import(/* webpackChunkName: "about" */ '../views/Combined.vue')
  },
  {
    path: '/final',
    name: 'final',
    component: () => import(/* webpackChunkName: "about" */ '../views/Final.vue')
  },
  {
    path: '/impressum',
    name: 'impressum',
    component: () => import(/* webpackChunkName: "about" */ '../views/Impressum.vue')
  }
]

const router = new VueRouter({
  routes
})

export default router
