import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  {
    path: '/',
    component: () => import('@/components/layout/AppShell.vue'),
    children: [
      {
        path: '',
        redirect: '/companies/import'
      },
      {
        path: 'companies/import',
        name: 'CompaniesImport',
        component: () => import('@/views/companies/CompaniesImportView.vue'),
        meta: { title: '公司信息导入', description: '导入附件1的上市公司基本信息', eyebrow: '主数据 / 导入' }
      }
    ]
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router
