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
      },
      {
        path: 'reports/list',
        name: 'ReportsList',
        component: () => import('@/views/reports/ReportsListView.vue'),
        meta: { title: '财报记录', description: '上传财报文件、查看解析状态', eyebrow: '项目 / 记录' }
      },
      {
        path: 'reports/detail/:id',
        name: 'ReportDetail',
        component: () => import('@/views/reports/ReportDetailView.vue'),
        meta: { title: '财报详情', description: '查看财报结构化数据', eyebrow: '项目 / 详情' }
      },
      {
        path: 'chat',
        name: 'Chat',
        component: () => import('@/views/chat/ChatPage.vue'),
        meta: { title: '智能问数', description: '自然语言查询财务数据', eyebrow: '项目 / 问数' }
      }
    ]
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router
