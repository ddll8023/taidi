import { createRouter, createWebHistory } from 'vue-router'

import AppShell from '@/components/layout/AppShell.vue'

const routes = [
  {
    path: '/',
    redirect: '/reports/list'
  },
  {
    path: '/companies',
    component: AppShell,
    children: [
      {
        path: '',
        redirect: { name: 'CompaniesImport' }
      },
      {
        path: 'import',
        name: 'CompaniesImport',
        component: () => import('@/views/companies/CompaniesImportView.vue'),
        meta: {
          title: '公司信息导入',
          description: '导入附件1的上市公司基本信息，作为财报数据的基础主数据。',
          eyebrow: '主数据 / 导入'
        }
      }
    ]
  },
  {
    path: '/reports',
    component: AppShell,
    children: [
      {
        path: '',
        redirect: { name: 'ReportsList' }
      },
      {
        path: 'list',
        name: 'ReportsList',
        component: () => import('@/views/reports/ReportsListView.vue'),
        meta: {
          title: '记录中心',
          description: '当前页直接展示文件信息、解析状态、向量化状态，并支持当前页批量向量化。',
          eyebrow: '项目 / 记录'
        }
      },
      {
        path: 'view/:reportId?',
        name: 'ReportsView',
        component: () => import('@/views/reports/ReportsViewView.vue'),
        meta: {
          title: '记录查看',
          description: '保留文本内容与结构化结果的双栏查看关系，不扩展额外业务操作。',
          eyebrow: '项目 / 查看'
        }
      }
    ]
  },
  {
    path: '/chat',
    component: AppShell,
    children: [
      {
        path: '',
        name: 'Chat',
        component: () => import('@/views/chat/ChatView.vue'),
        meta: {
          title: '智能问数',
          description: '通过自然语言查询上市公司财务数据',
          eyebrow: '智能助手 / 对话'
        }
      }
    ]
  },
  {
    path: '/task2',
    component: AppShell,
    children: [
      {
        path: '',
        name: 'Task2',
        component: () => import('@/views/task2/Task2WorkbenchView.vue'),
        meta: {
          title: '任务二工作台',
          description: '上传附件4、批量回答问题、导出结果',
          eyebrow: '智能助手 / 任务二'
        }
      }
    ]
  },
  {
    path: '/task3',
    component: AppShell,
    children: [
      {
        path: '',
        redirect: { name: 'Task3KnowledgeBase' }
      },
      {
        path: 'knowledge-base',
        name: 'Task3KnowledgeBase',
        component: () => import('@/views/task3/KnowledgeBaseView.vue'),
        meta: {
          title: '知识库管理',
          description: '任务三知识库：批量注册、向量化、状态查询',
          eyebrow: '智能助手 / 任务三 / 知识库管理'
        }
      },
      {
        path: 'workbench',
        name: 'Task3Workbench',
        component: () => import('@/views/task3/Task3WorkbenchView.vue'),
        meta: {
          title: '任务三结果工作台',
          description: '上传附件6、批量回答问题、导出 result_3.xlsx',
          eyebrow: '智能助手 / 任务三 / 结果工作台'
        }
      }
    ]
  },
  {
    path: '/:pathMatch(.*)*',
    name: 'NotFound',
    component: () => import('@/views/NotFoundView.vue'),
    meta: {
      title: '页面不存在',
      description: '请返回记录中心或上传页继续操作。'
    }
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router
