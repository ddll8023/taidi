export const navigationGroups = [
  {
    title: '主数据',
    items: [
      {
        title: '公司导入',
        shortLabel: '公司',
        description: '导入附件1的上市公司基本信息',
        icon: 'building',
        to: '/companies/import'
      }
    ]
  },
  {
    title: '项目模块',
    items: [
      {
        title: '记录',
        shortLabel: '记录',
        description: '上传财报文件、查看记录、解析状态与向量化状态',
        icon: 'file-lines',
        to: '/reports/list'
      }
    ]
  },
  {
    title: '智能助手',
    items: [
      {
        title: '智能问数',
        shortLabel: '问数',
        description: '通过自然语言查询上市公司财务数据',
        icon: 'comments',
        to: '/chat'
      },
      {
        title: '任务二工作台',
        shortLabel: '任务二',
        description: '上传附件4、批量回答问题、导出结果',
        icon: 'list-check',
        to: '/task2'
      },
      {
        title: '知识库管理',
        shortLabel: '知识库',
        description: '任务三知识库：批量注册、向量化、状态查询',
        icon: 'database',
        to: '/task3/knowledge-base'
      },
      {
        title: '任务三结果工作台',
        shortLabel: '结果工作台',
        description: '上传附件6、批量回答问题、导出 result_3.xlsx',
        icon: 'list-check',
        to: '/task3/workbench'
      }
    ]
  }
]
