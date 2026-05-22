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
        title: '智能问数',
        shortLabel: '问数',
        description: '自然语言查询财务数据',
        icon: 'robot',
        to: '/chat'
      },
      {
        title: '财报记录',
        shortLabel: '记录',
        description: '上传财报文件、查看解析状态',
        icon: 'file-lines',
        to: '/reports/list'
      }
    ]
  }
]
