import { ref } from 'vue'
import { defineStore } from 'pinia'

export const useWorkbenchStore = defineStore('workbench', () => {
  const stageLabel = ref('财报智能分析前端')
  const environmentLabel = ref('记录 / 上传 / 查看')
  const constraints = ref([
    '统一使用自定义组件与 Tailwind CSS',
    '上传页只承接上传接口',
    '列表页承接向量化操作和状态展示'
  ])

  return {
    constraints,
    environmentLabel,
    stageLabel
  }
})
