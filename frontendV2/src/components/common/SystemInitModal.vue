<script setup>
import { ref, watch } from 'vue'
import { initKnowledgeBase } from '@/api/knowledgeBase'

const props = defineProps({
  visible: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits(['close', 'success'])

const excelInput = ref(null)
const selectedFile = ref(null)
const selectedDocType = ref('RESEARCH_REPORT')
const isInitializing = ref(false)

// 分别记录两种类型的初始化结果
const stockResult = ref(null)
const industryResult = ref(null)

const DOC_TYPE_OPTIONS = [
  { value: 'RESEARCH_REPORT', label: '个股研报' },
  { value: 'INDUSTRY_REPORT', label: '行业研报' }
]

const DOC_TYPE_LABEL = {
  RESEARCH_REPORT: '个股研报',
  INDUSTRY_REPORT: '行业研报'
}

watch(() => props.visible, (newVal) => {
  if (newVal) {
    resetState()
  }
})

const resetState = () => {
  selectedFile.value = null
  selectedDocType.value = 'RESEARCH_REPORT'
  isInitializing.value = false
  stockResult.value = null
  industryResult.value = null
}

const triggerExcelInput = () => {
  excelInput.value?.click()
}

const handleExcelSelect = (event) => {
  const file = event.target.files?.[0]
  if (file) {
    if (!file.name.toLowerCase().endsWith('.xlsx') && !file.name.toLowerCase().endsWith('.xls')) {
      alert('请选择 Excel 文件（.xlsx 或 .xls）')
      return
    }
    selectedFile.value = file
  }
  event.target.value = ''
}

const canInit = () => {
  return selectedFile.value && !isInitializing.value
}

const handleInit = async () => {
  if (!canInit()) return

  isInitializing.value = true

  try {
    const result = await initKnowledgeBase(selectedFile.value, selectedDocType.value)
    const data = result?.data || result

    if (selectedDocType.value === 'RESEARCH_REPORT') {
      stockResult.value = data
    } else {
      industryResult.value = data
    }

    selectedFile.value = null
    emit('success')
  } catch (error) {
    const errorResult = {
      success: false,
      message: error.message || '导入失败',
      total_count: 0
    }
    if (selectedDocType.value === 'RESEARCH_REPORT') {
      stockResult.value = errorResult
    } else {
      industryResult.value = errorResult
    }
  } finally {
    isInitializing.value = false
  }
}

const handleClose = () => {
  if (isInitializing.value) {
    if (!confirm('初始化进行中，确定要关闭吗？')) return
  }
  emit('close')
}

const formatFileSize = (bytes) => {
  if (bytes < 1024) return bytes + ' B'
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB'
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB'
}
</script>

<template>
  <Teleport to="body">
    <Transition name="modal">
      <div v-if="visible" class="fixed inset-0 z-50 flex items-center justify-center p-4">
        <div class="absolute inset-0 bg-black/50 backdrop-blur-sm" @click="handleClose"></div>

        <div class="relative w-full max-w-xl rounded-[32px] bg-white shadow-2xl">
          <div class="flex items-center justify-between border-b border-black/5 px-6 py-5">
            <div>
              <h3 class="text-lg font-semibold text-ink-900">系统初始化</h3>
              <p class="mt-1 text-sm text-ink-500">上传 Excel 元数据文件，导入知识库文档索引</p>
            </div>
            <button
              type="button"
              class="rounded-full p-2 text-ink-400 hover:bg-ink-100 hover:text-ink-600"
              @click="handleClose"
            >
              <FontAwesomeIcon :icon="['fas', 'xmark']" class="text-lg" aria-hidden="true" />
            </button>
          </div>

          <div class="p-6 space-y-4">
            <!-- 文档类型选择 -->
            <div class="space-y-2">
              <label class="text-sm font-medium text-ink-700">文档类型</label>
              <div class="flex gap-2">
                <button
                  v-for="opt in DOC_TYPE_OPTIONS"
                  :key="opt.value"
                  type="button"
                  class="flex-1 rounded-xl border px-4 py-2.5 text-sm font-medium transition-colors"
                  :class="selectedDocType === opt.value
                    ? 'border-blue-300 bg-blue-50 text-blue-700'
                    : 'border-black/5 bg-white text-ink-600 hover:bg-ink-50'"
                  @click="selectedDocType = opt.value"
                >
                  {{ opt.label }}
                </button>
              </div>
            </div>

            <!-- 文件选择 -->
            <div class="space-y-3">
              <label class="text-sm font-medium text-ink-700">选择 {{ DOC_TYPE_LABEL[selectedDocType] }} Excel 文件</label>
              <div
                class="flex items-center gap-3 rounded-xl border border-black/5 p-3 cursor-pointer hover:bg-ink-50"
                @click="triggerExcelInput"
              >
                <div class="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-blue-50 text-blue-500">
                  <FontAwesomeIcon :icon="['fas', 'file-excel']" class="text-lg" aria-hidden="true" />
                </div>
                <div class="min-w-0 flex-1">
                  <p v-if="selectedFile" class="truncate text-sm font-medium text-ink-900">{{ selectedFile.name }}</p>
                  <p v-else class="text-sm text-ink-400">点击选择 Excel 文件（.xlsx / .xls）</p>
                  <p v-if="selectedFile" class="text-xs text-ink-400">{{ formatFileSize(selectedFile.size) }}</p>
                </div>
                <FontAwesomeIcon :icon="['fas', 'upload']" class="text-ink-300" aria-hidden="true" />
              </div>
              <input
                ref="excelInput"
                type="file"
                accept=".xlsx,.xls"
                class="hidden"
                @change="handleExcelSelect"
              />
            </div>

            <!-- 个股研报结果 -->
            <div
              v-if="stockResult"
              class="rounded-xl border p-4"
              :class="stockResult.success ? 'border-green-200 bg-green-50' : 'border-red-200 bg-red-50'"
            >
              <div class="flex items-center gap-2">
                <span class="text-xs font-medium px-2 py-0.5 rounded-full bg-blue-100 text-blue-700">个股研报</span>
                <p class="text-sm font-medium" :class="stockResult.success ? 'text-green-700' : 'text-red-700'">
                  {{ stockResult.message }}
                </p>
              </div>
              <p v-if="stockResult.total_count" class="mt-1 text-xs text-ink-500">
                导入 {{ stockResult.total_count }} 条<span v-if="stockResult.duplicate_count">，跳过重复 {{ stockResult.duplicate_count }} 条</span>
              </p>
            </div>

            <!-- 行业研报结果 -->
            <div
              v-if="industryResult"
              class="rounded-xl border p-4"
              :class="industryResult.success ? 'border-green-200 bg-green-50' : 'border-red-200 bg-red-50'"
            >
              <div class="flex items-center gap-2">
                <span class="text-xs font-medium px-2 py-0.5 rounded-full bg-purple-100 text-purple-700">行业研报</span>
                <p class="text-sm font-medium" :class="industryResult.success ? 'text-green-700' : 'text-red-700'">
                  {{ industryResult.message }}
                </p>
              </div>
              <p v-if="industryResult.total_count" class="mt-1 text-xs text-ink-500">
                导入 {{ industryResult.total_count }} 条<span v-if="industryResult.duplicate_count">，跳过重复 {{ industryResult.duplicate_count }} 条</span>
              </p>
            </div>
          </div>

          <div class="flex justify-end gap-3 border-t border-black/5 px-6 py-4">
            <button
              type="button"
              class="shell-button-secondary"
              @click="handleClose"
            >
              {{ stockResult || industryResult ? '关闭' : '取消' }}
            </button>
            <button
              type="button"
              class="shell-button"
              :disabled="!canInit()"
              @click="handleInit"
            >
              <FontAwesomeIcon v-if="isInitializing" :icon="['fas', 'spinner']" spin aria-hidden="true" />
              <span>{{ isInitializing ? '导入中...' : '开始导入' }}</span>
            </button>
          </div>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>

<style scoped>
.modal-enter-active,
.modal-leave-active {
  transition: opacity 0.2s ease;
}

.modal-enter-from,
.modal-leave-to {
  opacity: 0;
}

.modal-enter-active .relative,
.modal-leave-active .relative {
  transition: transform 0.2s ease;
}

.modal-enter-from .relative,
.modal-leave-to .relative {
  transform: scale(0.95);
}
</style>
