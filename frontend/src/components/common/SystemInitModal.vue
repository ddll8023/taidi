<script setup>
import { ref, watch } from 'vue'
import { initSystem } from '@/api/knowledge_base'

const props = defineProps({
  visible: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits(['close', 'success'])

const stockExcelInput = ref(null)
const industryExcelInput = ref(null)
const stockExcelFile = ref(null)
const industryExcelFile = ref(null)
const isInitializing = ref(false)
const initResult = ref(null)
const forceReload = ref(false)

watch(() => props.visible, (newVal) => {
  if (newVal) {
    resetState()
  }
})

const resetState = () => {
  stockExcelFile.value = null
  industryExcelFile.value = null
  isInitializing.value = false
  initResult.value = null
  forceReload.value = false
}

const triggerStockExcelInput = () => {
  stockExcelInput.value?.click()
}

const triggerIndustryExcelInput = () => {
  industryExcelInput.value?.click()
}

const handleStockExcelSelect = (event) => {
  const file = event.target.files?.[0]
  if (file) {
    if (!file.name.toLowerCase().endsWith('.xlsx') && !file.name.toLowerCase().endsWith('.xls')) {
      alert('请选择 Excel 文件（.xlsx 或 .xls）')
      return
    }
    stockExcelFile.value = file
  }
  event.target.value = ''
}

const handleIndustryExcelSelect = (event) => {
  const file = event.target.files?.[0]
  if (file) {
    if (!file.name.toLowerCase().endsWith('.xlsx') && !file.name.toLowerCase().endsWith('.xls')) {
      alert('请选择 Excel 文件（.xlsx 或 .xls）')
      return
    }
    industryExcelFile.value = file
  }
  event.target.value = ''
}

const canInit = () => {
  return stockExcelFile.value && industryExcelFile.value && !isInitializing.value
}

const handleInit = async () => {
  if (!canInit()) return

  isInitializing.value = true
  initResult.value = null

  try {
    const formData = new FormData()
    formData.append('stock_excel', stockExcelFile.value)
    formData.append('industry_excel', industryExcelFile.value)

    const response = await initSystem(formData, forceReload.value)
    const result = response?.data || response

    initResult.value = result

    if (result.success) {
      emit('success')
    }
  } catch (error) {
    initResult.value = {
      success: false,
      message: error.message || '初始化失败',
      stock_metadata_count: 0,
      industry_metadata_count: 0,
      total_count: 0,
      duplicates: 0,
      errors: [{ phase: 'init', error: error.message || '未知错误' }]
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
              <p class="mt-1 text-sm text-ink-500">上传Excel元数据文件，初始化知识库文档索引</p>
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
            <div class="space-y-3">
              <label class="text-sm font-medium text-ink-700">个股研报元数据</label>
              <div
                class="flex items-center gap-3 rounded-xl border border-black/5 p-3 cursor-pointer hover:bg-ink-50"
                @click="triggerStockExcelInput"
              >
                <div class="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-blue-50 text-blue-500">
                  <FontAwesomeIcon :icon="['fas', 'file-excel']" class="text-lg" aria-hidden="true" />
                </div>
                <div class="min-w-0 flex-1">
                  <p v-if="stockExcelFile" class="truncate text-sm font-medium text-ink-900">{{ stockExcelFile.name }}</p>
                  <p v-else class="text-sm text-ink-400">点击选择个股研报Excel文件</p>
                  <p v-if="stockExcelFile" class="text-xs text-ink-400">{{ formatFileSize(stockExcelFile.size) }}</p>
                </div>
                <FontAwesomeIcon :icon="['fas', 'upload']" class="text-ink-300" aria-hidden="true" />
              </div>
              <input
                ref="stockExcelInput"
                type="file"
                accept=".xlsx,.xls"
                class="hidden"
                @change="handleStockExcelSelect"
              />
            </div>

            <div class="space-y-3">
              <label class="text-sm font-medium text-ink-700">行业研报元数据</label>
              <div
                class="flex items-center gap-3 rounded-xl border border-black/5 p-3 cursor-pointer hover:bg-ink-50"
                @click="triggerIndustryExcelInput"
              >
                <div class="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-purple-50 text-purple-500">
                  <FontAwesomeIcon :icon="['fas', 'file-excel']" class="text-lg" aria-hidden="true" />
                </div>
                <div class="min-w-0 flex-1">
                  <p v-if="industryExcelFile" class="truncate text-sm font-medium text-ink-900">{{ industryExcelFile.name }}</p>
                  <p v-else class="text-sm text-ink-400">点击选择行业研报Excel文件</p>
                  <p v-if="industryExcelFile" class="text-xs text-ink-400">{{ formatFileSize(industryExcelFile.size) }}</p>
                </div>
                <FontAwesomeIcon :icon="['fas', 'upload']" class="text-ink-300" aria-hidden="true" />
              </div>
              <input
                ref="industryExcelInput"
                type="file"
                accept=".xlsx,.xls"
                class="hidden"
                @change="handleIndustryExcelSelect"
              />
            </div>

            <label class="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                v-model="forceReload"
                class="rounded border-ink-300 text-ink-900 focus:ring-ink-500"
              />
              <span class="text-sm text-ink-600">强制重新加载（清除已有元数据）</span>
            </label>

            <div
              v-if="initResult"
              class="rounded-xl border p-4"
              :class="initResult.success ? 'border-green-200 bg-green-50' : 'border-red-200 bg-red-50'"
            >
              <p
                class="text-sm font-medium"
                :class="initResult.success ? 'text-green-700' : 'text-red-700'"
              >
                {{ initResult.message }}
              </p>
              <div v-if="initResult.success" class="mt-2 space-y-1">
                <p class="text-xs text-ink-500">个股研报元数据：{{ initResult.stock_metadata_count }} 条</p>
                <p class="text-xs text-ink-500">行业研报元数据：{{ initResult.industry_metadata_count }} 条</p>
                <p class="text-xs text-ink-500">总计：{{ initResult.total_count }} 条（重复跳过 {{ initResult.duplicates }} 条）</p>
              </div>
              <div v-if="initResult.errors?.length" class="mt-2">
                <p class="text-xs text-red-600" v-for="(err, i) in initResult.errors" :key="i">
                  {{ err.error || err.message || JSON.stringify(err) }}
                </p>
              </div>
            </div>
          </div>

          <div class="flex justify-end gap-3 border-t border-black/5 px-6 py-4">
            <button
              type="button"
              class="shell-button-secondary"
              @click="handleClose"
            >
              {{ initResult ? '关闭' : '取消' }}
            </button>
            <button
              v-if="!initResult"
              type="button"
              class="shell-button"
              :disabled="!canInit()"
              @click="handleInit"
            >
              <FontAwesomeIcon v-if="isInitializing" :icon="['fas', 'spinner']" spin aria-hidden="true" />
              <span>{{ isInitializing ? '初始化中...' : '开始初始化' }}</span>
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
