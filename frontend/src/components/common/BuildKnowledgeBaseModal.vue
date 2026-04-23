<script setup>
import { ref, computed, watch } from 'vue'
import { buildPreview, buildFullStream } from '@/api/knowledge_base'

const props = defineProps({
  visible: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits(['close', 'success'])

const currentStep = ref(1)
const totalSteps = 4

const stockExcelInput = ref(null)
const industryExcelInput = ref(null)
const stockPdfInput = ref(null)
const industryPdfInput = ref(null)

const stockExcelFile = ref(null)
const industryExcelFile = ref(null)
const stockPdfFiles = ref([])
const industryPdfFiles = ref([])

const isLoading = ref(false)
const errorMessage = ref('')

const previewData = ref(null)
const buildResult = ref(null)
const matchResult = ref(null)

const buildProgress = ref(null)
const buildLogs = ref([])

const stepLabels = ['上传Excel', '上传PDF', '预览匹配', '确认构建']

watch(() => props.visible, (newVal) => {
  if (newVal) {
    resetState()
  }
})

const resetState = () => {
  currentStep.value = 1
  stockExcelFile.value = null
  industryExcelFile.value = null
  stockPdfFiles.value = []
  industryPdfFiles.value = []
  isLoading.value = false
  errorMessage.value = ''
  previewData.value = null
  buildResult.value = null
  matchResult.value = null
  buildProgress.value = null
  buildLogs.value = []
}

const canGoNext = computed(() => {
  if (currentStep.value === 1) {
    return stockExcelFile.value && industryExcelFile.value
  }
  if (currentStep.value === 2) {
    return true
  }
  if (currentStep.value === 3) {
    return stockPdfFiles.value.length > 0 || industryPdfFiles.value.length > 0
  }
  return false
})

const triggerStockExcelInput = () => stockExcelInput.value?.click()
const triggerIndustryExcelInput = () => industryExcelInput.value?.click()
const triggerStockPdfInput = () => stockPdfInput.value?.click()
const triggerIndustryPdfInput = () => industryPdfInput.value?.click()

const handleStockExcelSelect = (event) => {
  const file = event.target.files?.[0]
  if (file) {
    if (!file.name.toLowerCase().endsWith('.xlsx') && !file.name.toLowerCase().endsWith('.xls')) {
      errorMessage.value = '请选择 Excel 文件（.xlsx 或 .xls）'
      return
    }
    stockExcelFile.value = file
    errorMessage.value = ''
  }
  event.target.value = ''
}

const handleIndustryExcelSelect = (event) => {
  const file = event.target.files?.[0]
  if (file) {
    if (!file.name.toLowerCase().endsWith('.xlsx') && !file.name.toLowerCase().endsWith('.xls')) {
      errorMessage.value = '请选择 Excel 文件（.xlsx 或 .xls）'
      return
    }
    industryExcelFile.value = file
    errorMessage.value = ''
  }
  event.target.value = ''
}

const handleStockPdfSelect = (event) => {
  const files = Array.from(event.target.files || []).filter(f => f.name.toLowerCase().endsWith('.pdf'))
  if (files.length === 0) {
    errorMessage.value = '请选择 PDF 文件'
    return
  }
  stockPdfFiles.value = files
  errorMessage.value = ''
  event.target.value = ''
}

const handleIndustryPdfSelect = (event) => {
  const files = Array.from(event.target.files || []).filter(f => f.name.toLowerCase().endsWith('.pdf'))
  if (files.length === 0) {
    errorMessage.value = '请选择 PDF 文件'
    return
  }
  industryPdfFiles.value = files
  errorMessage.value = ''
  event.target.value = ''
}

const removeStockPdf = (index) => {
  stockPdfFiles.value.splice(index, 1)
}

const removeIndustryPdf = (index) => {
  industryPdfFiles.value.splice(index, 1)
}

const handleNext = async () => {
  if (currentStep.value === 1) {
    await handlePreview()
  } else if (currentStep.value === 2) {
    computeMatchResult()
    currentStep.value = 3
  } else if (currentStep.value < totalSteps) {
    currentStep.value++
  }
}

const handlePrev = () => {
  if (currentStep.value > 1) {
    currentStep.value--
    errorMessage.value = ''
  }
}

const handlePreview = async () => {
  if (!stockExcelFile.value || !industryExcelFile.value) return

  isLoading.value = true
  errorMessage.value = ''

  try {
    const formData = new FormData()
    formData.append('stock_excel', stockExcelFile.value)
    formData.append('industry_excel', industryExcelFile.value)

    const res = await buildPreview(formData)
    previewData.value = res?.data || res
    currentStep.value = 2
  } catch (error) {
    errorMessage.value = error.message || '预览失败'
  } finally {
    isLoading.value = false
  }
}

const computeMatchResult = () => {
  const stockTitles = new Set()
  const industryTitles = new Set()

  if (previewData.value) {
    const stockSample = previewData.value.stock_sample || []
    const industrySample = previewData.value.industry_sample || []
    stockSample.forEach(r => { if (r.title) stockTitles.add(r.title.trim()) })
    industrySample.forEach(r => { if (r.title) industryTitles.add(r.title.trim()) })
  }

  const stockPdfNames = stockPdfFiles.value.map(f => f.name.replace(/\.pdf$/i, ''))
  const industryPdfNames = industryPdfFiles.value.map(f => f.name.replace(/\.pdf$/i, ''))

  const stockMatched = stockPdfNames.filter(name => stockTitles.has(name))
  const stockUnmatched = stockPdfNames.filter(name => !stockTitles.has(name))
  const industryMatched = industryPdfNames.filter(name => industryTitles.has(name))
  const industryUnmatched = industryPdfNames.filter(name => !industryTitles.has(name))

  matchResult.value = {
    stockMatched: stockMatched.length,
    stockUnmatched: stockUnmatched.length,
    stockUnmatchedNames: stockUnmatched.slice(0, 20),
    industryMatched: industryMatched.length,
    industryUnmatched: industryUnmatched.length,
    industryUnmatchedNames: industryUnmatched.slice(0, 20),
    totalMatched: stockMatched.length + industryMatched.length,
    totalPdfs: stockPdfNames.length + industryPdfNames.length,
  }
}

const handleBuild = async () => {
  isLoading.value = true
  errorMessage.value = ''
  buildResult.value = null
  buildProgress.value = null
  buildLogs.value = []

  try {
    const formData = new FormData()
    formData.append('stock_excel', stockExcelFile.value)
    formData.append('industry_excel', industryExcelFile.value)

    stockPdfFiles.value.forEach(file => {
      formData.append('stock_pdfs', file)
    })
    industryPdfFiles.value.forEach(file => {
      formData.append('industry_pdfs', file)
    })

    const response = await buildFullStream(formData)

    if (!response.ok) {
      throw new Error(`请求失败: ${response.status}`)
    }

    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''

    while (true) {
      const { done, value } = await reader.read()
      if (done) break

      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      buffer = lines.pop() || ''

      for (const line of lines) {
        const trimmed = line.trim()
        if (!trimmed) continue

        try {
          const event = JSON.parse(trimmed)

          if (event.type === 'progress') {
            buildProgress.value = event
            buildLogs.value.push(event.message)
            if (buildLogs.value.length > 200) {
              buildLogs.value = buildLogs.value.slice(-200)
            }
          } else if (event.type === 'complete') {
            buildResult.value = event.data
            currentStep.value = 4
            emit('success')
          }
        } catch (parseErr) {
          // ignore
        }
      }
    }
  } catch (error) {
    errorMessage.value = error.message || '构建失败'
  } finally {
    isLoading.value = false
  }
}

const handleClose = () => {
  if (isLoading.value) {
    if (!confirm('操作进行中，确定要关闭吗？')) return
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

        <div class="relative w-full max-w-2xl rounded-[32px] bg-white shadow-2xl">
          <div class="flex items-center justify-between border-b border-black/5 px-6 py-5">
            <div>
              <h3 class="text-lg font-semibold text-ink-900">构建知识库</h3>
              <p class="mt-1 text-sm text-ink-500">上传 Excel 元数据与 PDF 文件完成知识库构建</p>
            </div>
            <button
              type="button"
              class="rounded-full p-2 text-ink-400 hover:bg-ink-100 hover:text-ink-600"
              @click="handleClose"
            >
              <FontAwesomeIcon :icon="['fas', 'xmark']" class="text-lg" aria-hidden="true" />
            </button>
          </div>

          <div class="px-6 pt-4">
            <div class="flex items-center gap-1">
              <template v-for="(label, index) in stepLabels" :key="index">
                <div
                  class="flex h-8 items-center justify-center rounded-full px-3 text-xs font-medium transition-colors"
                  :class="{
                    'bg-ink-900 text-white': currentStep === index + 1,
                    'bg-ink-100 text-ink-500': currentStep !== index + 1 && currentStep < index + 1,
                    'bg-green-100 text-green-700': currentStep > index + 1
                  }"
                >
                  <span v-if="currentStep > index + 1" class="mr-1">
                    <FontAwesomeIcon :icon="['fas', 'check']" class="text-[10px]" />
                  </span>
                  {{ label }}
                </div>
                <div
                  v-if="index < stepLabels.length - 1"
                  class="h-px flex-1"
                  :class="currentStep > index + 1 ? 'bg-green-300' : 'bg-ink-200'"
                ></div>
              </template>
            </div>
          </div>

          <div class="p-6 min-h-[300px]">
            <div
              v-if="errorMessage"
              class="mb-4 rounded-xl border border-red-200 bg-red-50 p-4"
            >
              <p class="text-sm text-red-600">
                <FontAwesomeIcon :icon="['fas', 'exclamation-circle']" class="mr-1" />
                {{ errorMessage }}
              </p>
            </div>

            <!-- Step 1: 上传 Excel -->
            <div v-if="currentStep === 1">
              <div class="space-y-4">
                <div>
                  <label class="mb-2 block text-sm font-medium text-ink-700">
                    个股研报元数据
                    <span class="text-ink-400 font-normal">（个股_研报信息.xlsx）</span>
                  </label>
                  <div
                    class="flex min-h-[100px] cursor-pointer flex-col items-center justify-center rounded-2xl border-2 border-dashed transition-colors"
                    :class="stockExcelFile ? 'border-green-300 bg-green-50/50' : 'border-ink-200 bg-ink-50/50 hover:border-ink-300 hover:bg-ink-50'"
                    @click="triggerStockExcelInput"
                  >
                    <template v-if="stockExcelFile">
                      <div class="flex items-center gap-2">
                        <FontAwesomeIcon :icon="['fas', 'file-excel']" class="text-green-600 text-xl" />
                        <span class="text-sm font-medium text-ink-900">{{ stockExcelFile.name }}</span>
                        <span class="text-xs text-ink-400">({{ formatFileSize(stockExcelFile.size) }})</span>
                      </div>
                    </template>
                    <template v-else>
                      <div class="flex h-10 w-10 items-center justify-center rounded-xl bg-ink-900 text-white">
                        <FontAwesomeIcon :icon="['fas', 'cloud-arrow-up']" />
                      </div>
                      <p class="mt-2 text-sm text-ink-600">点击选择个股研报 Excel</p>
                    </template>
                  </div>
                </div>

                <div>
                  <label class="mb-2 block text-sm font-medium text-ink-700">
                    行业研报元数据
                    <span class="text-ink-400 font-normal">（行业_研报信息.xlsx）</span>
                  </label>
                  <div
                    class="flex min-h-[100px] cursor-pointer flex-col items-center justify-center rounded-2xl border-2 border-dashed transition-colors"
                    :class="industryExcelFile ? 'border-green-300 bg-green-50/50' : 'border-ink-200 bg-ink-50/50 hover:border-ink-300 hover:bg-ink-50'"
                    @click="triggerIndustryExcelInput"
                  >
                    <template v-if="industryExcelFile">
                      <div class="flex items-center gap-2">
                        <FontAwesomeIcon :icon="['fas', 'file-excel']" class="text-green-600 text-xl" />
                        <span class="text-sm font-medium text-ink-900">{{ industryExcelFile.name }}</span>
                        <span class="text-xs text-ink-400">({{ formatFileSize(industryExcelFile.size) }})</span>
                      </div>
                    </template>
                    <template v-else>
                      <div class="flex h-10 w-10 items-center justify-center rounded-xl bg-ink-900 text-white">
                        <FontAwesomeIcon :icon="['fas', 'cloud-arrow-up']" />
                      </div>
                      <p class="mt-2 text-sm text-ink-600">点击选择行业研报 Excel</p>
                    </template>
                  </div>
                </div>

                <div class="rounded-xl bg-ink-50 p-3">
                  <p class="text-xs text-ink-500">
                    <FontAwesomeIcon :icon="['fas', 'info-circle']" class="mr-1" />
                    Excel 元数据将先于 PDF 上传，确保后续匹配时元数据完整。
                  </p>
                </div>
              </div>

              <input ref="stockExcelInput" type="file" accept=".xlsx,.xls" class="hidden" @change="handleStockExcelSelect" />
              <input ref="industryExcelInput" type="file" accept=".xlsx,.xls" class="hidden" @change="handleIndustryExcelSelect" />
            </div>

            <!-- Step 2: 上传 PDF -->
            <div v-else-if="currentStep === 2">
              <div class="space-y-4">
                <div v-if="previewData" class="mb-4 rounded-xl border border-green-200 bg-green-50 p-4">
                  <p class="text-sm font-medium text-green-700">Excel 解析成功</p>
                  <div class="mt-2 grid grid-cols-2 gap-2 text-xs text-green-600">
                    <span>个股研报: {{ previewData.stock_records }} 条</span>
                    <span>行业研报: {{ previewData.industry_records }} 条</span>
                  </div>
                </div>

                <div>
                  <label class="mb-2 block text-sm font-medium text-ink-700">
                    个股研报 PDF
                    <span class="text-ink-400 font-normal">（共 {{ stockPdfFiles.length }} 个文件）</span>
                  </label>
                  <div
                    class="flex min-h-[80px] cursor-pointer flex-col items-center justify-center rounded-2xl border-2 border-dashed transition-colors"
                    :class="stockPdfFiles.length > 0 ? 'border-green-300 bg-green-50/50' : 'border-ink-200 bg-ink-50/50 hover:border-ink-300 hover:bg-ink-50'"
                    @click="triggerStockPdfInput"
                  >
                    <template v-if="stockPdfFiles.length > 0">
                      <div class="flex items-center gap-2">
                        <FontAwesomeIcon :icon="['fas', 'file-pdf']" class="text-red-500 text-xl" />
                        <span class="text-sm font-medium text-ink-900">已选择 {{ stockPdfFiles.length }} 个 PDF</span>
                      </div>
                    </template>
                    <template v-else>
                      <div class="flex h-10 w-10 items-center justify-center rounded-xl bg-ink-900 text-white">
                        <FontAwesomeIcon :icon="['fas', 'cloud-arrow-up']" />
                      </div>
                      <p class="mt-2 text-sm text-ink-600">点击选择个股研报 PDF 文件</p>
                    </template>
                  </div>

                  <div v-if="stockPdfFiles.length > 0" class="mt-2 max-h-[120px] overflow-y-auto rounded-xl border border-black/5">
                    <div
                      v-for="(file, index) in stockPdfFiles.slice(0, 10)"
                      :key="'stock-' + index"
                      class="flex items-center gap-2 border-b border-black/5 px-3 py-1.5 last:border-b-0"
                    >
                      <FontAwesomeIcon :icon="['fas', 'file-pdf']" class="text-xs text-red-400" />
                      <span class="flex-1 truncate text-xs text-ink-700">{{ file.name }}</span>
                      <button
                        type="button"
                        class="rounded p-0.5 text-ink-400 hover:bg-ink-100 hover:text-ink-600"
                        @click.stop="removeStockPdf(index)"
                      >
                        <FontAwesomeIcon :icon="['fas', 'xmark']" class="text-xs" />
                      </button>
                    </div>
                    <div v-if="stockPdfFiles.length > 10" class="px-3 py-1.5 text-center text-xs text-ink-400">
                      还有 {{ stockPdfFiles.length - 10 }} 个文件...
                    </div>
                  </div>
                </div>

                <div>
                  <label class="mb-2 block text-sm font-medium text-ink-700">
                    行业研报 PDF
                    <span class="text-ink-400 font-normal">（共 {{ industryPdfFiles.length }} 个文件）</span>
                  </label>
                  <div
                    class="flex min-h-[80px] cursor-pointer flex-col items-center justify-center rounded-2xl border-2 border-dashed transition-colors"
                    :class="industryPdfFiles.length > 0 ? 'border-green-300 bg-green-50/50' : 'border-ink-200 bg-ink-50/50 hover:border-ink-300 hover:bg-ink-50'"
                    @click="triggerIndustryPdfInput"
                  >
                    <template v-if="industryPdfFiles.length > 0">
                      <div class="flex items-center gap-2">
                        <FontAwesomeIcon :icon="['fas', 'file-pdf']" class="text-red-500 text-xl" />
                        <span class="text-sm font-medium text-ink-900">已选择 {{ industryPdfFiles.length }} 个 PDF</span>
                      </div>
                    </template>
                    <template v-else>
                      <div class="flex h-10 w-10 items-center justify-center rounded-xl bg-ink-900 text-white">
                        <FontAwesomeIcon :icon="['fas', 'cloud-arrow-up']" />
                      </div>
                      <p class="mt-2 text-sm text-ink-600">点击选择行业研报 PDF 文件</p>
                    </template>
                  </div>

                  <div v-if="industryPdfFiles.length > 0" class="mt-2 max-h-[120px] overflow-y-auto rounded-xl border border-black/5">
                    <div
                      v-for="(file, index) in industryPdfFiles.slice(0, 10)"
                      :key="'industry-' + index"
                      class="flex items-center gap-2 border-b border-black/5 px-3 py-1.5 last:border-b-0"
                    >
                      <FontAwesomeIcon :icon="['fas', 'file-pdf']" class="text-xs text-red-400" />
                      <span class="flex-1 truncate text-xs text-ink-700">{{ file.name }}</span>
                      <button
                        type="button"
                        class="rounded p-0.5 text-ink-400 hover:bg-ink-100 hover:text-ink-600"
                        @click.stop="removeIndustryPdf(index)"
                      >
                        <FontAwesomeIcon :icon="['fas', 'xmark']" class="text-xs" />
                      </button>
                    </div>
                    <div v-if="industryPdfFiles.length > 10" class="px-3 py-1.5 text-center text-xs text-ink-400">
                      还有 {{ industryPdfFiles.length - 10 }} 个文件...
                    </div>
                  </div>
                </div>

                <div class="rounded-xl bg-ink-50 p-3">
                  <p class="text-xs text-ink-500">
                    <FontAwesomeIcon :icon="['fas', 'info-circle']" class="mr-1" />
                    PDF 文件名需与 Excel 中的 title 字段精确匹配（不含 .pdf 扩展名）。
                  </p>
                </div>
              </div>

              <input ref="stockPdfInput" type="file" accept=".pdf" multiple class="hidden" @change="handleStockPdfSelect" />
              <input ref="industryPdfInput" type="file" accept=".pdf" multiple class="hidden" @change="handleIndustryPdfSelect" />
            </div>

            <!-- Step 3: 预览匹配 -->
            <div v-else-if="currentStep === 3">
              <div class="space-y-4">
                <div v-if="previewData" class="space-y-3">
                  <div class="rounded-xl border border-black/5 p-4">
                    <h4 class="text-sm font-semibold text-ink-900 mb-2">元数据概览</h4>
                    <div class="grid grid-cols-2 gap-3">
                      <div class="rounded-lg bg-blue-50 p-3">
                        <p class="text-xs text-blue-600">个股研报元数据</p>
                        <p class="text-2xl font-bold text-blue-700">{{ previewData.stock_records }}</p>
                        <p class="text-xs text-blue-500 mt-1">条记录</p>
                      </div>
                      <div class="rounded-lg bg-purple-50 p-3">
                        <p class="text-xs text-purple-600">行业研报元数据</p>
                        <p class="text-2xl font-bold text-purple-700">{{ previewData.industry_records }}</p>
                        <p class="text-xs text-purple-500 mt-1">条记录</p>
                      </div>
                    </div>
                  </div>

                  <div class="rounded-xl border border-black/5 p-4">
                    <h4 class="text-sm font-semibold text-ink-900 mb-2">PDF 文件概览</h4>
                    <div class="grid grid-cols-2 gap-3">
                      <div class="rounded-lg bg-red-50 p-3">
                        <p class="text-xs text-red-600">个股研报 PDF</p>
                        <p class="text-2xl font-bold text-red-700">{{ stockPdfFiles.length }}</p>
                        <p class="text-xs text-red-500 mt-1">个文件</p>
                      </div>
                      <div class="rounded-lg bg-orange-50 p-3">
                        <p class="text-xs text-orange-600">行业研报 PDF</p>
                        <p class="text-2xl font-bold text-orange-700">{{ industryPdfFiles.length }}</p>
                        <p class="text-xs text-orange-500 mt-1">个文件</p>
                      </div>
                    </div>
                  </div>

                  <div v-if="matchResult" class="rounded-xl border p-4" :class="matchResult.totalMatched === matchResult.totalPdfs ? 'border-green-200 bg-green-50' : 'border-yellow-200 bg-yellow-50'">
                    <h4 class="text-sm font-semibold mb-2" :class="matchResult.totalMatched === matchResult.totalPdfs ? 'text-green-700' : 'text-yellow-700'">
                      匹配预览
                    </h4>
                    <div class="grid grid-cols-2 gap-3">
                      <div class="rounded-lg bg-white/80 p-3">
                        <p class="text-xs text-ink-500">个股匹配</p>
                        <p class="text-xl font-bold text-green-600">{{ matchResult.stockMatched }} <span class="text-sm text-ink-400">/ {{ stockPdfFiles.length }}</span></p>
                      </div>
                      <div class="rounded-lg bg-white/80 p-3">
                        <p class="text-xs text-ink-500">行业匹配</p>
                        <p class="text-xl font-bold text-green-600">{{ matchResult.industryMatched }} <span class="text-sm text-ink-400">/ {{ industryPdfFiles.length }}</span></p>
                      </div>
                    </div>
                    <div v-if="matchResult.stockUnmatchedNames.length > 0" class="mt-3">
                      <p class="text-xs font-medium text-yellow-600 mb-1">未匹配的个股研报 PDF（{{ matchResult.stockUnmatched }} 个）</p>
                      <div class="max-h-[60px] overflow-y-auto">
                        <p v-for="name in matchResult.stockUnmatchedNames" :key="name" class="text-xs text-yellow-500 truncate">{{ name }}</p>
                      </div>
                    </div>
                    <div v-if="matchResult.industryUnmatchedNames.length > 0" class="mt-2">
                      <p class="text-xs font-medium text-yellow-600 mb-1">未匹配的行业研报 PDF（{{ matchResult.industryUnmatched }} 个）</p>
                      <div class="max-h-[60px] overflow-y-auto">
                        <p v-for="name in matchResult.industryUnmatchedNames" :key="name" class="text-xs text-yellow-500 truncate">{{ name }}</p>
                      </div>
                    </div>
                  </div>

                  <div v-if="previewData.stock_sample && previewData.stock_sample.length > 0" class="rounded-xl border border-black/5 p-4">
                    <h4 class="text-sm font-semibold text-ink-900 mb-2">个股研报样例</h4>
                    <div class="overflow-x-auto">
                      <table class="w-full text-xs">
                        <thead>
                          <tr class="border-b border-black/5">
                            <th class="py-1 text-left text-ink-500">标题</th>
                            <th class="py-1 text-left text-ink-500">股票代码</th>
                            <th class="py-1 text-left text-ink-500">股票名称</th>
                            <th class="py-1 text-left text-ink-500">机构</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr v-for="(item, idx) in previewData.stock_sample.slice(0, 3)" :key="idx" class="border-b border-black/5 last:border-b-0">
                            <td class="py-1 text-ink-700 max-w-[200px] truncate" :title="item.title">{{ item.title }}</td>
                            <td class="py-1 text-ink-700">{{ item.stockCode }}</td>
                            <td class="py-1 text-ink-700">{{ item.stockName }}</td>
                            <td class="py-1 text-ink-700 max-w-[100px] truncate" :title="item.orgName">{{ item.orgName }}</td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <div v-if="previewData.industry_sample && previewData.industry_sample.length > 0" class="rounded-xl border border-black/5 p-4">
                    <h4 class="text-sm font-semibold text-ink-900 mb-2">行业研报样例</h4>
                    <div class="overflow-x-auto">
                      <table class="w-full text-xs">
                        <thead>
                          <tr class="border-b border-black/5">
                            <th class="py-1 text-left text-ink-500">标题</th>
                            <th class="py-1 text-left text-ink-500">行业</th>
                            <th class="py-1 text-left text-ink-500">机构</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr v-for="(item, idx) in previewData.industry_sample.slice(0, 3)" :key="idx" class="border-b border-black/5 last:border-b-0">
                            <td class="py-1 text-ink-700 max-w-[200px] truncate" :title="item.title">{{ item.title }}</td>
                            <td class="py-1 text-ink-700">{{ item.industryName }}</td>
                            <td class="py-1 text-ink-700 max-w-[100px] truncate" :title="item.orgName">{{ item.orgName }}</td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>

                <div v-if="isLoading && buildProgress" class="rounded-xl border border-blue-200 bg-blue-50 p-4">
                  <div class="flex items-center justify-between mb-2">
                    <h4 class="text-sm font-semibold text-blue-700">构建进度</h4>
                    <span class="text-xs text-blue-500">
                      {{ buildProgress.current || 0 }} / {{ buildProgress.total || 0 }}
                    </span>
                  </div>
                  <div class="h-2 rounded-full bg-blue-200 overflow-hidden mb-3">
                    <div
                      class="h-full rounded-full bg-blue-600 transition-all duration-300"
                      :style="{ width: buildProgress.total ? ((buildProgress.current / buildProgress.total) * 100) + '%' : '0%' }"
                    ></div>
                  </div>
                  <div class="grid grid-cols-3 gap-2 text-center mb-3">
                    <div class="rounded-lg bg-white/80 p-2">
                      <p class="text-xs text-ink-500">已匹配</p>
                      <p class="text-lg font-bold text-green-600">{{ buildProgress.matched || 0 }}</p>
                    </div>
                    <div class="rounded-lg bg-white/80 p-2">
                      <p class="text-xs text-ink-500">已注册</p>
                      <p class="text-lg font-bold text-blue-600">{{ buildProgress.registered || 0 }}</p>
                    </div>
                    <div class="rounded-lg bg-white/80 p-2">
                      <p class="text-xs text-ink-500">已切块</p>
                      <p class="text-lg font-bold text-purple-600">{{ buildProgress.chunked || 0 }}</p>
                    </div>
                  </div>
                  <div v-if="buildLogs.length > 0" class="max-h-[120px] overflow-y-auto rounded-lg bg-white/80 border border-blue-100 p-2">
                    <p v-for="(log, idx) in buildLogs.slice(-10)" :key="idx" class="text-xs text-ink-600 py-0.5">{{ log }}</p>
                  </div>
                </div>

                <div v-if="!isLoading" class="rounded-xl bg-yellow-50 border border-yellow-200 p-3">
                  <p class="text-xs text-yellow-700">
                    <FontAwesomeIcon :icon="['fas', 'exclamation-triangle']" class="mr-1" />
                    确认后将开始构建知识库（注册文档 + 切块），此过程可能需要较长时间。向量化需在构建完成后单独操作。
                  </p>
                </div>
              </div>
            </div>

            <!-- Step 4: 构建结果 -->
            <div v-else-if="currentStep === 4">
              <div v-if="buildResult" class="space-y-4">
                <div class="rounded-xl border p-4" :class="buildResult.errors?.length > 0 ? 'border-yellow-200 bg-yellow-50' : 'border-green-200 bg-green-50'">
                  <h4 class="text-sm font-semibold mb-3" :class="buildResult.errors?.length > 0 ? 'text-yellow-700' : 'text-green-700'">
                    构建结果
                  </h4>
                  <div class="grid grid-cols-2 gap-3">
                    <div class="rounded-lg bg-white/80 p-3">
                      <p class="text-xs text-ink-500">个股元数据</p>
                      <p class="text-xl font-bold text-ink-900">{{ buildResult.stock_metadata_count }}</p>
                    </div>
                    <div class="rounded-lg bg-white/80 p-3">
                      <p class="text-xs text-ink-500">行业元数据</p>
                      <p class="text-xl font-bold text-ink-900">{{ buildResult.industry_metadata_count }}</p>
                    </div>
                    <div class="rounded-lg bg-white/80 p-3">
                      <p class="text-xs text-ink-500">匹配成功</p>
                      <p class="text-xl font-bold text-green-600">{{ buildResult.matched }}</p>
                    </div>
                    <div class="rounded-lg bg-white/80 p-3">
                      <p class="text-xs text-ink-500">注册文档</p>
                      <p class="text-xl font-bold text-blue-600">{{ buildResult.registered }}</p>
                    </div>
                    <div class="rounded-lg bg-white/80 p-3">
                      <p class="text-xs text-ink-500">切块完成</p>
                      <p class="text-xl font-bold text-purple-600">{{ buildResult.chunked }}</p>
                    </div>
                    <div class="rounded-lg bg-white/80 p-3">
                      <p class="text-xs text-ink-500">总切块数</p>
                      <p class="text-xl font-bold text-ink-900">{{ buildResult.chunk_total }}</p>
                    </div>
                  </div>
                </div>

                <div v-if="buildResult.unmatched_pdfs && buildResult.unmatched_pdfs.length > 0" class="rounded-xl border border-yellow-200 bg-yellow-50 p-4">
                  <p class="text-sm font-medium text-yellow-700">
                    未匹配的 PDF（{{ buildResult.unmatched_pdfs.length }} 个）
                  </p>
                  <div class="mt-2 max-h-[100px] overflow-y-auto">
                    <p v-for="(name, idx) in buildResult.unmatched_pdfs.slice(0, 10)" :key="idx" class="text-xs text-yellow-600 truncate">
                      {{ name }}
                    </p>
                    <p v-if="buildResult.unmatched_pdfs.length > 10" class="text-xs text-yellow-500">
                      ...还有 {{ buildResult.unmatched_pdfs.length - 10 }} 个
                    </p>
                  </div>
                </div>

                <div v-if="buildResult.errors && buildResult.errors.length > 0" class="rounded-xl border border-red-200 bg-red-50 p-4">
                  <p class="text-sm font-medium text-red-700">
                    错误（{{ buildResult.errors.length }} 个）
                  </p>
                  <div class="mt-2 max-h-[100px] overflow-y-auto">
                    <p v-for="(err, idx) in buildResult.errors.slice(0, 5)" :key="idx" class="text-xs text-red-600">
                      {{ err.file || err.phase || '未知' }}: {{ err.error }}
                    </p>
                  </div>
                </div>

                <div class="rounded-xl bg-ink-50 p-3">
                  <p class="text-xs text-ink-500">
                    <FontAwesomeIcon :icon="['fas', 'info-circle']" class="mr-1" />
                    构建完成。如需向量化，请返回知识库管理页面点击"批量向量化"按钮。
                  </p>
                </div>
              </div>
            </div>

            <div v-if="isLoading" class="flex items-center justify-center py-8">
              <FontAwesomeIcon :icon="['fas', 'spinner']" spin class="text-2xl text-ink-400" />
              <span class="ml-3 text-sm text-ink-500">
                {{ currentStep === 1 ? '解析 Excel 中...' : (buildProgress?.message || '构建知识库中，请耐心等待...') }}
              </span>
            </div>
          </div>

          <div class="flex justify-between gap-3 border-t border-black/5 px-6 py-4">
            <button
              v-if="currentStep > 1 && currentStep < 4"
              type="button"
              class="shell-button-secondary"
              :disabled="isLoading"
              @click="handlePrev"
            >
              上一步
            </button>
            <div v-else></div>

            <div class="flex gap-3">
              <button
                v-if="currentStep < 4"
                type="button"
                class="shell-button-secondary"
                @click="handleClose"
              >
                取消
              </button>

              <button
                v-if="currentStep === 1"
                type="button"
                class="shell-button"
                :disabled="!canGoNext || isLoading"
                @click="handleNext"
              >
                <FontAwesomeIcon v-if="isLoading" :icon="['fas', 'spinner']" spin />
                <span>{{ isLoading ? '解析中...' : '下一步' }}</span>
              </button>

              <button
                v-if="currentStep === 2"
                type="button"
                class="shell-button"
                :disabled="isLoading"
                @click="handleNext"
              >
                <span>预览匹配</span>
              </button>

              <button
                v-if="currentStep === 3"
                type="button"
                class="shell-button"
                :disabled="!canGoNext || isLoading"
                @click="handleBuild"
              >
                <FontAwesomeIcon v-if="isLoading" :icon="['fas', 'spinner']" spin />
                <FontAwesomeIcon v-else :icon="['fas', 'database']" />
                <span>{{ isLoading ? '构建中...' : (canGoNext ? '开始构建' : '请先上传PDF') }}</span>
              </button>

              <button
                v-if="currentStep === 4"
                type="button"
                class="shell-button"
                @click="handleClose"
              >
                完成
              </button>
            </div>
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
