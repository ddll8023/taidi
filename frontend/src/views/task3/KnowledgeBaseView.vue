<script setup>
/**
 * 知识库管理页面
 * 功能描述：管理研报文档、增量构建向量索引、支持智能检索
 * 依赖组件：MetricTile, StatusBadge, UploadDocumentModal
 */
import { ref, computed, onMounted, onUnmounted } from 'vue'
import MetricTile from '@/components/common/MetricTile.vue'
import StatusBadge from '@/components/common/StatusBadge.vue'
import SystemInitModal from '@/components/common/SystemInitModal.vue'
import UploadPdfModal from '@/components/common/UploadPdfModal.vue'
import {
  getDocuments,
  getStats,
  chunkDocument,
  chunkDocumentsBatch,
  chunkAllPending,
  vectorizeDocument,
  vectorizeChunks,
  getProgress,
  getInitStatus,
  uploadSinglePdf,
  retryFailed,
  resetVectorStatus
} from '@/api/knowledge_base'

// 统计数据
const stats = ref({
  documents: {
    total: 0,
    by_chunk_status: {},
    by_vector_status: {},
    by_doc_type: {}
  },
  chunks: {
    total: 0,
    by_vector_status: {}
  }
})

// 文档列表
const documents = ref([])
const total = ref(0)
const currentPage = ref(1)
const pageSize = ref(20)
const loading = ref(false)
const statsLoading = ref(false)

// 筛选条件
const filterDocType = ref('')
const filterChunkStatus = ref('')
const filterVectorStatus = ref('')
const filterMetadataStatus = ref('')

// 选中的文档
const selectedIds = ref([])

// 操作加载状态
const chunking = ref(false)
const vectorizing = ref(false)
const showSystemInitModal = ref(false)
const showUploadPdfModal = ref(false)

const progress = ref({
  total_documents: 0,
  metadata_loaded: 0,
  pdf_uploaded: 0,
  chunked: 0,
  vectorized: 0,
  pending_pdf_upload: 0,
  pending_chunk: 0,
  pending_vectorize: 0,
  failed_chunk: 0,
  failed_vectorize: 0,
  progress_percentage: 0.0,
  recent_processed: []
})

const initStatus = ref({
  initialized: false,
  stock_metadata_count: 0,
  industry_metadata_count: 0,
  total_metadata_count: 0
})

// 自动刷新定时器
let refreshTimer = null

// 切块状态映射
const chunkStatusMap = {
  0: { label: '待处理', tone: 'neutral' },
  1: { label: '处理中', tone: 'warning' },
  2: { label: '已完成', tone: 'success' },
  3: { label: '失败', tone: 'danger' }
}

// 向量状态映射
const vectorStatusMap = {
  0: { label: '待处理', tone: 'neutral' },
  1: { label: '处理中', tone: 'warning' },
  2: { label: '已完成', tone: 'success' },
  3: { label: '失败', tone: 'danger' },
  4: { label: '跳过', tone: 'warning' }
}

// 文档类型映射
const docTypeMap = {
  RESEARCH_REPORT: '个股研报',
  INDUSTRY_REPORT: '行业研报',
  FINANCIAL_REPORT: '财报'
}

const metadataStatusMap = {
  0: { label: '未加载', tone: 'neutral' },
  1: { label: '待上传PDF', tone: 'warning' },
  2: { label: '已上传', tone: 'success' }
}

// 计算统计指标
const totalDocuments = computed(() => stats.value.documents.total)

const chunkedDocuments = computed(() => {
  const byChunk = stats.value.documents.by_chunk_status || {}
  return (byChunk[2] || 0) + (byChunk[3] || 0)
})

const vectorizedDocuments = computed(() => {
  const byVec = stats.value.documents.by_vector_status || {}
  return byVec[2] || 0
})

const pendingDocuments = computed(() => {
  const byChunk = stats.value.documents.by_chunk_status || {}
  const byVec = stats.value.documents.by_vector_status || {}
  return (byChunk[0] || 0) + (byVec[0] || 0)
})

const failedDocuments = computed(() => {
  const byChunk = stats.value.documents.by_chunk_status || {}
  const byVec = stats.value.documents.by_vector_status || {}
  return (byChunk[3] || 0) + (byVec[3] || 0)
})

const totalChunks = computed(() => stats.value.chunks.total)

// 是否有处理中的任务
const hasProcessing = computed(() => {
  const byChunk = stats.value.documents.by_chunk_status || {}
  const byVec = stats.value.documents.by_vector_status || {}
  return (byChunk[1] || 0) > 0 || (byVec[1] || 0) > 0
})

// Toast 提示
function showToast(message, type = 'info') {
  const colorMap = {
    success: 'bg-green-600',
    error: 'bg-red-600',
    warning: 'bg-yellow-500',
    info: 'bg-blue-600'
  }
  const toast = document.createElement('div')
  toast.className = `fixed bottom-6 right-6 ${colorMap[type] || colorMap.info} text-white px-4 py-3 rounded-xl shadow-lg z-50 text-sm`
  toast.textContent = message
  document.body.appendChild(toast)
  setTimeout(() => toast.remove(), 3000)
}

// 格式化日期
function formatDate(dateStr) {
  if (!dateStr) return '-'
  try {
    const d = new Date(dateStr)
    return d.toLocaleString('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit'
    })
  } catch {
    return dateStr
  }
}

// 获取文档类型文本
function getDocTypeLabel(type) {
  return docTypeMap[type] || type || '-'
}

// 获取切块状态配置
function getChunkStatusConfig(status) {
  return chunkStatusMap[status] || { label: '未知', tone: 'neutral' }
}

// 获取向量状态配置
function getVectorStatusConfig(status) {
  return vectorStatusMap[status] || { label: '未知', tone: 'neutral' }
}

function getMetadataStatusConfig(status) {
  return metadataStatusMap[status] || { label: '未知', tone: 'neutral' }
}

async function loadProgress() {
  try {
    const res = await getProgress()
    progress.value = res.data || res
  } catch (error) {
    // silent fail
  }
}

async function loadInitStatus() {
  try {
    const res = await getInitStatus()
    initStatus.value = res.data || res
  } catch (error) {
    // silent fail
  }
}

// 加载统计数据
async function loadStats() {
  statsLoading.value = true
  try {
    const res = await getStats()
    stats.value = res.data || res
  } catch (error) {
    showToast('加载统计数据失败', 'error')
  } finally {
    statsLoading.value = false
  }
}

// 加载文档列表
async function loadDocuments() {
  loading.value = true
  try {
    const params = {
      page: currentPage.value,
      page_size: pageSize.value
    }
    if (filterDocType.value) params.doc_type = filterDocType.value
    if (filterChunkStatus.value !== '') params.chunk_status = filterChunkStatus.value
    if (filterVectorStatus.value !== '') params.vector_status = filterVectorStatus.value
    if (filterMetadataStatus.value !== '') params.metadata_status = filterMetadataStatus.value

    const res = await getDocuments(params)
    documents.value = res.data?.lists || []
    total.value = res.data?.pagination?.total || 0
  } catch (error) {
    showToast('加载文档列表失败', 'error')
  } finally {
    loading.value = false
  }
}

// 刷新统计数据和文档列表
async function refreshAll() {
  await Promise.all([loadStats(), loadDocuments(), loadProgress(), loadInitStatus()])
}

function handleSystemInit() {
  showSystemInitModal.value = true
}

async function handleSystemInitSuccess() {
  showToast('系统初始化成功', 'success')
  await refreshAll()
}

function handleUploadPdf() {
  showUploadPdfModal.value = true
}

async function handleUploadPdfSuccess() {
  showToast('PDF上传处理完成', 'success')
  await refreshAll()
}

async function handleSingleUploadPdf(doc) {
  const input = document.createElement('input')
  input.type = 'file'
  input.accept = '.pdf'
  input.onchange = async (event) => {
    const file = event.target.files?.[0]
    if (!file) return
    try {
      const formData = new FormData()
      formData.append('pdf_file', file)
      await uploadSinglePdf(doc.id, formData)
      showToast(`${doc.title} PDF上传成功`, 'success')
      await refreshAll()
    } catch (error) {
      showToast(`上传失败: ${error.message || '未知错误'}`, 'error')
    }
  }
  input.click()
}

async function handleRetryFailed(doc) {
  const input = document.createElement('input')
  input.type = 'file'
  input.accept = '.pdf'
  input.onchange = async (event) => {
    const file = event.target.files?.[0]
    if (!file) return
    try {
      const formData = new FormData()
      formData.append('pdf_file', file)
      await retryFailed(doc.id, formData)
      showToast(`${doc.title} 重试成功`, 'success')
      await refreshAll()
    } catch (error) {
      showToast(`重试失败: ${error.message || '未知错误'}`, 'error')
    }
  }
  input.click()
}

// 切块单个文档
async function handleChunkDocument(doc) {
  try {
    await chunkDocument(doc.id)
    showToast('切块任务已提交', 'success')
    await refreshAll()
  } catch (error) {
    showToast('提交切块任务失败: ' + (error.message || '未知错误'), 'error')
  }
}

// 批量切块选中文档
async function handleBatchChunk() {
  if (selectedIds.value.length === 0) {
    showToast('请先选择要切块的文档', 'warning')
    return
  }

  chunking.value = true
  try {
    await chunkDocumentsBatch(selectedIds.value)
    showToast(`已提交 ${selectedIds.value.length} 个文档的切块任务`, 'success')
    selectedIds.value = []
    await refreshAll()
  } catch (error) {
    showToast('提交切块任务失败: ' + (error.message || '未知错误'), 'error')
  } finally {
    chunking.value = false
  }
}

// 一键切块所有待处理文档
async function handleChunkAll() {
  chunking.value = true
  try {
    const res = await chunkAllPending({ limit: 100 })
    const submitted = res.data?.submitted || 0
    showToast(`已提交 ${submitted} 个文档的切块任务`, 'success')
    await refreshAll()
  } catch (error) {
    showToast('提交切块任务失败: ' + (error.message || '未知错误'), 'error')
  } finally {
    chunking.value = false
  }
}

// 向量化单个文档
async function handleVectorizeDocument(doc) {
  try {
    // vector_status === 3 (FAILED) 或 2 (SUCCESS) 时强制重试
    const force = doc.vector_status === 3 || doc.vector_status === 2
    await vectorizeDocument(doc.id, { force })
    showToast('向量化任务已提交', 'success')
    await refreshAll()
  } catch (error) {
    showToast('提交向量化任务失败: ' + (error.message || '未知错误'), 'error')
  }
}

// 重置向量状态
async function handleResetVectorStatus(doc) {
  try {
    await resetVectorStatus(doc.id)
    showToast('向量状态已重置', 'success')
    await refreshAll()
  } catch (error) {
    showToast('重置状态失败: ' + (error.message || '未知错误'), 'error')
  }
}

// 批量向量化
async function handleBatchVectorize() {
  vectorizing.value = true
  try {
    await vectorizeChunks({ batch_size: 20 })
    showToast('向量化任务已提交', 'success')
    await refreshAll()
  } catch (error) {
    showToast('提交向量化任务失败: ' + (error.message || '未知错误'), 'error')
  } finally {
    vectorizing.value = false
  }
}

// 选择/取消选择文档
function toggleSelect(doc) {
  const index = selectedIds.value.indexOf(doc.id)
  if (index === -1) {
    selectedIds.value.push(doc.id)
  } else {
    selectedIds.value.splice(index, 1)
  }
}

// 全选/取消全选
function toggleSelectAll() {
  if (selectedIds.value.length === documents.value.length) {
    selectedIds.value = []
  } else {
    selectedIds.value = documents.value.map(d => d.id)
  }
}

// 分页处理
function handlePageChange(page) {
  currentPage.value = page
  loadDocuments()
}

function handleSizeChange(size) {
  pageSize.value = size
  currentPage.value = 1
  loadDocuments()
}

// 筛选处理
function handleFilterChange() {
  currentPage.value = 1
  loadDocuments()
}

// 总页数
const totalPages = computed(() => Math.ceil(total.value / pageSize.value) || 1)

// 初始化
onMounted(() => {
  refreshAll()

  refreshTimer = setInterval(() => {
    if (hasProcessing.value) {
      loadStats()
      loadProgress()
    }
  }, 5000)
})

onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
  }
})
</script>

<template>
  <div class="flex flex-col gap-4 p-4 h-full overflow-y-auto">
    <!-- 页面标题 -->
    <div class="flex items-center justify-between rounded-2xl border border-black/5 bg-white/80 p-4 shrink-0">
      <div>
        <h2 class="text-lg font-semibold text-ink-900">知识库管理</h2>
        <p class="mt-1 text-sm text-ink-500">增量构建模式：初始化 → 上传PDF → 向量化</p>
      </div>
      <div class="flex items-center gap-2">
        <span
          v-if="initStatus.initialized"
          class="inline-flex items-center gap-1.5 rounded-full bg-green-50 px-3 py-1 text-xs font-medium text-green-700 border border-green-100"
        >
          <FontAwesomeIcon :icon="['fas', 'check-circle']" aria-hidden="true" />
          已初始化（{{ initStatus.total_metadata_count }} 条元数据）
        </span>
        <span
          v-else
          class="inline-flex items-center gap-1.5 rounded-full bg-yellow-50 px-3 py-1 text-xs font-medium text-yellow-700 border border-yellow-100"
        >
          <FontAwesomeIcon :icon="['fas', 'exclamation-circle']" aria-hidden="true" />
          未初始化
        </span>
        <button
          class="flex h-9 w-9 items-center justify-center rounded-xl border border-ink-200 text-ink-600 transition-colors hover:bg-ink-50 disabled:opacity-50"
          :disabled="statsLoading || loading"
          @click="refreshAll"
        >
          <FontAwesomeIcon :icon="['fas', 'refresh']" :class="{ 'animate-spin': statsLoading || loading }" />
        </button>
      </div>
    </div>

    <!-- 增量处理进度卡片 -->
    <div v-if="initStatus.initialized" class="rounded-2xl border border-black/5 bg-white/80 p-4 shrink-0">
      <div class="flex items-center justify-between mb-3">
        <h3 class="text-sm font-semibold text-ink-700">增量处理进度</h3>
        <span class="text-xs text-ink-400">已上传 {{ progress.pdf_uploaded }} / {{ progress.total_documents }}（{{ progress.progress_percentage }}%）</span>
      </div>
      <div class="h-3 overflow-hidden rounded-full bg-ink-100">
        <div
          class="h-full rounded-full bg-gradient-to-r from-accent-500 to-accent-600 transition-all duration-500"
          :style="{ width: `${progress.progress_percentage}%` }"
        ></div>
      </div>
      <div class="mt-3 grid grid-cols-2 gap-2 lg:grid-cols-4 xl:grid-cols-6 text-xs">
        <div class="flex items-center gap-1.5">
          <span class="h-2 w-2 rounded-full bg-yellow-400"></span>
          <span class="text-ink-500">待上传PDF：{{ progress.pending_pdf_upload }}</span>
        </div>
        <div class="flex items-center gap-1.5">
          <span class="h-2 w-2 rounded-full bg-green-400"></span>
          <span class="text-ink-500">已切块：{{ progress.chunked }}</span>
        </div>
        <div class="flex items-center gap-1.5">
          <span class="h-2 w-2 rounded-full bg-blue-400"></span>
          <span class="text-ink-500">已向量化：{{ progress.vectorized }}</span>
        </div>
        <div class="flex items-center gap-1.5">
          <span class="h-2 w-2 rounded-full bg-purple-400"></span>
          <span class="text-ink-500">待向量化：{{ progress.pending_vectorize }}</span>
        </div>
        <div class="flex items-center gap-1.5">
          <span class="h-2 w-2 rounded-full bg-red-400"></span>
          <span class="text-ink-500">切块失败：{{ progress.failed_chunk }}</span>
        </div>
        <div class="flex items-center gap-1.5">
          <span class="h-2 w-2 rounded-full bg-red-400"></span>
          <span class="text-ink-500">向量化失败：{{ progress.failed_vectorize }}</span>
        </div>
      </div>
    </div>

    <!-- 统计卡片区 -->
    <div class="grid grid-cols-2 gap-4 lg:grid-cols-3 xl:grid-cols-6 shrink-0">
      <MetricTile
        title="总文档数"
        :value="String(totalDocuments)"
        tone="neutral"
      />
      <MetricTile
        title="已切块"
        :value="String(chunkedDocuments)"
        tone="success"
      />
      <MetricTile
        title="已向量化"
        :value="String(vectorizedDocuments)"
        tone="success"
      />
      <MetricTile
        title="待处理"
        :value="String(pendingDocuments)"
        tone="warning"
      />
      <MetricTile
        title="失败"
        :value="String(failedDocuments)"
        tone="danger"
      />
      <MetricTile
        title="总切块数"
        :value="String(totalChunks)"
        tone="neutral"
      />
    </div>

    <!-- 操作按钮区 -->
    <div class="flex flex-wrap items-center gap-2 rounded-2xl border border-black/5 bg-white/80 p-3 shrink-0">
      <button
        class="flex items-center gap-2 rounded-xl bg-amber-600 px-4 py-2 text-sm text-white transition-colors hover:bg-amber-700 disabled:opacity-50"
        @click="handleSystemInit"
      >
        <FontAwesomeIcon :icon="['fas', 'gear']" />
        <span>系统初始化</span>
      </button>

      <button
        class="flex items-center gap-2 rounded-xl bg-ink-900 px-4 py-2 text-sm text-white transition-colors hover:bg-ink-700 disabled:opacity-50"
        :disabled="!initStatus.initialized"
        @click="handleUploadPdf"
      >
        <FontAwesomeIcon :icon="['fas', 'cloud-arrow-up']" />
        <span>上传研报PDF</span>
      </button>

      <div class="h-6 w-px bg-ink-200 mx-1"></div>

      <button
        class="flex items-center gap-2 rounded-xl bg-blue-600 px-4 py-2 text-sm text-white transition-colors hover:bg-blue-700 disabled:opacity-50"
        :disabled="chunking || selectedIds.length === 0"
        @click="handleBatchChunk"
      >
        <FontAwesomeIcon v-if="!chunking" :icon="['fas', 'scissors']" />
        <FontAwesomeIcon v-else :icon="['fas', 'spinner']" class="animate-spin" />
        <span>批量切块{{ selectedIds.length > 0 ? ` (${selectedIds.length})` : '' }}</span>
      </button>

      <button
        class="flex items-center gap-2 rounded-xl bg-purple-600 px-4 py-2 text-sm text-white transition-colors hover:bg-purple-700 disabled:opacity-50"
        :disabled="chunking"
        @click="handleChunkAll"
      >
        <FontAwesomeIcon :icon="['fas', 'layer-group']" />
        <span>一键切块</span>
      </button>

      <button
        class="flex items-center gap-2 rounded-xl bg-green-600 px-4 py-2 text-sm text-white transition-colors hover:bg-green-700 disabled:opacity-50"
        :disabled="vectorizing"
        @click="handleBatchVectorize"
      >
        <FontAwesomeIcon v-if="!vectorizing" :icon="['fas', 'bolt']" />
        <FontAwesomeIcon v-else :icon="['fas', 'spinner']" class="animate-spin" />
        <span>{{ vectorizing ? '向量化中...' : '批量向量化' }}</span>
      </button>

      <span class="ml-auto text-xs text-ink-400">
        共 {{ total }} 条记录
        <span v-if="hasProcessing" class="ml-2 text-blue-500">（有任务处理中，自动刷新中...）</span>
      </span>
    </div>

    <!-- 筛选区 -->
    <div class="flex flex-wrap items-center gap-3 rounded-xl border border-black/5 bg-slate-50 p-3 shrink-0">
      <span class="text-sm text-ink-500">筛选：</span>
      <select
        v-model="filterDocType"
        class="rounded-lg border border-ink-200 px-3 py-1.5 text-sm text-ink-600 outline-none focus:border-ink-400"
        @change="handleFilterChange"
      >
        <option value="">全部类型</option>
        <option value="RESEARCH_REPORT">个股研报</option>
        <option value="INDUSTRY_REPORT">行业研报</option>
      </select>

      <select
        v-model="filterChunkStatus"
        class="rounded-lg border border-ink-200 px-3 py-1.5 text-sm text-ink-600 outline-none focus:border-ink-400"
        @change="handleFilterChange"
      >
        <option value="">切块状态</option>
        <option :value="0">待处理</option>
        <option :value="1">处理中</option>
        <option :value="2">已完成</option>
        <option :value="3">失败</option>
      </select>

      <select
        v-model="filterVectorStatus"
        class="rounded-lg border border-ink-200 px-3 py-1.5 text-sm text-ink-600 outline-none focus:border-ink-400"
        @change="handleFilterChange"
      >
        <option value="">向量状态</option>
        <option :value="0">待处理</option>
        <option :value="1">处理中</option>
        <option :value="2">已完成</option>
        <option :value="3">失败</option>
      </select>

      <select
        v-model="filterMetadataStatus"
        class="rounded-lg border border-ink-200 px-3 py-1.5 text-sm text-ink-600 outline-none focus:border-ink-400"
        @change="handleFilterChange"
      >
        <option value="">元数据状态</option>
        <option :value="0">未加载</option>
        <option :value="1">待上传PDF</option>
        <option :value="2">已上传</option>
      </select>
    </div>

    <!-- 文档列表区 -->
    <div class="flex flex-col rounded-2xl border border-black/5 bg-white/80 overflow-hidden shrink-0" style="height: 500px;">
      <!-- 加载状态 -->
      <div v-if="loading" class="flex items-center justify-center py-12">
        <FontAwesomeIcon :icon="['fas', 'spinner']" class="animate-spin text-2xl text-ink-400" />
      </div>

      <!-- 表格 -->
      <div v-else class="overflow-x-auto overflow-y-auto h-full">
        <table class="w-full text-sm">
          <thead class="border-b border-black/5 bg-slate-50">
            <tr>
              <th class="px-4 py-3 text-left font-semibold text-ink-600 w-10">
                <input
                  type="checkbox"
                  :checked="selectedIds.length === documents.length && documents.length > 0"
                  class="rounded border-ink-300"
                  @change="toggleSelectAll"
                />
              </th>
              <th class="px-4 py-3 text-left font-semibold text-ink-600">ID</th>
              <th class="px-4 py-3 text-left font-semibold text-ink-600">标题</th>
              <th class="px-4 py-3 text-left font-semibold text-ink-600">文档类型</th>
              <th class="px-4 py-3 text-left font-semibold text-ink-600">股票代码</th>
              <th class="px-4 py-3 text-left font-semibold text-ink-600">股票简称</th>
              <th class="px-4 py-3 text-center font-semibold text-ink-600">切块数</th>
              <th class="px-4 py-3 text-left font-semibold text-ink-600">切块状态</th>
              <th class="px-4 py-3 text-left font-semibold text-ink-600">向量化状态</th>
              <th class="px-4 py-3 text-left font-semibold text-ink-600">元数据状态</th>
              <th class="px-4 py-3 text-left font-semibold text-ink-600">操作</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-black/5">
            <tr
              v-for="doc in documents"
              :key="doc.id"
              class="transition-colors hover:bg-slate-50"
              :class="{ 'bg-blue-50/50': selectedIds.includes(doc.id) }"
            >
              <td class="px-4 py-3">
                <input
                  type="checkbox"
                  :checked="selectedIds.includes(doc.id)"
                  class="rounded border-ink-300"
                  @change="toggleSelect(doc)"
                />
              </td>
              <td class="px-4 py-3 text-ink-700">{{ doc.id }}</td>
              <td class="px-4 py-3 text-ink-700 max-w-xs truncate" :title="doc.title">{{ doc.title || '-' }}</td>
              <td class="px-4 py-3">
                <span class="inline-flex items-center gap-1.5 rounded-full bg-blue-50 px-2.5 py-1 text-xs font-medium text-blue-700 border border-blue-100">
                  {{ getDocTypeLabel(doc.doc_type) }}
                </span>
              </td>
              <td class="px-4 py-3 text-ink-700">{{ doc.stock_code || '-' }}</td>
              <td class="px-4 py-3 text-ink-700 max-w-xs truncate" :title="doc.stock_abbr">{{ doc.stock_abbr || '-' }}</td>
              <td class="px-4 py-3 text-center text-ink-700">{{ doc.chunk_count }}</td>
              <td class="px-4 py-3">
                <StatusBadge
                  :label="getChunkStatusConfig(doc.chunk_status).label"
                  :tone="getChunkStatusConfig(doc.chunk_status).tone"
                />
              </td>
              <td class="px-4 py-3">
                <StatusBadge
                  :label="getVectorStatusConfig(doc.vector_status).label"
                  :tone="getVectorStatusConfig(doc.vector_status).tone"
                />
              </td>
              <td class="px-4 py-3">
                <StatusBadge
                  :label="getMetadataStatusConfig(doc.metadata_status).label"
                  :tone="getMetadataStatusConfig(doc.metadata_status).tone"
                />
              </td>
              <td class="px-4 py-3">
                <div class="flex items-center gap-1">
                  <button
                    v-if="doc.metadata_status === 1"
                    class="rounded-lg px-2 py-1 text-xs text-amber-600 hover:bg-amber-50"
                    @click="handleSingleUploadPdf(doc)"
                  >
                    上传PDF
                  </button>
                  <button
                    v-if="doc.chunk_status === 3 || doc.error_message"
                    class="rounded-lg px-2 py-1 text-xs text-red-600 hover:bg-red-50"
                    @click="handleRetryFailed(doc)"
                  >
                    重试
                  </button>
                  <button
                    v-if="doc.chunk_status !== 2 && doc.metadata_status === 2"
                    class="rounded-lg px-2 py-1 text-xs text-blue-600 hover:bg-blue-50 disabled:opacity-50"
                    :disabled="doc.chunk_status === 1"
                    @click="handleChunkDocument(doc)"
                  >
                    切块
                  </button>
                  <button
                    v-if="doc.vector_status === 1"
                    class="rounded-lg px-2 py-1 text-xs text-orange-600 hover:bg-orange-50"
                    @click="handleResetVectorStatus(doc)"
                  >
                    重置状态
                  </button>
                  <button
                    v-if="doc.chunk_status === 2 && doc.vector_status !== 2 && doc.vector_status !== 1"
                    class="rounded-lg px-2 py-1 text-xs text-green-600 hover:bg-green-50 disabled:opacity-50"
                    :disabled="doc.vector_status === 1"
                    @click="handleVectorizeDocument(doc)"
                  >
                    向量化
                  </button>
                  <button
                    v-if="doc.vector_status === 2"
                    class="rounded-lg px-2 py-1 text-xs text-purple-600 hover:bg-purple-50"
                    @click="handleVectorizeDocument(doc)"
                  >
                    重新向量化
                  </button>
                  <span v-if="doc.chunk_status === 2 && doc.vector_status === 2" class="text-xs text-ink-400">
                    已完成
                  </span>
                  <span
                    v-if="doc.error_message"
                    class="text-xs text-red-400 cursor-help"
                    :title="doc.error_message"
                  >
                    <FontAwesomeIcon :icon="['fas', 'circle-exclamation']" aria-hidden="true" />
                  </span>
                </div>
              </td>
            </tr>

            <!-- 空状态 -->
            <tr v-if="documents.length === 0">
              <td colspan="11" class="px-4 py-12 text-center text-sm text-ink-400">
                <div class="flex flex-col items-center gap-2">
                  <FontAwesomeIcon :icon="['fas', 'folder-open']" class="text-2xl" />
                  <span>暂无文档，请先上传文档</span>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- 分页 -->
      <div v-if="documents.length > 0" class="flex items-center justify-between border-t border-black/5 px-4 py-3">
        <div class="flex items-center gap-2 text-sm text-ink-600">
          <span>每页</span>
          <select
            v-model.number="pageSize"
            class="rounded-lg border border-ink-200 px-2 py-1 text-xs text-ink-600 outline-none focus:border-ink-400"
            @change="handleSizeChange(pageSize)"
          >
            <option :value="10">10</option>
            <option :value="20">20</option>
            <option :value="50">50</option>
            <option :value="100">100</option>
          </select>
          <span>条</span>
        </div>

        <div class="flex items-center gap-1">
          <button
            class="flex h-8 min-w-[2rem] items-center justify-center rounded-lg border border-ink-200 px-2 text-xs text-ink-600 transition-colors hover:bg-ink-50 disabled:opacity-50"
            :disabled="currentPage === 1"
            @click="handlePageChange(currentPage - 1)"
          >
            <FontAwesomeIcon :icon="['fas', 'chevron-left']" />
          </button>

          <button
            v-for="page in totalPages"
            :key="page"
            class="flex h-8 min-w-[2rem] items-center justify-center rounded-lg border px-2 text-xs transition-colors"
            :class="{
              'border-ink-900 bg-ink-900 text-white': page === currentPage,
              'border-ink-200 text-ink-600 hover:bg-ink-50': page !== currentPage
            }"
            @click="handlePageChange(page)"
          >
            {{ page }}
          </button>

          <button
            class="flex h-8 min-w-[2rem] items-center justify-center rounded-lg border border-ink-200 px-2 text-xs text-ink-600 transition-colors hover:bg-ink-50 disabled:opacity-50"
            :disabled="currentPage === totalPages"
            @click="handlePageChange(currentPage + 1)"
          >
            <FontAwesomeIcon :icon="['fas', 'chevron-right']" />
          </button>
        </div>
      </div>
    </div>

    <!-- 系统初始化弹窗 -->
    <SystemInitModal
      :visible="showSystemInitModal"
      @close="showSystemInitModal = false"
      @success="handleSystemInitSuccess"
    />

    <!-- 增量上传PDF弹窗 -->
    <UploadPdfModal
      :visible="showUploadPdfModal"
      @close="showUploadPdfModal = false"
      @success="handleUploadPdfSuccess"
    />
  </div>
</template>

<style scoped>
.animate-spin {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}
</style>
