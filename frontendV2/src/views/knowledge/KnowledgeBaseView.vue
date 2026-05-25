<script setup>
/**
 * 知识库管理页面
 * 功能描述：系统初始化（研报元数据导入）+ 状态查询 + 统计信息仪表盘
 * 依赖组件：MetricTile, StatusBadge, BaseButton, BaseSelect, SystemInitModal, UploadPdfModal
 */
import { ref, reactive, computed, onMounted } from 'vue'
import MetricTile from '@/components/common/MetricTile.vue'
import StatusBadge from '@/components/common/StatusBadge.vue'
import BaseButton from '@/components/ui/BaseButton.vue'
import BaseInput from '@/components/ui/BaseInput.vue'
import BaseSelect from '@/components/ui/BaseSelect.vue'
import SystemInitModal from '@/components/common/SystemInitModal.vue'
import UploadPdfModal from '@/components/common/UploadPdfModal.vue'
import { initKnowledgeBase, getInitStatus, getKnowledgeBaseStats, getKnowledgeDocumentList } from '@/api/knowledgeBase'

// ── 常量 ──

const DOC_TYPE_OPTIONS = [
  { value: 'RESEARCH_REPORT', label: '个股研报' },
  { value: 'INDUSTRY_REPORT', label: '行业研报' }
]

const DOC_TYPE_LABEL_MAP = {
  RESEARCH_REPORT: '个股研报',
  INDUSTRY_REPORT: '行业研报'
}

// 状态码映射（与 V1 保持一致）
const CHUNK_STATUS_MAP = {
  0: { label: '待处理', tone: 'neutral' },
  1: { label: '处理中', tone: 'warning' },
  2: { label: '已完成', tone: 'success' },
  3: { label: '失败', tone: 'danger' }
}

const VECTOR_STATUS_MAP = {
  0: { label: '待处理', tone: 'neutral' },
  1: { label: '处理中', tone: 'warning' },
  2: { label: '已完成', tone: 'success' },
  3: { label: '失败', tone: 'danger' },
  4: { label: '跳过', tone: 'warning' }
}

// ── 统计数据 ──

const stats = ref({
  documents: { total: 0, by_chunk_status: {}, by_vector_status: {}, by_doc_type: {} },
  chunks: { total: 0, by_vector_status: {} }
})

const statsLoading = ref(false)

// ── 初始化状态 ──

const initStatus = ref({ initialized: false, stock_metadata_count: 0, industry_metadata_count: 0, total_metadata_count: 0 })
const isStatusLoading = ref(false)
const isRefreshing = ref(false)

// ── 导入操作 ──

const fileInput = ref(null)
const selectedDocType = ref('RESEARCH_REPORT')
const isUploading = ref(false)
const uploadMessage = ref({ type: '', text: '' })
const importResult = ref(null)

// ── 弹窗 ──

const showSystemInitModal = ref(false)
const showUploadPdfModal = ref(false)

// ── 文档列表 ──

const listState = reactive({
  items: [],
  page: 1,
  pageSize: 10,
  total: 0
})

const listKeyword = ref('')
const listDocTypeFilter = ref('')
const listChunkStatusFilter = ref('')
const listVectorStatusFilter = ref('')
const isLoadingList = ref(false)
const isRefreshingList = ref(false)
const listErrorMessage = ref('')

const docTypeFilterOptions = [
  { value: '', label: '全部类型' },
  { value: 'RESEARCH_REPORT', label: '个股研报' },
  { value: 'INDUSTRY_REPORT', label: '行业研报' }
]

const chunkStatusFilterOptions = [
  { value: '', label: '全部切块状态' },
  { value: '0', label: '待切块' },
  { value: '1', label: '切块中' },
  { value: '2', label: '已完成' },
  { value: '3', label: '失败' }
]

const vectorStatusFilterOptions = [
  { value: '', label: '全部向量状态' },
  { value: '0', label: '未向量化' },
  { value: '1', label: '向量化中' },
  { value: '2', label: '已向量化' },
  { value: '3', label: '失败' }
]

// ── 计算属性 ──

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

const isInitialized = computed(() => initStatus.value.initialized ?? false)
const totalMetadataCount = computed(() => initStatus.value.total_metadata_count ?? 0)

const noticeClass = computed(() => {
  if (!uploadMessage.value.text) return ''
  return uploadMessage.value.type === 'success'
    ? 'border-green-200 bg-green-50 text-green-700'
    : 'border-red-200 bg-red-50 text-red-700'
})

const hasListItems = computed(() => listState.items.length > 0)
const listTotalPages = computed(() => {
  const pageSize = Number(listState.pageSize) || 10
  const total = Number(listState.total) || listState.items.length
  return Math.max(1, Math.ceil(total / pageSize))
})

// ── 数据加载 ──

const loadInitStatus = async ({ silent = false } = {}) => {
  if (silent) isRefreshing.value = true
  else isStatusLoading.value = true
  try {
    const res = await getInitStatus()
    initStatus.value = res.data || res
  } catch {
    initStatus.value = { initialized: false, stock_metadata_count: 0, industry_metadata_count: 0, total_metadata_count: 0 }
  } finally {
    isStatusLoading.value = false
    isRefreshing.value = false
  }
}

const loadStats = async () => {
  statsLoading.value = true
  try {
    const res = await getKnowledgeBaseStats()
    stats.value = res.data || res
  } catch {
    showToast('加载统计数据失败', 'error')
  } finally {
    statsLoading.value = false
  }
}

// ── 事件处理 ──

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

const triggerFileInput = () => fileInput.value?.click()

const handleFileChange = async (event) => {
  const file = event.target.files?.[0]
  if (!file) return

  if (!file.name.toLowerCase().endsWith('.xlsx') && !file.name.toLowerCase().endsWith('.xls')) {
    uploadMessage.value = { type: 'error', text: '仅支持 Excel 文件（.xlsx 或 .xls）' }
    return
  }

  isUploading.value = true
  uploadMessage.value = { type: '', text: '' }
  importResult.value = null

  try {
    const result = await initKnowledgeBase(file, selectedDocType.value)
    importResult.value = result
    const msg = result.data?.message || result.message || '导入成功'
    uploadMessage.value = { type: 'success', text: msg }
    event.target.value = ''
    await loadInitStatus()
    await loadStats()
  } catch (error) {
    uploadMessage.value = { type: 'error', text: error.message || '导入失败' }
  } finally {
    isUploading.value = false
  }
}

function handleSystemInit() {
  showSystemInitModal.value = true
}

async function handleSystemInitSuccess() {
  showToast('系统初始化成功', 'success')
  await loadInitStatus()
  await loadStats()
}

function handleUploadPdf() {
  showUploadPdfModal.value = true
}

async function handleUploadPdfSuccess() {
  showToast('PDF上传处理完成', 'success')
  await loadInitStatus()
  await loadStats()
}

// ── 文档列表加载 ──

const formatDate = (value) => {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).format(date)
}

const fetchDocumentList = async ({ silent = false } = {}) => {
  if (silent) {
    isRefreshingList.value = true
  } else {
    isLoadingList.value = true
    listErrorMessage.value = ''
  }

  try {
    const params = {
      page: listState.page,
      page_size: listState.pageSize
    }
    if (listKeyword.value.trim()) params.keyword = listKeyword.value.trim()
    if (listDocTypeFilter.value) params.doc_type = listDocTypeFilter.value
    if (listChunkStatusFilter.value !== '') params.chunk_status = Number(listChunkStatusFilter.value)
    if (listVectorStatusFilter.value !== '') params.vector_status = Number(listVectorStatusFilter.value)

    const response = await getKnowledgeDocumentList(params)
    const payload = response?.data || response
    listState.items = payload?.lists || []
    listState.total = payload?.pagination?.total || 0
    listErrorMessage.value = ''
  } catch (error) {
    listErrorMessage.value = error.message || '加载文档列表失败'
    listState.items = []
  } finally {
    isLoadingList.value = false
    isRefreshingList.value = false
  }
}

const handleListSearch = () => {
  listState.page = 1
  fetchDocumentList()
}

const handleListReset = () => {
  listKeyword.value = ''
  listDocTypeFilter.value = ''
  listChunkStatusFilter.value = ''
  listVectorStatusFilter.value = ''
  listState.page = 1
  fetchDocumentList()
}

const handleListPageChange = (page) => {
  if (page < 1 || page > listTotalPages.value) return
  listState.page = page
  fetchDocumentList()
}

const handleListKeydown = (event) => {
  if (event.key === 'Enter') handleListSearch()
}

// ── 初始化 ──

onMounted(() => {
  loadInitStatus()
  loadStats()
  fetchDocumentList()
})
</script>

<template>
  <div class="flex flex-col gap-4 p-4 h-full overflow-y-auto">
    <!-- ═══ 页面标题 ═══ -->
    <div class="flex items-center justify-between rounded-2xl border border-black/5 bg-white/80 p-4 shrink-0">
      <div>
        <h2 class="text-lg font-semibold text-ink-900">知识库管理</h2>
        <p class="mt-1 text-sm text-ink-500">增量构建模式：初始化 → 上传PDF → 向量化</p>
      </div>
      <div class="flex items-center gap-2">
        <span
          v-if="isInitialized"
          class="inline-flex items-center gap-1.5 rounded-full bg-green-50 px-3 py-1 text-xs font-medium text-green-700 border border-green-200"
        >
          <FontAwesomeIcon :icon="['fas', 'check-circle']" aria-hidden="true" />
          已初始化（{{ totalMetadataCount }} 条元数据）
        </span>
        <span
          v-else-if="initStatus"
          class="inline-flex items-center gap-1.5 rounded-full bg-yellow-50 px-3 py-1 text-xs font-medium text-yellow-700 border border-yellow-200"
        >
          <FontAwesomeIcon :icon="['fas', 'exclamation-circle']" aria-hidden="true" />
          未初始化
        </span>
        <span
          v-else
          class="inline-flex items-center gap-1.5 rounded-full bg-ink-50 px-3 py-1 text-xs font-medium text-ink-500 border border-black/5"
        >
          <FontAwesomeIcon :icon="['fas', 'spinner']" spin aria-hidden="true" />
          查询中
        </span>
        <BaseButton
          variant="secondary"
          icon="rotate-right"
          size="sm"
          icon-only
          :loading="isRefreshing"
          aria-label="刷新"
          @click="loadInitStatus({ silent: true })"
        />
      </div>
    </div>

    <!-- ═══ 统计卡片区 ═══ -->
    <div class="grid grid-cols-2 gap-4 lg:grid-cols-3 xl:grid-cols-6 shrink-0">
      <MetricTile title="总文档数" :value="String(totalDocuments)" tone="neutral" />
      <MetricTile title="已切块" :value="String(chunkedDocuments)" tone="success" />
      <MetricTile title="已向量化" :value="String(vectorizedDocuments)" tone="success" />
      <MetricTile title="待处理" :value="String(pendingDocuments)" tone="warning" />
      <MetricTile title="失败" :value="String(failedDocuments)" tone="danger" />
      <MetricTile title="总切块数" :value="String(totalChunks)" tone="neutral" />
    </div>

    <!-- ═══ 操作按钮区 ═══ -->
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
        :disabled="!isInitialized"
        @click="handleUploadPdf"
      >
        <FontAwesomeIcon :icon="['fas', 'cloud-arrow-up']" />
        <span>上传研报PDF</span>
      </button>

      <div class="h-6 w-px bg-ink-200 mx-1"></div>

      <!-- 快捷导入区 -->
      <div class="flex items-center gap-2">
        <BaseSelect v-model="selectedDocType" :options="DOC_TYPE_OPTIONS" size="sm" />
        <BaseButton icon="upload" :loading="isUploading" @click="triggerFileInput">
          {{ isUploading ? '导入中...' : '选择Excel文件' }}
        </BaseButton>
        <input
          ref="fileInput"
          type="file"
          accept=".xlsx,.xls"
          class="hidden"
          @change="handleFileChange"
        />
      </div>

      <span class="ml-auto text-xs text-ink-400">
        共 {{ totalMetadataCount }} 条元数据
      </span>
    </div>

    <!-- ═══ 导入结果通知 ═══ -->
    <div
      v-if="uploadMessage.text"
      class="shrink-0 rounded-2xl border px-4 py-3 text-sm"
      :class="noticeClass"
    >
      <p class="font-medium">{{ uploadMessage.text }}</p>
      <p v-if="importResult?.data?.total_count" class="mt-1 text-xs opacity-80">
        总计导入：{{ importResult.data.total_count }} 条
      </p>
    </div>

    <!-- ═══ 统计信息仪表盘 ═══ -->
    <div class="flex flex-col gap-3">
      <!-- 加载中 -->
      <div
        v-if="statsLoading"
        class="flex items-center justify-center rounded-2xl border border-black/5 bg-white py-10"
      >
        <div class="flex items-center gap-2.5">
          <FontAwesomeIcon :icon="['fas', 'spinner']" spin class="text-base text-ink-400" aria-hidden="true" />
          <span class="text-sm text-ink-400">正在加载统计信息...</span>
        </div>
      </div>

      <!-- 无数据 -->
      <div
        v-else-if="!stats"
        class="flex items-center justify-center rounded-2xl border border-black/5 bg-white py-10"
      >
        <FontAwesomeIcon :icon="['fas', 'chart-pie']" class="mr-2.5 text-base text-ink-300" aria-hidden="true" />
        <span class="text-sm text-ink-400">暂无统计数据，请先完成系统初始化后再查看</span>
        <BaseButton variant="secondary" size="sm" class="ml-4" @click="loadStats">重新加载</BaseButton>
      </div>

      <template v-else>
        <!-- KPI 指标栏 -->
        <div class="grid grid-cols-2 gap-2 sm:grid-cols-4">
          <div class="flex items-center gap-3 rounded-xl border border-black/5 bg-white px-4 py-3">
            <FontAwesomeIcon :icon="['fas', 'file-lines']" class="text-sm text-ink-300" aria-hidden="true" />
            <div>
              <p class="text-lg font-semibold tabular-nums text-ink-900">{{ stats.documents?.total ?? 0 }}</p>
              <p class="text-xs text-ink-400">文档总数</p>
            </div>
          </div>
          <div class="flex items-center gap-3 rounded-xl border border-black/5 bg-white px-4 py-3">
            <FontAwesomeIcon :icon="['fas', 'scissors']" class="text-sm text-ink-300" aria-hidden="true" />
            <div>
              <p class="text-lg font-semibold tabular-nums text-ink-900">{{ chunkedDocuments }}</p>
              <p class="text-xs text-ink-400">已切块</p>
            </div>
          </div>
          <div class="flex items-center gap-3 rounded-xl border border-black/5 bg-white px-4 py-3">
            <FontAwesomeIcon :icon="['fas', 'bolt']" class="text-sm text-ink-300" aria-hidden="true" />
            <div>
              <p class="text-lg font-semibold tabular-nums text-ink-900">{{ vectorizedDocuments }}</p>
              <p class="text-xs text-ink-400">已向量化</p>
            </div>
          </div>
          <div class="flex items-center gap-3 rounded-xl border border-black/5 bg-white px-4 py-3">
            <FontAwesomeIcon :icon="['fas', 'cubes']" class="text-sm text-ink-300" aria-hidden="true" />
            <div>
              <p class="text-lg font-semibold tabular-nums text-ink-900">{{ totalChunks }}</p>
              <p class="text-xs text-ink-400">切块总数</p>
            </div>
          </div>
        </div>

        <!-- 文档分布 -->
        <section class="rounded-xl border border-black/5 bg-white">
          <div class="flex items-center gap-2 border-b border-black/5 px-4 py-2.5">
            <FontAwesomeIcon :icon="['fas', 'file-lines']" class="text-xs text-ink-300" aria-hidden="true" />
            <h3 class="text-sm font-medium text-ink-700">文档分布</h3>
            <span class="ml-auto text-xs text-ink-400 tabular-nums">{{ stats.documents?.total ?? 0 }} 份</span>
          </div>
          <div class="grid grid-cols-1 gap-0 divide-y divide-black/5 sm:grid-cols-3 sm:divide-x sm:divide-y-0">
            <!-- 文档类型 -->
            <div class="px-4 py-3">
              <p class="text-xs text-ink-400 mb-2">文档类型</p>
              <div v-if="Object.keys(stats.documents?.by_doc_type || {}).length" class="space-y-1.5">
                <div
                  v-for="(count, key) in stats.documents.by_doc_type"
                  :key="key"
                  class="flex items-center justify-between"
                >
                  <span class="text-sm text-ink-600">{{ DOC_TYPE_LABEL_MAP[key] || key }}</span>
                  <span class="text-sm font-medium tabular-nums text-ink-800">{{ count }}</span>
                </div>
              </div>
              <p v-else class="text-xs text-ink-300">-</p>
            </div>
            <!-- 切块状态 -->
            <div class="px-4 py-3">
              <p class="text-xs text-ink-400 mb-2">切块状态</p>
              <div v-if="Object.keys(stats.documents?.by_chunk_status || {}).length" class="space-y-1.5">
                <div
                  v-for="(count, key) in stats.documents.by_chunk_status"
                  :key="key"
                  class="flex items-center justify-between"
                >
                  <StatusBadge :label="CHUNK_STATUS_MAP[key]?.label || `状态${key}`" :tone="CHUNK_STATUS_MAP[key]?.tone || 'neutral'" />
                  <span class="text-sm font-medium tabular-nums text-ink-800">{{ count }}</span>
                </div>
              </div>
              <p v-else class="text-xs text-ink-300">-</p>
            </div>
            <!-- 向量状态 -->
            <div class="px-4 py-3">
              <p class="text-xs text-ink-400 mb-2">向量状态</p>
              <div v-if="Object.keys(stats.documents?.by_vector_status || {}).length" class="space-y-1.5">
                <div
                  v-for="(count, key) in stats.documents.by_vector_status"
                  :key="key"
                  class="flex items-center justify-between"
                >
                  <StatusBadge :label="VECTOR_STATUS_MAP[key]?.label || `状态${key}`" :tone="VECTOR_STATUS_MAP[key]?.tone || 'neutral'" />
                  <span class="text-sm font-medium tabular-nums text-ink-800">{{ count }}</span>
                </div>
              </div>
              <p v-else class="text-xs text-ink-300">-</p>
            </div>
          </div>
        </section>

        <!-- 向量化进度 -->
        <section class="rounded-xl border border-black/5 bg-white">
          <div class="flex items-center gap-2 border-b border-black/5 px-4 py-2.5">
            <FontAwesomeIcon :icon="['fas', 'bolt']" class="text-xs text-ink-300" aria-hidden="true" />
            <h3 class="text-sm font-medium text-ink-700">切块向量化进度</h3>
            <span class="ml-auto text-xs text-ink-400 tabular-nums">{{ totalChunks }} 块</span>
          </div>
          <div class="px-4 py-3">
            <div v-if="Object.keys(stats.chunks?.by_vector_status || {}).length" class="space-y-3">
              <div
                v-for="(count, key) in stats.chunks.by_vector_status"
                :key="key"
              >
                <div class="flex items-center justify-between mb-1">
                  <StatusBadge :label="VECTOR_STATUS_MAP[key]?.label || `状态${key}`" :tone="VECTOR_STATUS_MAP[key]?.tone || 'neutral'" />
                  <span class="text-xs tabular-nums text-ink-400">
                    {{ count }}（{{ totalChunks ? (count / totalChunks * 100).toFixed(1) : 0 }}%）
                  </span>
                </div>
                <div class="h-1.5 w-full overflow-hidden rounded-full bg-ink-100">
                  <div
                    class="h-full rounded-full transition-all"
                    :style="{ width: totalChunks ? (count / totalChunks * 100).toFixed(1) + '%' : '0%' }"
                  ></div>
                </div>
              </div>
            </div>
            <p v-else class="py-4 text-center text-sm text-ink-300">暂无切块数据</p>
          </div>
        </section>

        <!-- 底部操作 -->
        <div class="flex items-center justify-end gap-3">
          <span class="text-xs text-ink-400">上次更新：刚刚</span>
          <BaseButton
            variant="secondary"
            icon="rotate-right"
            size="sm"
            :loading="statsLoading"
            @click="loadStats"
          >
            刷新统计
          </BaseButton>
        </div>
      </template>
    </div>

    <!-- ═══ 文档列表 ═══ -->
    <div class="flex flex-col gap-3">
      <div class="flex items-center gap-2">
        <FontAwesomeIcon :icon="['fas', 'list']" class="text-xs text-ink-300" aria-hidden="true" />
        <h3 class="text-sm font-medium text-ink-700">文档列表</h3>
        <span class="text-xs text-ink-400 tabular-nums">共 {{ listState.total }} 条</span>
        <div class="ml-auto">
          <BaseButton
            variant="secondary"
            icon="rotate-right"
            size="sm"
            :loading="isRefreshingList"
            @click="fetchDocumentList({ silent: true })"
          >
            刷新
          </BaseButton>
        </div>
      </div>

      <!-- 筛选栏 -->
      <div class="flex flex-wrap items-center gap-2 rounded-2xl border border-black/5 bg-white/80 p-3">
        <BaseInput
          v-model="listKeyword"
          placeholder="搜索标题关键词..."
          size="sm"
          class="w-48"
          @keydown="handleListKeydown"
        />
        <BaseSelect v-model="listDocTypeFilter" :options="docTypeFilterOptions" size="sm" />
        <BaseSelect v-model="listChunkStatusFilter" :options="chunkStatusFilterOptions" size="sm" />
        <BaseSelect v-model="listVectorStatusFilter" :options="vectorStatusFilterOptions" size="sm" />
        <BaseButton variant="primary" icon="search" size="sm" @click="handleListSearch">搜索</BaseButton>
        <BaseButton variant="secondary" icon="rotate-right" size="sm" @click="handleListReset">重置</BaseButton>
      </div>

      <!-- 加载中 -->
      <div
        v-if="isLoadingList"
        class="flex items-center justify-center rounded-2xl border border-black/5 bg-white py-10"
      >
        <div class="flex items-center gap-2.5">
          <FontAwesomeIcon :icon="['fas', 'spinner']" spin class="text-base text-ink-400" aria-hidden="true" />
          <span class="text-sm text-ink-400">正在加载文档列表...</span>
        </div>
      </div>

      <!-- 加载失败 -->
      <div
        v-else-if="listErrorMessage"
        class="flex items-center justify-center rounded-2xl border border-black/5 bg-white py-10"
      >
        <div class="text-center">
          <FontAwesomeIcon :icon="['fas', 'triangle-exclamation']" class="mb-2 text-base text-danger" aria-hidden="true" />
          <p class="text-sm text-ink-500">{{ listErrorMessage }}</p>
          <BaseButton variant="secondary" size="sm" class="mt-3" @click="fetchDocumentList()">重新加载</BaseButton>
        </div>
      </div>

      <!-- 空数据 -->
      <div
        v-else-if="!hasListItems"
        class="flex items-center justify-center rounded-2xl border border-black/5 bg-white py-10"
      >
        <FontAwesomeIcon :icon="['fas', 'inbox']" class="mr-2.5 text-base text-ink-300" aria-hidden="true" />
        <span class="text-sm text-ink-400">暂无文档数据</span>
      </div>

      <!-- 文档表格 -->
      <template v-else>
        <div class="overflow-x-auto rounded-2xl border border-black/5 bg-white">
          <table class="w-full text-sm">
            <thead>
              <tr class="border-b border-black/5 bg-ink-50 text-left text-xs font-medium text-ink-500 uppercase tracking-wider">
                <th class="px-4 py-3 w-28">标题</th>
                <th class="px-4 py-3 w-24">文档类型</th>
                <th class="px-4 py-3 w-28">股票代码</th>
                <th class="px-4 py-3 w-40">股票简称</th>
                <th class="px-4 py-3 w-28">发布日期</th>
                <th class="px-4 py-3 w-24">切块状态</th>
                <th class="px-4 py-3 w-24">向量状态</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-black/5">
              <tr
                v-for="item in listState.items"
                :key="item.id"
                class="transition-colors hover:bg-ink-50/50"
              >
                <td class="px-4 py-3">
                  <p class="max-w-xs truncate font-medium text-ink-800" :title="item.title">
                    {{ item.title || '-' }}
                  </p>
                </td>
                <td class="px-4 py-3">
                  <span class="text-ink-600">{{ DOC_TYPE_LABEL_MAP[item.doc_type] || item.doc_type || '-' }}</span>
                </td>
                <td class="px-4 py-3 tabular-nums text-ink-600">{{ item.stock_code || '-' }}</td>
                <td class="px-4 py-3 text-ink-600">{{ item.stock_abbr || '-' }}</td>
                <td class="px-4 py-3 text-ink-500">{{ formatDate(item.publish_date) }}</td>
                <td class="px-4 py-3">
                  <StatusBadge
                    :label="CHUNK_STATUS_MAP[item.chunk_status]?.label || `状态${item.chunk_status}`"
                    :tone="CHUNK_STATUS_MAP[item.chunk_status]?.tone || 'neutral'"
                  />
                </td>
                <td class="px-4 py-3">
                  <StatusBadge
                    :label="VECTOR_STATUS_MAP[item.vector_status]?.label || `状态${item.vector_status}`"
                    :tone="VECTOR_STATUS_MAP[item.vector_status]?.tone || 'neutral'"
                  />
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- 分页 -->
        <div class="flex items-center justify-between rounded-2xl border border-black/5 bg-white/80 px-4 py-3">
          <span class="text-xs text-ink-400">
            第 {{ listState.page }}/{{ listTotalPages }} 页，共 {{ listState.total }} 条
          </span>
          <div class="flex items-center gap-1.5">
            <BaseButton
              variant="secondary"
              size="sm"
              icon-only
              icon="angles-left"
              :disabled="listState.page <= 1"
              @click="handleListPageChange(1)"
            />
            <BaseButton
              variant="secondary"
              size="sm"
              icon-only
              icon="angle-left"
              :disabled="listState.page <= 1"
              @click="handleListPageChange(listState.page - 1)"
            />
            <span class="px-2 text-xs tabular-nums text-ink-600">{{ listState.page }}</span>
            <BaseButton
              variant="secondary"
              size="sm"
              icon-only
              icon="angle-right"
              :disabled="listState.page >= listTotalPages"
              @click="handleListPageChange(listState.page + 1)"
            />
            <BaseButton
              variant="secondary"
              size="sm"
              icon-only
              icon="angles-right"
              :disabled="listState.page >= listTotalPages"
              @click="handleListPageChange(listTotalPages)"
            />
          </div>
        </div>
      </template>
    </div>

    <!-- ═══ 系统初始化弹窗 ═══ -->
    <SystemInitModal
      :visible="showSystemInitModal"
      @close="showSystemInitModal = false"
      @success="handleSystemInitSuccess"
    />

    <!-- ═══ 增量上传PDF弹窗 ═══ -->
    <UploadPdfModal
      :visible="showUploadPdfModal"
      @close="showUploadPdfModal = false"
      @success="handleUploadPdfSuccess"
    />
  </div>
</template>

<style scoped>
.hidden {
  display: none;
}
</style>