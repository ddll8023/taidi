<script setup>
/**
 * 知识库管理页面
 * 功能描述：系统初始化（研报元数据导入）+ 状态查询 + 统计信息仪表盘 + 文档列表 + 向量化 + 语义检索
 * 依赖组件：MetricTile, StatusBadge, BaseButton, BaseSelect, BaseInput, SystemInitModal, UploadPdfModal, SurfacePanel, AppEmptyState, MarkdownCleanerModal
 */
import { ref, reactive, computed, onMounted } from 'vue'
import MetricTile from '@/components/common/MetricTile.vue'
import StatusBadge from '@/components/common/StatusBadge.vue'
import BaseButton from '@/components/ui/BaseButton.vue'
import BaseInput from '@/components/ui/BaseInput.vue'
import BaseSelect from '@/components/ui/BaseSelect.vue'
import SystemInitModal from '@/components/common/SystemInitModal.vue'
import UploadPdfModal from '@/components/common/UploadPdfModal.vue'
import PaginationBar from '@/components/common/PaginationBar.vue'
import SurfacePanel from '@/components/ui/SurfacePanel.vue'
import AppEmptyState from '@/components/ui/AppEmptyState.vue'
import MarkdownCleanerModal from '@/components/knowledge/MarkdownCleanerModal.vue'
import { initKnowledgeBase, getInitStatus, getKnowledgeBaseStats, getKnowledgeDocumentList, uploadKnowledgeDocuments, parseDocuments, chunkDocuments, vectorizeDocuments, toggleCleanStatus, searchKnowledge } from '@/api/knowledgeBase'

// ── 常量 ──

const DOC_TYPE_OPTIONS = [
  { value: 'RESEARCH_REPORT', label: '个股研报' },
  { value: 'INDUSTRY_REPORT', label: '行业研报' }
]

const DOC_TYPE_LABEL_MAP = {
  RESEARCH_REPORT: '个股研报',
  INDUSTRY_REPORT: '行业研报'
}

const METADATA_STATUS_MAP = {
  0: { label: '未加载', tone: 'neutral' },
  1: { label: '待上传', tone: 'warning' },
  2: { label: '已上传', tone: 'success' }
}

const PARSE_STATUS_MAP = {
  0: { label: '未解析', tone: 'neutral' },
  1: { label: '解析中', tone: 'warning' },
  2: { label: '解析完成', tone: 'success' },
  3: { label: '解析失败', tone: 'danger' }
}

const CLEAN_STATUS_MAP = {
  0: { label: '未清洗', tone: 'neutral' },
  1: { label: '已清洗', tone: 'success' }
}

const CHUNK_STATUS_MAP = {
  0: { label: '待切块', tone: 'neutral' },
  1: { label: '切块中', tone: 'warning' },
  2: { label: '已完成', tone: 'success' },
  3: { label: '失败', tone: 'danger' }
}

const VECTOR_STATUS_MAP = {
  0: { label: '未向量化', tone: 'neutral' },
  1: { label: '向量化中', tone: 'warning' },
  2: { label: '已向量化', tone: 'success' },
  3: { label: '失败', tone: 'danger' }
}

// ── 统计数据 ──

const stats = ref({
  documents: { total: 0, by_vector_status: {}, by_doc_type: {} }
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
const showCleanerModal = ref(false)
const cleaningDocument = ref(null)

// ── 文档列表 ──

const listState = reactive({
  items: [],
  page: 1,
  pageSize: 10,
  total: 0
})

const listKeyword = ref('')
const listDocTypeFilter = ref('')
const listParseStatusFilter = ref('')
const listCleanStatusFilter = ref('')
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

const parseStatusFilterOptions = [
  { value: '', label: '全部解析状态' },
  { value: '0', label: '未解析' },
  { value: '1', label: '解析中' },
  { value: '2', label: '解析完成' },
  { value: '3', label: '解析失败' }
]

const cleanStatusFilterOptions = [
  { value: '', label: '全部清洗状态' },
  { value: '0', label: '未清洗' },
  { value: '1', label: '已清洗' }
]

const chunkStatusFilterOptions = [
  { value: '', label: '全部切块状态' },
  { value: '0', label: '待切块' },
  { value: '1', label: '切块中' },
  { value: '2', label: '已完成' },
  { value: '3', label: '失败' }
]

const vectorStatusFilterOptions = [
  { value: '', label: '全部向量化状态' },
  { value: '0', label: '未向量化' },
  { value: '1', label: '向量化中' },
  { value: '2', label: '已向量化' },
  { value: '3', label: '失败' }
]

// ── 向量化操作 ──

const isVectorizing = ref(false)
const isVectorizingAll = ref(false)
const vectorizeMessage = ref({ type: '', text: '' })

const handleSelectedVectorize = async () => {
  if (selectedIds.value.size === 0) return

  isVectorizing.value = true
  vectorizeMessage.value = { type: '', text: '' }

  try {
    const res = await vectorizeDocuments([...selectedIds.value])
    const payload = res.data || res
    const successCount = payload.success_count ?? 0
    const failedCount = payload.failed_count ?? 0
    vectorizeMessage.value = {
      type: failedCount === 0 ? 'success' : 'warning',
      text: `向量化完成：成功 ${successCount} 个，失败 ${failedCount} 个`
    }
    selectedIds.value = new Set()
    await loadStats()
    await fetchDocumentList()
  } catch (error) {
    vectorizeMessage.value = { type: 'error', text: error.message || '向量化操作失败' }
  } finally {
    isVectorizing.value = false
  }
}

const handleVectorizeAll = async () => {
  isVectorizingAll.value = true
  vectorizeMessage.value = { type: '', text: '' }
  try {
    const ids = listState.items.map(item => item.id)
    const res = await vectorizeDocuments(ids)
    const payload = res.data || res
    const successCount = payload.success_count ?? 0
    const failedCount = payload.failed_count ?? 0
    vectorizeMessage.value = {
      type: failedCount === 0 ? 'success' : 'warning',
      text: `当前页全部向量化完成：成功 ${successCount} 个，失败 ${failedCount} 个`
    }
    selectedIds.value = new Set()
    await loadStats()
    await fetchDocumentList()
  } catch (error) {
    vectorizeMessage.value = { type: 'error', text: error.message || '批量向量化失败' }
  } finally {
    isVectorizingAll.value = false
  }
}

const handleRowVectorize = async (doc) => {
  rowLoading.value = { ...rowLoading.value, [doc.id]: 'vectorize' }
  try {
    const res = await vectorizeDocuments([doc.id])
    const payload = res.data || res
    const ok = payload.success_count > 0
    showToast(
      ok ? `「${doc.title}」向量化完成` : '向量化失败',
      ok ? 'success' : 'error'
    )
    await loadStats()
    await fetchDocumentList()
  } catch (e) {
    showToast(e.message || '向量化失败', 'error')
  } finally {
    rowLoading.value = { ...rowLoading.value, [doc.id]: null }
  }
}

// ── 语义检索 ──

const searchQuery = ref('')
const searchStockCodes = ref('')
const searchIndustryNames = ref('')
const searchResults = ref([])
const searchTotal = ref(0)
const isSearching = ref(false)
const searchMessage = ref('')

const handleSearch = async () => {
  const query = searchQuery.value.trim()
  if (!query) {
    showToast('请输入检索查询语句', 'warning')
    return
  }

  isSearching.value = true
  searchMessage.value = ''
  searchResults.value = []

  try {
    const params = { query }
    if (searchStockCodes.value.trim()) {
      params.stock_codes = searchStockCodes.value.trim().split(/[,，\s]+/).filter(Boolean)
    }
    if (searchIndustryNames.value.trim()) {
      params.industry_names = searchIndustryNames.value.trim().split(/[,，\s]+/).filter(Boolean)
    }
    const res = await searchKnowledge(params)
    const payload = res.data || res
    searchResults.value = payload.results || []
    searchTotal.value = payload.total || 0
  } catch (error) {
    searchMessage.value = error.message || '检索失败'
    showToast(searchMessage.value, 'error')
  } finally {
    isSearching.value = false
  }
}

// ── 计算属性 ──

const totalDocuments = computed(() => stats.value.documents.total)

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

async function handleUploadPdfSuccess() {
  showToast('PDF上传处理完成', 'success')
  await loadStats()
  await fetchDocumentList()
}

function handleCleanMarkdown(doc) {
  cleaningDocument.value = doc
  showCleanerModal.value = true
}

async function handleCleanSuccess() {
  showToast('清洗结果已保存', 'success')
  await fetchDocumentList()
}

async function handleUnmarkClean(item) {
  if (!confirm(`确定解除「${item.title}」的清洗标记？解除后可以重新编辑和保存。`)) return
  try {
    await toggleCleanStatus(item.id)
    showToast('清洗标记已解除', 'success')
    await fetchDocumentList()
  } catch (e) {
    showToast(e.message || '解除失败', 'error')
  }
}

// ── 切块操作 ──

const isChunking = ref(false)
const isChunkingAll = ref(false)
const chunkMessage = ref({ type: '', text: '' })

const selectedIds = ref(new Set())

// ── 解析操作 ──

const isParsing = ref(false)
const isParsingAll = ref(false)
const parseMessage = ref({ type: '', text: '' })

const allSelected = computed(() => {
  return listState.items.length > 0 && selectedIds.value.size === listState.items.length
})

const selectedCount = computed(() => selectedIds.value.size)

const toggleSelectAll = () => {
  if (allSelected.value) {
    selectedIds.value = new Set()
  } else {
    selectedIds.value = new Set(listState.items.map(item => item.id))
  }
}

const toggleSelectOne = (id) => {
  const next = new Set(selectedIds.value)
  if (next.has(id)) {
    next.delete(id)
  } else {
    next.add(id)
  }
  selectedIds.value = next
}

// ── 解析操作 ──

const handleSelectedParse = async () => {
  if (selectedIds.value.size === 0) return

  isParsing.value = true
  parseMessage.value = { type: '', text: '' }

  try {
    const res = await parseDocuments([...selectedIds.value])
    const payload = res.data || res
    const successCount = payload.success_count ?? 0
    const failedCount = payload.failed_count ?? 0
    parseMessage.value = {
      type: failedCount === 0 ? 'success' : 'warning',
      text: `解析完成：成功 ${successCount} 个，失败 ${failedCount} 个`
    }
    selectedIds.value = new Set()
    await loadStats()
    await fetchDocumentList()
  } catch (error) {
    parseMessage.value = { type: 'error', text: error.message || '解析操作失败' }
  } finally {
    isParsing.value = false
  }
}

const handleParseAll = async () => {
  isParsingAll.value = true
  parseMessage.value = { type: '', text: '' }
  try {
    const ids = listState.items.map(item => item.id)
    const res = await parseDocuments(ids)
    const payload = res.data || res
    const successCount = payload.success_count ?? 0
    const failedCount = payload.failed_count ?? 0
    parseMessage.value = {
      type: failedCount === 0 ? 'success' : 'warning',
      text: `当前页全部解析完成：成功 ${successCount} 个，失败 ${failedCount} 个`
    }
    selectedIds.value = new Set()
    await loadStats()
    await fetchDocumentList()
  } catch (error) {
    parseMessage.value = { type: 'error', text: error.message || '批量解析失败' }
  } finally {
    isParsingAll.value = false
  }
}

const handleRowParse = async (doc) => {
  rowLoading.value = { ...rowLoading.value, [doc.id]: 'parse' }
  try {
    const res = await parseDocuments([doc.id])
    const payload = res.data || res
    const ok = payload.success_count > 0
    showToast(
      ok ? `「${doc.title}」解析完成` : '解析失败',
      ok ? 'success' : 'error'
    )
    await loadStats()
    await fetchDocumentList()
  } catch (e) {
    showToast(e.message || '解析失败', 'error')
  } finally {
    rowLoading.value = { ...rowLoading.value, [doc.id]: null }
  }
}

// ── 切块操作 ──

const handleSelectedChunk = async () => {
  if (selectedIds.value.size === 0) return

  isChunking.value = true
  chunkMessage.value = { type: '', text: '' }

  try {
    const res = await chunkDocuments([...selectedIds.value])
    const payload = res.data || res
    const successCount = payload.success_count ?? 0
    const failedCount = payload.failed_count ?? 0
    chunkMessage.value = {
      type: failedCount === 0 ? 'success' : 'warning',
      text: `切块完成：成功 ${successCount} 个，失败 ${failedCount} 个`
    }
    selectedIds.value = new Set()
    await loadStats()
    await fetchDocumentList()
  } catch (error) {
    chunkMessage.value = { type: 'error', text: error.message || '切块操作失败' }
  } finally {
    isChunking.value = false
  }
}

const handleChunkAll = async () => {
  isChunkingAll.value = true
  chunkMessage.value = { type: '', text: '' }
  try {
    const ids = listState.items.map(item => item.id)
    const res = await chunkDocuments(ids)
    const payload = res.data || res
    const successCount = payload.success_count ?? 0
    const failedCount = payload.failed_count ?? 0
    chunkMessage.value = {
      type: failedCount === 0 ? 'success' : 'warning',
      text: `当前页全部切块完成：成功 ${successCount} 个，失败 ${failedCount} 个`
    }
    selectedIds.value = new Set()
    await loadStats()
    await fetchDocumentList()
  } catch (error) {
    chunkMessage.value = { type: 'error', text: error.message || '批量切块失败' }
  } finally {
    isChunkingAll.value = false
  }
}

const handleRowChunk = async (doc) => {
  rowLoading.value = { ...rowLoading.value, [doc.id]: 'chunk' }
  try {
    const res = await chunkDocuments([doc.id])
    const payload = res.data || res
    const ok = payload.success_count > 0
    showToast(
      ok ? `「${doc.title}」切块完成` : '切块失败',
      ok ? 'success' : 'error'
    )
    await loadStats()
    await fetchDocumentList()
  } catch (e) {
    showToast(e.message || '切块失败', 'error')
  } finally {
    rowLoading.value = { ...rowLoading.value, [doc.id]: null }
  }
}

// ── 行级操作 ──

const rowFileInputs = ref({})
const rowLoading = ref({})

const setRowFileRef = (id) => (el) => {
  if (el) rowFileInputs.value[id] = el
}

const triggerRowFileInput = (id) => {
  rowFileInputs.value[id]?.click()
}

const handleRowUploadPdf = async (doc, event) => {
  const file = event.target.files?.[0]
  if (!file) return

  rowLoading.value = { ...rowLoading.value, [doc.id]: 'upload' }
  try {
    await uploadKnowledgeDocuments([file])
    showToast(`「${doc.title}」上传成功`, 'success')
    await loadStats()
    await fetchDocumentList()
  } catch (e) {
    showToast(e.message || '上传失败', 'error')
  } finally {
    rowLoading.value = { ...rowLoading.value, [doc.id]: null }
    event.target.value = ''
  }
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
    if (listParseStatusFilter.value !== '') params.parse_status = Number(listParseStatusFilter.value)
    if (listCleanStatusFilter.value !== '') params.clean_status = Number(listCleanStatusFilter.value)
    if (listChunkStatusFilter.value !== '') params.chunk_status = Number(listChunkStatusFilter.value)
    if (listVectorStatusFilter.value !== '') params.vector_status = Number(listVectorStatusFilter.value)

    const response = await getKnowledgeDocumentList(params)
    const payload = response?.data || response
    listState.items = payload?.lists || []
    listState.total = payload?.pagination?.total || 0
    listErrorMessage.value = ''
    selectedIds.value = new Set()
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
  listParseStatusFilter.value = ''
  listCleanStatusFilter.value = ''
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
        <p class="mt-1 text-sm text-ink-500">增量构建模式：初始化 → 上传PDF → 解析 → 清洗 → 切块 → 向量化 → 检索</p>
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
    <div class="grid grid-cols-2 gap-4 lg:grid-cols-3 xl:grid-cols-5 shrink-0">
      <MetricTile title="总文档数" :value="String(totalDocuments)" tone="neutral" />

    </div>

    <!-- ═══ 操作按钮区 ═══ -->
    <div class="flex flex-wrap items-center gap-2 rounded-2xl border border-black/5 bg-white/80 p-3 shrink-0">
      <BaseButton variant="amber" icon="gear" @click="handleSystemInit">系统初始化</BaseButton>

      <BaseButton variant="dark" icon="cloud-arrow-up" :disabled="!isInitialized" @click="showUploadPdfModal = true">上传研报PDF</BaseButton>

      <span class="ml-auto text-xs text-ink-400">
        共 {{ totalMetadataCount }} 条元数据
      </span>
    </div>

    <!-- ═══ 批量操作按钮行（选中后出现） ═══ -->
    <div
      v-if="selectedCount > 0"
      class="shrink-0 rounded-2xl border border-black/5 bg-white/80 p-3 flex items-center gap-3"
    >
      <span class="text-sm text-ink-600">已选中 <strong class="text-ink-900">{{ selectedCount }}</strong> 个文档</span>
      <div class="flex items-center gap-2 ml-auto">
        <BaseButton variant="amber" icon="file-import" size="sm" :loading="isParsing" :disabled="isParsing" @click="handleSelectedParse">
          {{ isParsing ? '解析中...' : '批量解析' }}
        </BaseButton>
        <BaseButton variant="success" icon="cube" size="sm" :loading="isChunking" :disabled="isChunking" @click="handleSelectedChunk">
          {{ isChunking ? '切块中...' : '批量切块' }}
        </BaseButton>
        <BaseButton variant="info" icon="brain" size="sm" :loading="isVectorizing" :disabled="isVectorizing" @click="handleSelectedVectorize">
          {{ isVectorizing ? '向量化中...' : '批量向量化' }}
        </BaseButton>
        <BaseButton variant="ghost" size="sm" @click="selectedIds = new Set()">取消选择</BaseButton>
      </div>
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

    <!-- ═══ 解析结果通知 ═══ -->
    <div
      v-if="parseMessage.text"
      class="shrink-0 rounded-2xl border px-4 py-3 text-sm"
      :class="{
        'border-green-200 bg-green-50 text-green-700': parseMessage.type === 'success',
        'border-yellow-200 bg-yellow-50 text-yellow-700': parseMessage.type === 'warning',
        'border-red-200 bg-red-50 text-red-700': parseMessage.type === 'error'
      }"
    >
      <p class="font-medium">{{ parseMessage.text }}</p>
    </div>

    <!-- ═══ 切块结果通知 ═══ -->
    <div
      v-if="chunkMessage.text"
      class="shrink-0 rounded-2xl border px-4 py-3 text-sm"
      :class="{
        'border-green-200 bg-green-50 text-green-700': chunkMessage.type === 'success',
        'border-yellow-200 bg-yellow-50 text-yellow-700': chunkMessage.type === 'warning',
        'border-red-200 bg-red-50 text-red-700': chunkMessage.type === 'error'
      }"
    >
      <p class="font-medium">{{ chunkMessage.text }}</p>
    </div>

    <!-- ═══ 向量化结果通知 ═══ -->
    <div
      v-if="vectorizeMessage.text"
      class="shrink-0 rounded-2xl border px-4 py-3 text-sm"
      :class="{
        'border-green-200 bg-green-50 text-green-700': vectorizeMessage.type === 'success',
        'border-yellow-200 bg-yellow-50 text-yellow-700': vectorizeMessage.type === 'warning',
        'border-red-200 bg-red-50 text-red-700': vectorizeMessage.type === 'error'
      }"
    >
      <p class="font-medium">{{ vectorizeMessage.text }}</p>
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

        <!-- ═══ 文档分布（增加向量化状态列） ═══ -->
        <section class="rounded-xl border border-black/5 bg-white">
          <div class="flex items-center gap-2 border-b border-black/5 px-4 py-2.5">
            <FontAwesomeIcon :icon="['fas', 'file-lines']" class="text-xs text-ink-300" aria-hidden="true" />
            <h3 class="text-sm font-medium text-ink-700">文档分布</h3>
            <span class="ml-auto text-xs text-ink-400 tabular-nums">{{ stats.documents?.total ?? 0 }} 份</span>
          </div>
          <div class="grid grid-cols-1 gap-0 divide-y divide-black/5 sm:grid-cols-4 sm:divide-x sm:divide-y-0">
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
                  <span class="text-sm text-ink-600">{{ CHUNK_STATUS_MAP[Number(key)]?.label || `状态${key}` }}</span>
                  <span class="text-sm font-medium tabular-nums text-ink-800">{{ count }}</span>
                </div>
              </div>
              <p v-else class="text-xs text-ink-300">-</p>
            </div>

            <!-- 解析状态 -->
            <div class="px-4 py-3">
              <p class="text-xs text-ink-400 mb-2">解析状态</p>
              <div v-if="Object.keys(stats.documents?.by_parse_status || {}).length" class="space-y-1.5">
                <div
                  v-for="(count, key) in stats.documents.by_parse_status"
                  :key="key"
                  class="flex items-center justify-between"
                >
                  <span class="text-sm text-ink-600">{{ PARSE_STATUS_MAP[Number(key)]?.label || `状态${key}` }}</span>
                  <span class="text-sm font-medium tabular-nums text-ink-800">{{ count }}</span>
                </div>
              </div>
              <p v-else class="text-xs text-ink-300">-</p>
            </div>

            <!-- 向量化状态 -->
            <div class="px-4 py-3">
              <p class="text-xs text-ink-400 mb-2">向量化状态</p>
              <div v-if="Object.keys(stats.documents?.by_vector_status || {}).length" class="space-y-1.5">
                <div
                  v-for="(count, key) in stats.documents.by_vector_status"
                  :key="key"
                  class="flex items-center justify-between"
                >
                  <span class="text-sm text-ink-600">{{ VECTOR_STATUS_MAP[Number(key)]?.label || `状态${key}` }}</span>
                  <span class="text-sm font-medium tabular-nums text-ink-800">{{ count }}</span>
                </div>
              </div>
              <p v-else class="text-xs text-ink-300">-</p>
            </div>

          </div>
        </section>
      </template>
    </div>

    <!-- ═══ 文档列表 ═══ -->
    <SurfacePanel :padded="false">
      <!-- 页头 -->
      <div class="border-b border-black/5 px-5 py-5 sm:px-6">
        <div class="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
          <div>
            <p class="shell-kicker">Documents</p>
            <h3 class="mt-2 text-xl font-semibold text-ink-900">文档列表</h3>
            <p class="mt-2 max-w-3xl text-sm leading-6 text-ink-600">
              管理系统中的文档记录，支持上传 PDF、提交转换操作。
            </p>
            <p class="mt-4 text-sm text-ink-500">当前页 {{ listState.items.length }} 条记录</p>
          </div>

          <div class="flex flex-wrap items-center gap-3">
            <BaseButton variant="secondary" icon="rotate-right" size="sm" :loading="isRefreshingList" :disabled="isLoadingList" @click="fetchDocumentList({ silent: true })">刷新列表</BaseButton>
            <BaseButton variant="amber" icon="file-import" size="sm" :loading="isParsingAll" :disabled="!isInitialized || listState.items.length === 0 || isParsingAll" @click="handleParseAll">{{ isParsingAll ? '解析中...' : '一键解析（当前页）' }}</BaseButton>
            <BaseButton variant="success" icon="cube" size="sm" :loading="isChunkingAll" :disabled="!isInitialized || listState.items.length === 0 || isChunkingAll" @click="handleChunkAll">{{ isChunkingAll ? '切块中...' : '一键切块（当前页）' }}</BaseButton>
            <BaseButton variant="info" icon="brain" size="sm" :loading="isVectorizingAll" :disabled="!isInitialized || listState.items.length === 0 || isVectorizingAll" @click="handleVectorizeAll">{{ isVectorizingAll ? '向量化中...' : '一键向量化（当前页）' }}</BaseButton>
          </div>
        </div>

        <!-- 筛选区域 -->
        <div class="mt-4 flex flex-wrap items-center gap-3">
          <BaseInput
            v-model="listKeyword"
            icon="search"
            placeholder="搜索标题关键词..."
            clearable
            @keydown="handleListKeydown"
          />
          <BaseSelect v-model="listDocTypeFilter" :options="docTypeFilterOptions" placeholder="全部类型" />
          <BaseSelect v-model="listParseStatusFilter" :options="parseStatusFilterOptions" placeholder="全部解析状态" />
          <BaseSelect v-model="listCleanStatusFilter" :options="cleanStatusFilterOptions" placeholder="全部清洗状态" />
          <BaseSelect v-model="listChunkStatusFilter" :options="chunkStatusFilterOptions" placeholder="全部切块状态" />
          <BaseSelect v-model="listVectorStatusFilter" :options="vectorStatusFilterOptions" placeholder="全部向量化状态" />
          <BaseButton icon="search" size="sm" @click="handleListSearch">筛选</BaseButton>
          <BaseButton variant="ghost" size="sm" @click="handleListReset">重置</BaseButton>
        </div>
      </div>

      <!-- 内容区 -->
      <div class="p-5 sm:p-6">
        <!-- 加载中 -->
        <div
          v-if="isLoadingList && !hasListItems"
          class="flex flex-col items-center justify-center py-16"
        >
          <FontAwesomeIcon
            :icon="['fas', 'spinner']"
            spin
            class="text-3xl text-accent-500"
            aria-hidden="true"
          />
          <p class="mt-4 text-sm text-ink-500">正在加载文档列表...</p>
        </div>

        <!-- 错误状态 -->
        <div
          v-else-if="listErrorMessage && !hasListItems"
          class="flex flex-col items-center justify-center py-16"
        >
          <p class="text-sm text-danger">{{ listErrorMessage }}</p>
          <BaseButton variant="secondary" @click="fetchDocumentList()">重试</BaseButton>
        </div>

        <!-- 空状态 -->
        <AppEmptyState
          v-else-if="!hasListItems"
          title="当前没有文档记录"
          description="上传研报 PDF 文件后，文档会出现在这里并展示处理状态。"
        />

        <!-- 数据表格 -->
        <div
          v-else
          class="flex flex-col overflow-hidden rounded-[28px] border border-black/5 bg-white"
          style="height: 560px"
        >
          <div class="min-h-0 flex-1 overflow-auto">
            <table class="shell-grid-table min-w-[1400px]">
              <thead class="sticky top-0 z-10">
                <tr>
                  <th class="w-10">
                    <input
                      type="checkbox"
                      class="h-4 w-4 rounded border-ink-300 text-emerald-600 focus:ring-emerald-500"
                      :checked="allSelected"
                      @change="toggleSelectAll"
                    />
                  </th>
                  <th class="text-left">标题</th>
                  <th class="text-left">文档类型</th>
                  <th class="!text-center">股票代码</th>
                  <th class="text-left">股票简称</th>
                  <th class="!text-center">发布日期</th>
                  <th class="!text-center">PDF状态</th>
                  <th class="!text-center">解析状态</th>
                  <th class="!text-center">切块状态</th>
                  <th class="!text-center">向量化状态</th>
                  <th class="!text-center">操作</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="item in listState.items" :key="item.id">
                  <td>
                    <input
                      type="checkbox"
                      class="h-4 w-4 rounded border-ink-300 text-emerald-600 focus:ring-emerald-500"
                      :checked="selectedIds.has(item.id)"
                      @change="toggleSelectOne(item.id)"
                    />
                  </td>
                  <td>
                    <p class="max-w-xs truncate text-sm font-medium text-ink-900" :title="item.title">
                      {{ item.title || '-' }}
                    </p>
                  </td>
                  <td>
                    <span class="text-sm text-ink-600">{{ DOC_TYPE_LABEL_MAP[item.doc_type] || item.doc_type || '-' }}</span>
                  </td>
                  <td class="text-center">
                    <p class="font-mono text-sm text-ink-900">{{ item.stock_code || '-' }}</p>
                  </td>
                  <td>
                    <p class="text-sm text-ink-600">{{ item.stock_abbr || '-' }}</p>
                  </td>
                  <td class="text-center">
                    <p class="text-sm text-ink-500">{{ formatDate(item.publish_date) }}</p>
                  </td>
                  <td class="text-center">
                    <span
                      class="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium"
                      :class="{
                        'bg-yellow-50 text-yellow-700': METADATA_STATUS_MAP[item.metadata_status]?.tone === 'warning',
                        'bg-green-50 text-green-700': METADATA_STATUS_MAP[item.metadata_status]?.tone === 'success',
                        'bg-red-50 text-red-700': METADATA_STATUS_MAP[item.metadata_status]?.tone === 'danger',
                        'bg-blue-50 text-blue-700': METADATA_STATUS_MAP[item.metadata_status]?.tone === 'accent',
                        'bg-ink-50 text-ink-600': !METADATA_STATUS_MAP[item.metadata_status] || METADATA_STATUS_MAP[item.metadata_status]?.tone === 'neutral'
                      }"
                    >
                      {{ METADATA_STATUS_MAP[item.metadata_status]?.label || `状态${item.metadata_status}` }}
                    </span>
                  </td>
                  <td class="text-center">
                    <span
                      class="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium"
                      :class="{
                        'bg-yellow-50 text-yellow-700': PARSE_STATUS_MAP[item.parse_status]?.tone === 'warning',
                        'bg-green-50 text-green-700': PARSE_STATUS_MAP[item.parse_status]?.tone === 'success',
                        'bg-red-50 text-red-700': PARSE_STATUS_MAP[item.parse_status]?.tone === 'danger',
                        'bg-ink-50 text-ink-600': !PARSE_STATUS_MAP[item.parse_status] || PARSE_STATUS_MAP[item.parse_status]?.tone === 'neutral'
                      }"
                    >
                      {{ PARSE_STATUS_MAP[item.parse_status]?.label || `状态${item.parse_status}` }}
                    </span>
                  </td>
                  <td class="text-center">
                    <span
                      class="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium"
                      :class="{
                        'bg-yellow-50 text-yellow-700': CHUNK_STATUS_MAP[item.chunk_status]?.tone === 'warning',
                        'bg-green-50 text-green-700': CHUNK_STATUS_MAP[item.chunk_status]?.tone === 'success',
                        'bg-red-50 text-red-700': CHUNK_STATUS_MAP[item.chunk_status]?.tone === 'danger',
                        'bg-ink-50 text-ink-600': !CHUNK_STATUS_MAP[item.chunk_status] || CHUNK_STATUS_MAP[item.chunk_status]?.tone === 'neutral'
                      }"
                    >
                      {{ CHUNK_STATUS_MAP[item.chunk_status]?.label || `状态${item.chunk_status}` }}
                    </span>
                  </td>
                  <td class="text-center">
                    <span
                      class="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium"
                      :class="{
                        'bg-yellow-50 text-yellow-700': VECTOR_STATUS_MAP[item.vector_status]?.tone === 'warning',
                        'bg-green-50 text-green-700': VECTOR_STATUS_MAP[item.vector_status]?.tone === 'success',
                        'bg-red-50 text-red-700': VECTOR_STATUS_MAP[item.vector_status]?.tone === 'danger',
                        'bg-ink-50 text-ink-600': !VECTOR_STATUS_MAP[item.vector_status] || VECTOR_STATUS_MAP[item.vector_status]?.tone === 'neutral'
                      }"
                    >
                      {{ VECTOR_STATUS_MAP[item.vector_status]?.label || `状态${item.vector_status}` }}
                    </span>
                  </td>
                  <td class="text-center">
                    <div class="inline-flex items-center gap-2 flex-nowrap">
                      <input
                        :ref="setRowFileRef(item.id)"
                        type="file"
                        accept=".pdf"
                        class="hidden"
                        @change="handleRowUploadPdf(item, $event)"
                      />
                      <BaseButton
                        variant="info"
                        icon="cloud-arrow-up"
                        size="xs"
                        :loading="rowLoading[item.id] === 'upload'"
                        :disabled="item.metadata_status === 2"
                        :title="item.metadata_status === 1 ? '上传PDF' : item.metadata_status === 2 ? 'PDF已上传' : '还未上传'"
                        @click="triggerRowFileInput(item.id)"
                      >上传</BaseButton>
                      <BaseButton
                        variant="amber"
                        icon="file-import"
                        size="xs"
                        :loading="rowLoading[item.id] === 'parse'"
                        :disabled="item.metadata_status !== 2 || ![0, 3].includes(item.parse_status)"
                        :title="item.metadata_status !== 2 ? '请先上传PDF' : [0, 3].includes(item.parse_status) ? '提交解析' : (PARSE_STATUS_MAP[item.parse_status]?.label || '')"
                        @click="handleRowParse(item)"
                      >解析</BaseButton>
                      <BaseButton
                        variant="secondary"
                        icon="broom"
                        size="xs"
                        :disabled="item.parse_status !== 2"
                        :title="item.parse_status === 2 ? '清洗Markdown' : '请先完成解析'"
                        @click="handleCleanMarkdown(item)"
                      >清洗</BaseButton>
                      <BaseButton
                        variant="success"
                        icon="cube"
                        size="xs"
                        :loading="rowLoading[item.id] === 'chunk'"
                        :disabled="item.clean_status !== 1"
                        :title="item.clean_status !== 1 ? '请先完成清洗' : '提交切块（支持重新切块）'"
                        @click="handleRowChunk(item)"
                      >切块</BaseButton>
                      <BaseButton
                        variant="info"
                        icon="brain"
                        size="xs"
                        :loading="rowLoading[item.id] === 'vectorize'"
                        :disabled="item.chunk_status !== 2"
                        :title="item.chunk_status !== 2 ? '请先完成切块' : '提交向量化'"
                        @click="handleRowVectorize(item)"
                      >向量化</BaseButton>

                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <!-- 分页 -->
          <PaginationBar
            v-model:page="listState.page"
            v-model:pageSize="listState.pageSize"
            :total="listState.total"
            @change="fetchDocumentList"
          />
        </div>
      </div>
    </SurfacePanel>

    <!-- ═══ 语义检索 ═══ -->
    <SurfacePanel :padded="false">
      <div class="border-b border-black/5 px-5 py-5 sm:px-6">
        <div class="flex flex-col gap-4">
          <div>
            <p class="shell-kicker">Search</p>
            <h3 class="mt-2 text-xl font-semibold text-ink-900">语义检索</h3>
            <p class="mt-2 max-w-3xl text-sm leading-6 text-ink-600">
              对已向量化的文档进行语义搜索，支持按股票代码和行业名称过滤。
            </p>
          </div>

          <div class="flex flex-wrap items-end gap-3">
            <div class="min-w-[300px] flex-1">
              <label class="mb-1 block text-xs font-medium text-ink-500">检索查询</label>
              <BaseInput
                v-model="searchQuery"
                icon="search"
                placeholder="输入检索语句，如：新能源汽车市场前景分析"
                @keydown.enter="handleSearch"
              />
            </div>
            <div class="w-48">
              <label class="mb-1 block text-xs font-medium text-ink-500">股票代码（可选）</label>
              <BaseInput
                v-model="searchStockCodes"
                placeholder="多个用逗号分隔"
              />
            </div>
            <div class="w-48">
              <label class="mb-1 block text-xs font-medium text-ink-500">行业名称（可选）</label>
              <BaseInput
                v-model="searchIndustryNames"
                placeholder="多个用逗号分隔"
              />
            </div>
            <BaseButton variant="dark" icon="search" :loading="isSearching" @click="handleSearch">检索</BaseButton>
          </div>
        </div>
      </div>

      <div class="p-5 sm:p-6">
        <!-- 检索加载中 -->
        <div
          v-if="isSearching"
          class="flex flex-col items-center justify-center py-16"
        >
          <FontAwesomeIcon :icon="['fas', 'spinner']" spin class="text-3xl text-accent-500" aria-hidden="true" />
          <p class="mt-4 text-sm text-ink-500">正在检索...</p>
        </div>

        <!-- 检索结果为空 -->
        <AppEmptyState
          v-else-if="!isSearching && searchResults.length === 0 && searchQuery && !searchMessage"
          title="未找到相关结果"
          description="请尝试调整检索语句或过滤条件。"
        />

        <!-- 检索结果列表 -->
        <div v-else-if="searchResults.length > 0" class="space-y-3">
          <p class="text-sm text-ink-500">共找到 {{ searchTotal }} 条结果</p>
          <div
            v-for="(result, idx) in searchResults"
            :key="idx"
            class="rounded-xl border border-black/5 bg-white p-4"
          >
            <div class="mb-2 flex items-center justify-between">
              <div class="flex items-center gap-2">
                <span class="inline-flex items-center rounded-full bg-emerald-50 px-2 py-0.5 text-xs font-medium text-emerald-700">
                  相关度 {{ (result.score * 100).toFixed(1) }}%
                </span>
                <span
                  class="inline-flex items-center rounded-full bg-ink-50 px-2 py-0.5 text-xs font-medium text-ink-600"
                >
                  {{ DOC_TYPE_LABEL_MAP[result.doc_type] || result.doc_type }}
                </span>
                <span v-if="result.stock_code" class="font-mono text-xs text-ink-400">
                  {{ result.stock_code }} {{ result.stock_abbr }}
                </span>
                <span v-if="result.industry_name" class="text-xs text-ink-400">
                  {{ result.industry_name }}
                </span>
              </div>
              <span class="text-xs text-ink-400">文档ID: {{ result.document_id }} / 块索引: {{ result.chunk_index }}</span>
            </div>
            <p class="text-sm leading-6 text-ink-700 line-clamp-4">{{ result.chunk_text }}</p>
          </div>
        </div>

        <!-- 初始状态 -->
        <AppEmptyState
          v-else
          title="输入检索语句开始查询"
          description="在上方输入查询内容，可选择股票代码和行业名称进行过滤。"
        />
      </div>
    </SurfacePanel>

    <!-- ═══ 系统初始化弹窗 ═══ -->
    <SystemInitModal
      :visible="showSystemInitModal"
      @close="showSystemInitModal = false"
      @success="handleSystemInitSuccess"
    />

    <!-- ═══ 批量上传PDF弹窗 ═══ -->
    <UploadPdfModal
      :visible="showUploadPdfModal"
      @close="showUploadPdfModal = false"
      @success="handleUploadPdfSuccess"
    />

    <!-- ═══ Markdown 清洗弹窗 ═══ -->
    <MarkdownCleanerModal
      :visible="showCleanerModal"
      :document="cleaningDocument"
      @close="showCleanerModal = false"
      @success="handleCleanSuccess"
    />
  </div>
</template>

<style scoped>
.hidden {
  display: none;
}
</style>