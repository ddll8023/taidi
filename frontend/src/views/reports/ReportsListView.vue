<script setup>
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'

import { getFinancialReports, deleteFinancialReport, parseReport, parseReportsBatch, getBatchParseStatus } from '@/api/financialReports'
import AppEmptyState from '@/components/common/AppEmptyState.vue'
import AppErrorState from '@/components/common/AppErrorState.vue'
import AppLoadingState from '@/components/common/AppLoadingState.vue'
import FilterBar from '@/components/common/FilterBar.vue'
import StatusBadge from '@/components/common/StatusBadge.vue'
import SurfacePanel from '@/components/common/SurfacePanel.vue'
import UploadModal from '@/components/common/UploadModal.vue'

const DEFAULT_PAGE_SIZE = 10
const PAGE_SIZE_OPTIONS = [10, 20, 50]

const listState = reactive({
  items: [],
  page: 1,
  pageSize: DEFAULT_PAGE_SIZE,
  total: 0
})

const jumpPageInput = ref('')

const searchKeyword = ref('')
const parseStatusFilter = ref('')
const vectorStatusFilter = ref('')
const sortBy = ref('updated_at')
const sortOrder = ref('desc')

const PARSE_STATUS_OPTIONS = [
  { value: '', label: '全部解析状态' },
  { value: '0', label: '待解析' },
  { value: '1', label: '解析成功' },
  { value: '2', label: '解析失败' },
  { value: '3', label: '解析中' }
]

const VECTOR_STATUS_OPTIONS = [
  { value: '', label: '全部向量化状态' },
  { value: '0', label: '待向量化' },
  { value: '1', label: '向量化中' },
  { value: '2', label: '已完成' },
  { value: '3', label: '失败' },
  { value: '4', label: '已跳过' }
]

const SORT_BY_OPTIONS = [
  { value: 'updated_at', label: '更新时间' },
  { value: 'created_at', label: '创建时间' }
]

const SORT_ORDER_OPTIONS = [
  { value: 'desc', label: '倒序' },
  { value: 'asc', label: '正序' }
]

const isLoading = ref(false)
const isRefreshing = ref(false)
const errorMessage = ref('')
const deletingId = ref(null)
const isParsingAll = ref(false)
const isRetryingAll = ref(false)
const parsingReportIds = ref(new Set())
const pollingReportIds = ref(new Set())
let pollingTimer = null
const showUploadModal = ref(false)
const notice = ref({
  type: '',
  message: ''
})

const hasRecords = computed(() => listState.items.length > 0)
const hasPendingReports = computed(() => listState.items.some(item => item.parseStatusKey === 'pending'))
const hasFailedReports = computed(() => listState.items.some(item => item.parseStatusKey === 'failed'))
const totalPages = computed(() => {
  const pageSize = Number(listState.pageSize) || DEFAULT_PAGE_SIZE
  const total = Number(listState.total) || listState.items.length

  return Math.max(1, Math.ceil(total / pageSize))
})

const pageSummary = computed(() => `当前页 ${listState.items.length} 条记录`)

const noticeClass = computed(() => {
  const classMap = {
    error: 'border-transparent bg-[var(--danger-soft)] text-danger',
    info: 'border-accent-200 bg-accent-50/80 text-accent-700',
    warning: 'border-transparent bg-[var(--warning-soft)] text-warning'
  }

  return classMap[notice.value.type] || 'border-ink-200 bg-white text-ink-700'
})

const pickValue = (source, keys, fallback = '') => {
  for (const key of keys) {
    const value = source?.[key]

    if (value !== undefined && value !== null && `${value}`.trim() !== '') {
      return value
    }
  }

  return fallback
}

const formatDateTime = (value) => {
  if (!value) {
    return '待接口返回'
  }

  const date = new Date(value)

  if (Number.isNaN(date.getTime())) {
    return value
  }

  return new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  }).format(date)
}

const formatParseStatus = (value) => {
  if (value === undefined || value === null || value === '') {
    return '待接口返回'
  }

  const numValue = Number(value)
  const statusMap = {
    0: '待处理',
    1: '解析成功',
    2: '解析失败'
  }

  if (!Number.isNaN(numValue) && statusMap[numValue] !== undefined) {
    return statusMap[numValue]
  }

  const rawValue = `${value}`.trim()
  const normalizedValue = rawValue.toLowerCase()

  const labelMap = {
    completed: '已完成',
    created: '已创建',
    done: '已完成',
    failed: '失败',
    idle: '未执行',
    imported: '已入库',
    in_progress: '处理中',
    not_started: '未执行',
    parsed: '已解析',
    pending: '未执行',
    processing: '处理中',
    queued: '处理中',
    running: '处理中',
    saved: '已保存',
    stored: '已入库',
    structured: '已结构化',
    success: '已完成',
    uploaded: '已上传'
  }

  if (labelMap[normalizedValue]) {
    return labelMap[normalizedValue]
  }

  if (/^[\u4e00-\u9fa5]+$/.test(rawValue)) {
    return rawValue
  }

  return rawValue.replace(/[_-]/g, ' ')
}

const formatImportStatus = (value) => {
  if (value === undefined || value === null || value === '') {
    return '待接口返回'
  }

  const numValue = Number(value)
  const statusMap = {
    0: '待入库',
    1: '入库成功',
    2: '入库失败'
  }

  if (!Number.isNaN(numValue) && statusMap[numValue] !== undefined) {
    return statusMap[numValue]
  }

  return `${value}`
}

const formatStatusLabel = (value, fallback = '待接口返回') => {
  if (!value) {
    return fallback
  }

  const rawValue = `${value}`.trim()
  const normalizedValue = rawValue.toLowerCase()

  const labelMap = {
    completed: '已完成',
    created: '已创建',
    done: '已完成',
    failed: '失败',
    idle: '未执行',
    imported: '已入库',
    in_progress: '处理中',
    not_started: '未执行',
    parsed: '已解析',
    pending: '未执行',
    processing: '处理中',
    queued: '处理中',
    running: '处理中',
    saved: '已保存',
    stored: '已入库',
    structured: '已结构化',
    success: '已完成',
    uploaded: '已上传'
  }

  if (labelMap[normalizedValue]) {
    return labelMap[normalizedValue]
  }

  if (/^[\u4e00-\u9fa5]+$/.test(rawValue)) {
    return rawValue
  }

  return rawValue.replace(/[_-]/g, ' ')
}

const resolveVectorStatusMeta = (value) => {
  const numValue = Number(value)

  if (!Number.isNaN(numValue)) {
    const numericStatusMap = {
      0: { key: 'pending', label: '待向量化', tone: 'warning' },
      1: { key: 'processing', label: '向量化中', tone: 'accent' },
      2: { key: 'completed', label: '已完成', tone: 'success' },
      3: { key: 'failed', label: '失败', tone: 'danger' },
      4: { key: 'skipped', label: '已跳过', tone: 'neutral' }
    }

    if (numericStatusMap[numValue] !== undefined) {
      return numericStatusMap[numValue]
    }
  }

  const rawValue = `${value || ''}`.trim()
  const normalizedValue = rawValue.toLowerCase()

  if (!rawValue || ['idle', 'not_started', 'pending', 'unprocessed'].includes(normalizedValue) || rawValue === '未执行') {
    return {
      key: 'pending',
      label: '未执行',
      tone: 'warning'
    }
  }

  if (
    ['in_progress', 'processing', 'queued', 'running'].includes(normalizedValue) ||
    rawValue === '处理中'
  ) {
    return {
      key: 'processing',
      label: '处理中',
      tone: 'accent'
    }
  }

  if (
    ['completed', 'done', 'success', 'vectorized'].includes(normalizedValue) ||
    rawValue === '已完成'
  ) {
    return {
      key: 'completed',
      label: '已完成',
      tone: 'success'
    }
  }

  if (['error', 'failed'].includes(normalizedValue) || rawValue === '失败') {
    return {
      key: 'failed',
      label: '失败',
      tone: 'danger'
    }
  }

  return {
    key: 'unknown',
    label: formatStatusLabel(rawValue, '未知状态'),
    tone: 'neutral'
  }
}

const resolveParseStatusMeta = (value) => {
  if (value === undefined || value === null || value === '') {
    return { key: 'unknown', label: '待接口返回', tone: 'neutral' }
  }

  const numValue = Number(value)
  if (!Number.isNaN(numValue)) {
    const statusMap = {
      0: { key: 'pending', label: '待解析', tone: 'warning' },
      1: { key: 'success', label: '解析成功', tone: 'success' },
      2: { key: 'failed', label: '解析失败', tone: 'danger' },
      3: { key: 'processing', label: '解析中', tone: 'accent' }
    }
    if (statusMap[numValue] !== undefined) {
      return statusMap[numValue]
    }
  }

  return { key: 'unknown', label: `${value}`, tone: 'neutral' }
}

const resolveImportStatusMeta = (value) => {
  if (value === undefined || value === null || value === '') {
    return null
  }

  const numValue = Number(value)
  if (!Number.isNaN(numValue)) {
    const statusMap = {
      0: { label: '待入库', tone: 'warning' },
      1: { label: '入库成功', tone: 'success' },
      2: { label: '入库失败', tone: 'danger' }
    }
    if (statusMap[numValue] !== undefined) {
      return statusMap[numValue]
    }
  }

  return { label: `${value}`, tone: 'neutral' }
}

const buildParsingStatusItems = (item) => {
  const parseStatus = pickValue(item, ['parse_status', 'parsing_status', 'processing_status'])
  const importStatus = pickValue(item, ['import_status', 'importStatus'])
  const items = []

  const parseStatusMeta = resolveParseStatusMeta(parseStatus)
  items.push({
    key: 'parse',
    labelPrefix: '解析',
    ...parseStatusMeta
  })

  const importMeta = resolveImportStatusMeta(importStatus)
  if (importMeta) {
    items.push({
      key: 'import',
      labelPrefix: '入库',
      ...importMeta
    })
  }

  return {
    items,
    parseStatusKey: parseStatusMeta.key
  }
}

const normalizeReportItem = (item, index) => {
  const id = pickValue(item, ['id', 'report_id', 'reportId'])
  const vectorStatusMeta = resolveVectorStatusMeta(
    pickValue(item, ['vector_status', 'vectorStatus', 'embedding_status', 'embeddingStatus'])
  )
  const parsingResult = buildParsingStatusItems(item)

  return {
    id,
    key: id || `${listState.page}-${index}`,
    fileName: pickValue(
      item,
      ['file_name', 'fileName', 'filename', 'original_filename', 'report_name', 'name'],
      '未命名文件'
    ),
    reportTitle: pickValue(item, ['report_title', 'reportTitle', 'title'], '暂无标题'),
    parseStatusItems: parsingResult.items,
    parseStatusKey: parsingResult.parseStatusKey,
    uploadedAtText: formatDateTime(
      pickValue(item, ['uploaded_at', 'upload_time', 'created_at', 'createdAt'])
    ),
    vectorStatusKey: vectorStatusMeta.key,
    vectorStatusLabel: vectorStatusMeta.label,
    vectorStatusTone: vectorStatusMeta.tone
  }
}

const normalizeListResponse = (response) => {
  const payload = response?.data && !Array.isArray(response.data) ? response.data : response
  const items =
    [
      Array.isArray(payload) ? payload : null,
      payload?.lists,
      payload?.items,
      payload?.records,
      payload?.list,
      payload?.results,
      payload?.data
    ].find((candidate) => Array.isArray(candidate)) || []
  const pagination = payload?.pagination || {}
  const resolvedPageSize =
    Number(pagination.page_size || pagination.pageSize || payload?.page_size || payload?.pageSize || payload?.limit || listState.pageSize) ||
    DEFAULT_PAGE_SIZE
  const resolvedTotal =
    Number(
      pagination.total ||
        pagination.total_count ||
        pagination.count ||
        payload?.total ||
        payload?.total_count ||
        payload?.count ||
        (pagination.total_pages ? Number(pagination.total_pages) * resolvedPageSize : items.length)
    ) || 0

  return {
    items: items.map((item, index) => normalizeReportItem(item, index)),
    page: Number(pagination.page || pagination.current_page || pagination.page_num || payload?.page || payload?.current_page || payload?.page_num || listState.page) || 1,
    pageSize: resolvedPageSize,
    total: resolvedTotal
  }
}

const setNotice = (type, message) => {
  notice.value = {
    type,
    message
  }
}

const clearNotice = () => {
  notice.value = {
    type: '',
    message: ''
  }
}

const buildFilterParams = () => {
  const params = {
    page: listState.page,
    page_size: listState.pageSize
  }

  if (searchKeyword.value.trim()) {
    params.keyword = searchKeyword.value.trim()
  }

  if (parseStatusFilter.value !== '') {
    params.parse_status = parseStatusFilter.value
  }

  if (vectorStatusFilter.value !== '') {
    params.vector_status = vectorStatusFilter.value
  }

  if (sortBy.value) {
    params.sort_by = sortBy.value
  }

  if (sortOrder.value) {
    params.sort_order = sortOrder.value
  }

  return params
}

const fetchReports = async ({ silent = false } = {}) => {
  if (silent) {
    isRefreshing.value = true
  } else {
    isLoading.value = true
    errorMessage.value = ''
  }

  try {
    const response = await getFinancialReports(buildFilterParams())
    const normalized = normalizeListResponse(response)

    listState.items = normalized.items
    listState.page = normalized.page
    listState.pageSize = normalized.pageSize
    listState.total = normalized.total
    errorMessage.value = ''

    return normalized.items
  } catch (error) {
    errorMessage.value = error.message || '记录列表加载失败，请稍后重试。'
    throw error
  } finally {
    if (silent) {
      isRefreshing.value = false
    } else {
      isLoading.value = false
    }
  }
}

const refreshReports = async () => {
  clearNotice()

  try {
    await fetchReports({ silent: true })
  } catch (error) {
    setNotice('error', error.message || '记录刷新失败，请稍后重试。')
  }
}

const handleSearch = async () => {
  listState.page = 1
  clearNotice()

  try {
    await fetchReports()
  } catch (error) {
    setNotice('error', error.message || '搜索失败，请稍后重试。')
  }
}

const resetFilters = async () => {
  searchKeyword.value = ''
  parseStatusFilter.value = ''
  vectorStatusFilter.value = ''
  sortBy.value = 'updated_at'
  sortOrder.value = 'desc'
  listState.page = 1
  clearNotice()

  try {
    await fetchReports()
  } catch (error) {
    setNotice('error', error.message || '重置筛选失败，请稍后重试。')
  }
}

const changePage = async (targetPage) => {
  if (
    targetPage < 1 ||
    targetPage > totalPages.value ||
    targetPage === listState.page ||
    isLoading.value ||
    isRefreshing.value
  ) {
    return
  }

  listState.page = targetPage

  try {
    await fetchReports()
  } catch (error) {
    setNotice('error', error.message || '分页切换失败，请稍后重试。')
  }
}

const changePageSize = async (newSize) => {
  const size = Number(newSize)
  if (isNaN(size) || !PAGE_SIZE_OPTIONS.includes(size) || size === listState.pageSize) {
    return
  }

  listState.pageSize = size
  listState.page = 1

  try {
    await fetchReports()
  } catch (error) {
    setNotice('error', error.message || '每页数量切换失败，请稍后重试。')
  }
}

const handleJumpPage = async () => {
  const targetPage = parseInt(jumpPageInput.value, 10)
  
  if (isNaN(targetPage) || targetPage < 1 || targetPage > totalPages.value) {
    setNotice('warning', `请输入有效的页码（1-${totalPages.value}）`)
    return
  }

  jumpPageInput.value = ''
  await changePage(targetPage)
}

const getViewRoute = (report) => (report.id ? `/reports/view/${report.id}` : '/reports/view')

const handleDelete = async (report) => {
  if (!report.id) {
    setNotice('error', '无法删除：记录 ID 不存在')
    return
  }

  const confirmed = confirm(
    '确定要删除这条财报记录吗？此操作将同时删除数据库记录和相关文件，且无法恢复。'
  )

  if (!confirmed) {
    return
  }

  deletingId.value = report.id
  clearNotice()

  try {
    await deleteFinancialReport(report.id)
    listState.items = listState.items.filter((item) => item.id !== report.id)
    listState.total = Math.max(0, listState.total - 1)
    setNotice('info', '财报记录删除成功')
  } catch (error) {
    setNotice('error', error.message || '删除失败，请稍后重试。')
  } finally {
    deletingId.value = null
  }
}

const startPolling = (reportIds) => {
  reportIds.forEach(id => pollingReportIds.value.add(id))
  
  if (pollingTimer) {
    return
  }
  
  const poll = async () => {
    if (pollingReportIds.value.size === 0) {
      stopPolling()
      return
    }
    
    try {
      const response = await getBatchParseStatus(Array.from(pollingReportIds.value))
      const result = response?.data || response
      
      const stillProcessing = new Set(
        Object.entries(result.results || {})
          .filter(([_, r]) => r.parse_status === 3)
          .map(([id, _]) => parseInt(id))
      )
      
      pollingReportIds.value = stillProcessing
      
      if (pollingReportIds.value.size === 0) {
        stopPolling()
        setNotice('info', `所有解析任务已完成：成功 ${result.completed_count || 0} 个`)
        await fetchReports({ silent: true })
      }
    } catch (error) {
      console.error('轮询状态查询失败:', error)
    }
  }
  
  pollingTimer = setInterval(poll, 5000)
}

const stopPolling = () => {
  if (pollingTimer) {
    clearInterval(pollingTimer)
    pollingTimer = null
  }
}

const handleParseReport = async (report, force = false) => {
  if (!report.id) {
    setNotice('error', '无法解析：记录 ID 不存在')
    return
  }

  clearNotice()

  try {
    const response = await parseReport(report.id, force)
    const result = response?.data || response
    
    if (result.status === 'processing') {
      setNotice('info', `报告「${report.reportTitle}」解析任务已提交`)
      startPolling([report.id])
    } else {
      setNotice('info', result.message || '解析完成')
      await fetchReports({ silent: true })
    }
  } catch (error) {
    setNotice('error', error.message || '解析失败，请稍后重试。')
  }
}

const handleParseAll = async () => {
  const pendingReports = listState.items.filter((item) => item.parseStatusKey === 'pending' && item.id)
  if (pendingReports.length === 0) {
    setNotice('warning', '当前列表中没有待解析的报告')
    return
  }

  isParsingAll.value = true
  clearNotice()

  try {
    const reportIds = pendingReports.map((item) => item.id)
    const response = await parseReportsBatch(reportIds)
    const result = response?.data || response
    
    if (result.submitted_count > 0) {
      setNotice('info', `已提交 ${result.submitted_count} 个解析任务，正在后台处理...`)
      startPolling(result.submitted_report_ids)
    }
    
    if (result.skipped_count > 0) {
      setNotice('warning', `${result.skipped_count} 个报告被跳过（已解析或正在解析）`)
    }
  } catch (error) {
    setNotice('error', error.message || '批量解析失败，请稍后重试。')
  } finally {
    isParsingAll.value = false
  }
}

const handleRetryAll = async () => {
  const failedReports = listState.items.filter((item) => item.parseStatusKey === 'failed' && item.id)
  if (failedReports.length === 0) {
    setNotice('warning', '当前列表中没有解析失败的报告')
    return
  }

  isRetryingAll.value = true
  clearNotice()

  try {
    const reportIds = failedReports.map((item) => item.id)
    const response = await parseReportsBatch(reportIds)
    const result = response?.data || response
    
    if (result.submitted_count > 0) {
      setNotice('info', `已提交 ${result.submitted_count} 个重新解析任务，正在后台处理...`)
      startPolling(result.submitted_report_ids)
    }
    
    if (result.skipped_count > 0) {
      setNotice('warning', `${result.skipped_count} 个报告被跳过`)
    }
  } catch (error) {
    setNotice('error', error.message || '批量重新解析失败，请稍后重试。')
  } finally {
    isRetryingAll.value = false
  }
}

const handleUploadComplete = async () => {
  await fetchReports({ silent: true })
  showUploadModal.value = false
}

onMounted(async () => {
  try {
    await fetchReports()
  } catch (error) {
    setNotice('error', error.message || '记录列表加载失败，请稍后重试。')
  }
})

onUnmounted(() => {
  stopPolling()
})
</script>

<template>
  <div class="space-y-6">
    <SurfacePanel :padded="false">
      <div class="border-b border-black/5 px-5 py-5 sm:px-6">
        <div class="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
          <div>
            <p class="shell-kicker">Records</p>
            <h2 class="mt-2 text-xl font-semibold text-ink-900">财报记录列表</h2>
            <p class="mt-2 max-w-3xl text-sm leading-6 text-ink-600">
              展示文件信息、解析状态、向量化状态和操作区。向量化功能待后端实现后开放。
            </p>
            <p class="mt-4 text-sm text-ink-500">{{ pageSummary }}</p>
          </div>

          <div class="flex flex-wrap items-center gap-3">
            <button
              type="button"
              class="shell-button"
              @click="showUploadModal = true"
            >
              <FontAwesomeIcon :icon="['fas', 'cloud-arrow-up']" aria-hidden="true" />
              <span>上传文件</span>
            </button>
            <button
              type="button"
              class="shell-button-secondary"
              :disabled="isRefreshing || isLoading"
              @click="refreshReports"
            >
              <FontAwesomeIcon :icon="['fas', 'rotate-right']" aria-hidden="true" />
              <span>{{ isRefreshing ? '刷新中...' : '刷新列表' }}</span>
            </button>
            <button
              type="button"
              class="shell-button"
              :disabled="isParsingAll || !hasPendingReports"
              @click="handleParseAll"
            >
              <FontAwesomeIcon :icon="['fas', 'play']" aria-hidden="true" />
              <span>{{ isParsingAll ? '解析中...' : '一键解析' }}</span>
            </button>
            <button
              type="button"
              class="shell-button-secondary"
              :disabled="isRetryingAll || !hasFailedReports"
              @click="handleRetryAll"
            >
              <FontAwesomeIcon :icon="['fas', 'rotate-right']" aria-hidden="true" />
              <span>{{ isRetryingAll ? '重试中...' : '批量重试' }}</span>
            </button>
            <button
              type="button"
              class="shell-button"
              disabled
              title="向量化功能待后端实现"
            >
              <FontAwesomeIcon :icon="['fas', 'gears']" aria-hidden="true" />
              <span>一键向量化（待实现）</span>
            </button>
          </div>
        </div>

        <div class="mt-4">
          <FilterBar
            :search-value="searchKeyword"
            :parse-status-options="PARSE_STATUS_OPTIONS"
            :vector-status-options="VECTOR_STATUS_OPTIONS"
            :parse-status-value="parseStatusFilter"
            :vector-status-value="vectorStatusFilter"
            :is-loading="isLoading"
            @update:search-value="searchKeyword = $event"
            @update:parse-status-value="parseStatusFilter = $event"
            @update:vector-status-value="vectorStatusFilter = $event"
            @search="handleSearch"
            @reset="resetFilters"
          />
          <div class="mt-3 flex flex-wrap items-center gap-4">
            <div class="flex items-center gap-2">
              <span class="text-sm text-ink-500">排序：</span>
              <select
                v-model="sortBy"
                class="rounded-lg border border-black/10 bg-white px-3 py-1.5 text-sm focus:border-accent-500 focus:outline-none focus:ring-1 focus:ring-accent-500"
                @change="handleSearch"
              >
                <option v-for="option in SORT_BY_OPTIONS" :key="option.value" :value="option.value">
                  {{ option.label }}
                </option>
              </select>
              <select
                v-model="sortOrder"
                class="rounded-lg border border-black/10 bg-white px-3 py-1.5 text-sm focus:border-accent-500 focus:outline-none focus:ring-1 focus:ring-accent-500"
                @change="handleSearch"
              >
                <option v-for="option in SORT_ORDER_OPTIONS" :key="option.value" :value="option.value">
                  {{ option.label }}
                </option>
              </select>
            </div>
          </div>
        </div>

        <div
          v-if="notice.message"
          class="mt-4 rounded-2xl border px-4 py-3 text-sm"
          :class="noticeClass"
        >
          {{ notice.message }}
        </div>
      </div>

      <div class="p-5 sm:p-6">
        <AppLoadingState
          v-if="isLoading && !hasRecords"
          title="正在载入记录列表"
          description="列表数据将直接来自现有接口，当前不再展示静态假行数据。"
        />

        <AppErrorState
          v-else-if="errorMessage && !hasRecords"
          title="记录列表加载失败"
          :description="errorMessage"
          @retry="fetchReports().catch(() => {})"
        />

        <AppEmptyState
          v-else-if="!hasRecords"
          title="当前没有记录"
          description="上传财报文件后，记录会出现在这里，并直接展示解析状态和向量化状态。"
        />

        <div v-else class="flex flex-col overflow-hidden rounded-[28px] border border-black/5 bg-white" style="height: 600px;">
          <div class="min-h-0 flex-1 overflow-auto">
            <table class="shell-grid-table min-w-[860px]">
              <thead class="sticky top-0 z-10">
                <tr>
                  <th>文件信息</th>
                  <th>报告标题</th>
                  <th>解析状态</th>
                  <th>向量化状态</th>
                  <th>操作区</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="report in listState.items" :key="report.key">
                  <td class="w-[26%]">
                    <div class="space-y-2">
                      <p class="font-medium text-ink-900">{{ report.fileName }}</p>
                      <p class="text-xs uppercase tracking-[0.16em] text-ink-400">上传时间</p>
                      <p class="text-sm text-ink-600">{{ report.uploadedAtText }}</p>
                    </div>
                  </td>
                  <td class="w-[26%]">
                    <p class="text-sm leading-6 text-ink-700">{{ report.reportTitle }}</p>
                  </td>
                  <td class="w-[16%]">
                    <div class="space-y-2">
                      <div
                        v-for="statusItem in report.parseStatusItems"
                        :key="statusItem.key"
                        class="flex items-center gap-1.5"
                      >
                        <span class="text-xs text-ink-500">{{ statusItem.labelPrefix }}：</span>
                        <StatusBadge
                          :label="statusItem.label"
                          :tone="statusItem.tone"
                        />
                      </div>
                    </div>
                  </td>
                  <td class="w-[12%]">
                    <StatusBadge
                      :label="report.vectorStatusLabel"
                      :tone="report.vectorStatusTone"
                    />
                  </td>
                  <td class="w-[20%]">
                    <div class="flex flex-wrap gap-2">
                      <button
                        v-if="report.parseStatusKey === 'pending'"
                        type="button"
                        class="shell-button"
                        :disabled="pollingReportIds.has(report.id)"
                        @click="handleParseReport(report)"
                      >
                        <FontAwesomeIcon :icon="['fas', 'play']" aria-hidden="true" />
                        <span>解析</span>
                      </button>
                      <button
                        v-if="report.parseStatusKey === 'processing'"
                        type="button"
                        class="shell-button"
                        disabled
                      >
                        <FontAwesomeIcon :icon="['fas', 'spinner']" spin aria-hidden="true" />
                        <span>解析中...</span>
                      </button>
                      <button
                        v-if="report.parseStatusKey === 'failed'"
                        type="button"
                        class="shell-button"
                        :disabled="pollingReportIds.has(report.id)"
                        @click="handleParseReport(report)"
                      >
                        <FontAwesomeIcon :icon="['fas', 'rotate-right']" aria-hidden="true" />
                        <span>重新解析</span>
                      </button>
                      <button
                        v-if="report.parseStatusKey === 'success' && report.vectorStatusKey === 'pending'"
                        type="button"
                        class="shell-button"
                        disabled
                        title="向量化功能待后端实现"
                      >
                        <FontAwesomeIcon :icon="['fas', 'gears']" aria-hidden="true" />
                        <span>向量化（待实现）</span>
                      </button>
                      <button
                        v-if="report.parseStatusKey === 'success'"
                        type="button"
                        class="shell-button-secondary"
                        :disabled="pollingReportIds.has(report.id)"
                        @click="handleParseReport(report, true)"
                      >
                        <FontAwesomeIcon :icon="['fas', 'rotate-right']" aria-hidden="true" />
                        <span>重新解析</span>
                      </button>
                      <RouterLink :to="getViewRoute(report)" class="shell-button-secondary">
                        <FontAwesomeIcon
                          :icon="['fas', 'arrow-up-right-from-square']"
                          aria-hidden="true"
                        />
                        <span>查看</span>
                      </RouterLink>
                      <button
                        type="button"
                        class="shell-button-secondary text-danger hover:bg-danger/10"
                        :disabled="deletingId === report.id"
                        @click="handleDelete(report)"
                      >
                        <FontAwesomeIcon
                          :icon="['fas', 'trash']"
                          aria-hidden="true"
                          :class="{ 'animate-spin': deletingId === report.id }"
                        />
                        <span>{{ deletingId === report.id ? '删除中...' : '删除' }}</span>
                      </button>
                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <div
            class="flex flex-col gap-4 border-t border-black/5 px-5 py-4 text-sm text-ink-500 sm:px-6 lg:flex-row lg:items-center lg:justify-between"
          >
            <p>第 {{ listState.page }} / {{ totalPages }} 页，共 {{ listState.total }} 条记录</p>
            <div class="flex flex-wrap items-center gap-3">
              <div class="flex items-center gap-2">
                <span>每页</span>
                <select
                  v-model="listState.pageSize"
                  class="rounded-lg border border-black/10 bg-white px-3 py-1.5 text-sm focus:border-accent-500 focus:outline-none focus:ring-1 focus:ring-accent-500"
                  @change="changePageSize($event.target.value)"
                >
                  <option v-for="size in PAGE_SIZE_OPTIONS" :key="size" :value="size">
                    {{ size }}
                  </option>
                </select>
                <span>条</span>
              </div>
              <div class="flex items-center gap-2">
                <button
                  type="button"
                  class="shell-button-secondary"
                  :disabled="listState.page <= 1"
                  @click="changePage(listState.page - 1)"
                >
                  上一页
                </button>
                <button
                  type="button"
                  class="shell-button-secondary"
                  :disabled="listState.page >= totalPages"
                  @click="changePage(listState.page + 1)"
                >
                  下一页
                </button>
              </div>
              <div class="flex items-center gap-2">
                <span>跳至</span>
                <input
                  v-model="jumpPageInput"
                  type="number"
                  min="1"
                  :max="totalPages"
                  class="w-16 rounded-lg border border-black/10 px-2 py-1.5 text-center text-sm focus:border-accent-500 focus:outline-none focus:ring-1 focus:ring-accent-500"
                  @keyup.enter="handleJumpPage"
                />
                <span>页</span>
                <button
                  type="button"
                  class="shell-button-secondary"
                  @click="handleJumpPage"
                >
                  跳转
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </SurfacePanel>
    <UploadModal
      :visible="showUploadModal"
      @close="showUploadModal = false"
      @uploaded="handleUploadComplete"
    />
  </div>
</template>
