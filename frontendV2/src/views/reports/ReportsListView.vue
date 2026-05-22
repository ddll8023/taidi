<script setup>
/**
 * 财报记录列表页
 * 功能描述：展示已上传的财报记录列表，支持上传文件、查看详情、删除记录
 * 依赖组件：SurfacePanel, AppEmptyState, UploadReportModal
 */
import { ref, reactive, computed, onMounted } from 'vue'
import BaseButton from '@/components/ui/BaseButton.vue'
import BaseInput from '@/components/ui/BaseInput.vue'
import BaseSelect from '@/components/ui/BaseSelect.vue'
import { getReportList, parseReports, deleteReport } from '@/api/financialReports'
import SurfacePanel from '@/components/ui/SurfacePanel.vue'
import AppEmptyState from '@/components/ui/AppEmptyState.vue'
import UploadReportModal from '@/components/reports/UploadReportModal.vue'

// ── 状态 ──

const listState = reactive({
  items: [],
  page: 1,
  pageSize: 10,
  total: 0
})

// 筛选条件
const keyword = ref('')
const parseStatusFilter = ref('')
const importStatusFilter = ref('')

const parseStatusOptions = [
  { value: '', label: '全部状态' },
  { value: '0', label: '待处理' },
  { value: '1', label: '解析成功' },
  { value: '2', label: '解析失败' },
  { value: '3', label: '解析中' }
]

const importStatusOptions = [
  { value: '', label: '全部入库状态' },
  { value: '0', label: '待入库' },
  { value: '1', label: '已入库' },
  { value: '2', label: '入库失败' }
]

const isLoading = ref(false)
const isRefreshing = ref(false)
const errorMessage = ref('')
const showUploadModal = ref(false)
const parsingIds = ref(new Set())
const isParsing = ref(false)
const notice = ref({ type: '', message: '' })

// ── 计算属性 ──

const hasRecords = computed(() => listState.items.length > 0)

const totalPages = computed(() => {
  const pageSize = Number(listState.pageSize) || 10
  const total = Number(listState.total) || listState.items.length
  return Math.max(1, Math.ceil(total / pageSize))
})

const pageSummary = computed(() => `当前页 ${listState.items.length} 条记录`)

const noticeClass = computed(() => {
  const classMap = {
    error: 'border-transparent bg-red-50/80 text-danger',
    info: 'border-accent-200 bg-accent-50/80 text-accent-700',
    warning: 'border-transparent bg-yellow-50/80 text-warning'
  }
  return classMap[notice.value.type] || 'border-ink-200 bg-white text-ink-700'
})

// ── 工具函数 ──

const formatDateTime = (value) => {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  }).format(date)
}

// ── 状态映射 ──

const parseStatusMap = {
  0: { label: '待处理', tone: 'warning' },
  1: { label: '解析成功', tone: 'success' },
  2: { label: '解析失败', tone: 'danger' },
  3: { label: '解析中', tone: 'accent' }
}

const importStatusMap = {
  0: { label: '待入库', tone: 'warning' },
  1: { label: '已入库', tone: 'success' },
  2: { label: '入库失败', tone: 'danger' }
}

const getStatusMeta = (value, statusMap) => {
  if (value === undefined || value === null) return { label: '-', tone: 'neutral' }
  const num = Number(value)
  return statusMap[num] || { label: String(value), tone: 'neutral' }
}

// ── 数据请求 ──

const fetchReports = async ({ silent = false } = {}) => {
  if (silent) {
    isRefreshing.value = true
  } else {
    isLoading.value = true
    errorMessage.value = ''
  }

  try {
    const params = {
      page: listState.page,
      page_size: listState.pageSize
    }
    if (keyword.value.trim()) params.keyword = keyword.value.trim()
    if (parseStatusFilter.value !== '') params.parse_status = Number(parseStatusFilter.value)
    if (importStatusFilter.value !== '') params.import_status = Number(importStatusFilter.value)

    const response = await getReportList(params)
    const payload = response?.data || response
    const items = payload?.lists || payload?.items || payload?.data || []
    const pagination = payload?.pagination || {}
    listState.items = items.map(normalizeItem)
    listState.total = pagination?.total || items.length
    errorMessage.value = ''
  } catch (error) {
    errorMessage.value = error.message || '加载记录列表失败'
  } finally {
    isLoading.value = false
    isRefreshing.value = false
  }
}

const normalizeItem = (item) => {
  const parseMeta = getStatusMeta(item.parse_status, parseStatusMap)
  const importMeta = getStatusMeta(item.import_status, importStatusMap)

  return {
    id: item.id || item.report_id,
    fileName: item.source_file_name || item.file_name || item.filename || '未命名文件',
    reportTitle: item.report_title || item.reportTitle || '暂无标题',
    stockCode: item.stock_code || item.stockCode || '',
    stockAbbr: item.stock_abbr || item.stockAbbr || '',
    parseStatus: parseMeta,
    importStatus: importMeta,
    uploadedAt: formatDateTime(item.created_at || item.createdAt || item.uploaded_at)
  }
}

const refreshReports = async () => {
  notice.value = { type: '', message: '' }
  try {
    await fetchReports({ silent: true })
  } catch (error) {
    notice.value = { type: 'error', message: error.message || '刷新失败' }
  }
}

const setNotice = (type, message) => {
  notice.value = { type, message }
}

const clearNotice = () => {
  notice.value = { type: '', message: '' }
}

// ── 事件处理 ──

const handleSearch = async () => {
  listState.page = 1
  await fetchReports()
}

const resetFilters = async () => {
  keyword.value = ''
  parseStatusFilter.value = ''
  importStatusFilter.value = ''
  listState.page = 1
  await fetchReports()
}

const handleKeydown = (event) => {
  if (event.key === 'Enter') handleSearch()
}

const handleParseReport = async (reportId, force = false) => {
  parsingIds.value.add(reportId)
  try {
    const result = await parseReports([reportId])
    setNotice('info', `财报 ${reportId} ${force ? '重新' : ''}解析任务已提交`)
  } catch (error) {
    setNotice('error', error.message || '提交解析失败')
  } finally {
    parsingIds.value.delete(reportId)
    await fetchReports({ silent: true })
  }
}

const handleParseAllPending = async () => {
  const pendingIds = listState.items
    .filter((item) => item.parseStatus.tone === 'warning')
    .map((item) => item.id)
  if (pendingIds.length === 0) {
    setNotice('warning', '没有待解析的记录')
    return
  }
  isParsing.value = true
  try {
    const result = await parseReports(pendingIds)
    setNotice('info', `已提交 ${result.start_parse_count || pendingIds.length} 个解析任务`)
  } catch (error) {
    setNotice('error', error.message || '批量提交解析失败')
  } finally {
    isParsing.value = false
    await fetchReports({ silent: true })
  }
}

const handleDeleteReport = async (reportId) => {
  if (!confirm('确定要删除这条财报记录吗？此操作不可撤销。')) return
  try {
    await deleteReport(reportId)
    setNotice('info', `财报 ${reportId} 已删除`)
    await fetchReports({ silent: true })
  } catch (error) {
    setNotice('error', error.message || '删除失败')
  }
}

const handleUploadComplete = async () => {
  showUploadModal.value = false
  clearNotice()
  await fetchReports({ silent: true })
  setNotice('info', '文件上传完成，列表已更新')
}

onMounted(async () => {
  await fetchReports()
})
</script>

<template>
  <div class="h-full space-y-6">
    <SurfacePanel :padded="false">
      <!-- 页头 -->
      <div class="border-b border-black/5 px-5 py-5 sm:px-6">
        <div class="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
          <div>
            <p class="shell-kicker">Records</p>
            <h2 class="mt-2 text-xl font-semibold text-ink-900">财报记录列表</h2>
            <p class="mt-2 max-w-3xl text-sm leading-6 text-ink-600">
              上传财报 PDF 文件后，记录会出现在这里并展示解析状态。
            </p>
            <p class="mt-4 text-sm text-ink-500">{{ pageSummary }}</p>
          </div>

          <div class="flex flex-wrap items-center gap-3">
            <BaseButton icon="cloud-arrow-up" @click="showUploadModal = true">上传文件</BaseButton>
            <BaseButton variant="secondary" icon="microchip" :loading="isParsing" :disabled="isRefreshing || isLoading" @click="handleParseAllPending">{{ isParsing ? '提交中...' : '一键解析' }}</BaseButton>
            <BaseButton variant="secondary" icon="rotate-right" :loading="isRefreshing" :disabled="isLoading" @click="refreshReports">{{ isRefreshing ? '刷新中...' : '刷新列表' }}</BaseButton>
          </div>
        </div>

        <!-- 通知条 -->
        <div
          v-if="notice.message"
          class="mt-4 rounded-2xl border px-4 py-3 text-sm"
          :class="noticeClass"
        >
          {{ notice.message }}
        </div>

        <!-- 筛选区域 -->
        <div class="mt-4 flex flex-wrap items-center gap-3">
          <!-- 关键词搜索 -->
          <BaseInput
            v-model="keyword"
            icon="search"
            placeholder="搜索报告标题..."
            clearable
            @keydown="handleKeydown"
          />

          <!-- 解析状态筛选 -->
          <BaseSelect
            v-model="parseStatusFilter"
            :options="parseStatusOptions"
            placeholder="全部状态"
          />

          <!-- 入库状态筛选 -->
          <BaseSelect
            v-model="importStatusFilter"
            :options="importStatusOptions"
            placeholder="全部入库状态"
          />

          <BaseButton icon="search" size="sm" @click="handleSearch">筛选</BaseButton>
          <BaseButton variant="ghost" size="sm" @click="resetFilters">重置</BaseButton>
        </div>
      </div>

      <!-- 内容区 -->
      <div class="p-5 sm:p-6">
        <!-- 加载中 -->
        <div
          v-if="isLoading && !hasRecords"
          class="flex flex-col items-center justify-center py-16"
        >
          <FontAwesomeIcon
            :icon="['fas', 'spinner']"
            spin
            class="text-3xl text-accent-500"
            aria-hidden="true"
          />
          <p class="mt-4 text-sm text-ink-500">正在加载记录列表...</p>
        </div>

        <!-- 错误状态 -->
        <div
          v-else-if="errorMessage && !hasRecords"
          class="flex flex-col items-center justify-center py-16"
        >
          <p class="text-sm text-danger">{{ errorMessage }}</p>
          <BaseButton variant="secondary" @click="fetchReports()">重试</BaseButton>
        </div>

        <!-- 空状态 -->
        <AppEmptyState
          v-else-if="!hasRecords"
          title="当前没有记录"
          description="上传财报 PDF 文件后，记录会出现在这里并展示解析状态。"
        />

        <!-- 数据表格 -->
        <div
          v-else
          class="flex flex-col overflow-hidden rounded-[28px] border border-black/5 bg-white"
          style="height: 560px"
        >
          <div class="min-h-0 flex-1 overflow-auto">
            <table class="shell-grid-table min-w-[800px]">
              <thead class="sticky top-0 z-10">
                <tr>
                  <th class="w-[18%] text-left">文件信息</th>
                  <th class="w-[12%] !text-center">股票代码</th>
                  <th class="w-[14%] text-left">报告标题</th>
                  <th class="w-[10%] !text-center">解析状态</th>
                  <th class="w-[10%] !text-center">入库状态</th>
                  <th class="w-[12%] !text-center">上传时间</th>
                  <th class="w-[24%] !text-center">操作</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="report in listState.items" :key="report.id">
                  <td>
                    <div class="flex items-center gap-2">
                      <div class="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-red-50 text-red-500">
                        <FontAwesomeIcon :icon="['fas', 'file-pdf']" class="text-xs" aria-hidden="true" />
                      </div>
                      <p class="truncate text-sm font-medium text-ink-900">{{ report.fileName }}</p>
                    </div>
                  </td>
                  <td class="text-center">
                    <p class="font-mono text-sm text-ink-900">{{ report.stockCode || '-' }}</p>
                    <p v-if="report.stockAbbr" class="mt-0.5 text-xs text-ink-500">{{ report.stockAbbr }}</p>
                  </td>
                  <td>
                    <p class="text-sm leading-6 text-ink-700">{{ report.reportTitle }}</p>
                  </td>
                  <td class="text-center">
                    <div class="inline-flex items-center space-y-1.5">
                      <span
                        class="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium"
                        :class="{
                          'bg-yellow-50 text-yellow-700': report.parseStatus.tone === 'warning',
                          'bg-green-50 text-green-700': report.parseStatus.tone === 'success',
                          'bg-red-50 text-red-700': report.parseStatus.tone === 'danger',
                          'bg-blue-50 text-blue-700': report.parseStatus.tone === 'accent',
                          'bg-ink-50 text-ink-600': report.parseStatus.tone === 'neutral'
                        }"
                      >
                        {{ report.parseStatus.label }}
                      </span>
                    </div>
                  </td>
                  <td class="text-center">
                    <div class="inline-flex items-center space-y-1.5">
                      <span
                        class="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium"
                        :class="{
                          'bg-yellow-50 text-yellow-700': report.importStatus.tone === 'warning',
                          'bg-green-50 text-green-700': report.importStatus.tone === 'success',
                          'bg-red-50 text-red-700': report.importStatus.tone === 'danger',
                          'bg-ink-50 text-ink-600': report.importStatus.tone === 'neutral'
                        }"
                      >
                        {{ report.importStatus.label }}
                      </span>
                    </div>
                  </td>
                  <td class="text-center">
                    <p class="text-sm text-ink-500">{{ report.uploadedAt }}</p>
                  </td>
                  <td class="text-center">
                    <div class="inline-flex items-center gap-2 flex-nowrap">
                      <!-- 解析中：加载动画 -->
                      <BaseButton v-if="report.parseStatus.tone === 'accent'" :loading="true" disabled>解析中...</BaseButton>
                      <!-- 待处理：正常解析 -->
                      <BaseButton v-else-if="report.parseStatus.tone === 'warning'" icon="play" :disabled="parsingIds.has(report.id)" @click="handleParseReport(report.id)">解析</BaseButton>
                      <!-- 解析失败：重新解析 -->
                      <BaseButton v-else-if="report.parseStatus.tone === 'danger'" icon="rotate-right" :disabled="parsingIds.has(report.id)" @click="handleParseReport(report.id)">重新解析</BaseButton>
                      <!-- 解析成功：强制重新解析 -->
                      <BaseButton v-else variant="secondary" icon="rotate-right" :disabled="parsingIds.has(report.id)" @click="handleParseReport(report.id, true)">重新解析</BaseButton>
                      <!-- 查看详情 -->
                      <RouterLink
                        :to="`/reports/detail/${report.id}`"
                        class="inline-flex items-center gap-1 rounded-lg border border-black/10 px-2.5 py-1.5 text-xs font-medium text-ink-600 hover:bg-accent-50 hover:text-accent-700 hover:border-accent-200 transition-colors"
                      >
                        <FontAwesomeIcon :icon="['fas', 'arrow-up-right-from-square']" aria-hidden="true" />
                        <span>详情</span>
                      </RouterLink>
                      <!-- 删除 -->
                      <BaseButton variant="ghost" icon="trash" size="xs" class="rounded-lg border border-black/10 px-2 text-danger hover:bg-red-50 hover:border-red-200" @click="handleDeleteReport(report.id)">删除</BaseButton>
                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <!-- 分页 -->
          <div
            class="flex flex-col gap-4 border-t border-black/5 px-5 py-4 text-sm text-ink-500 sm:flex-row sm:items-center sm:justify-between"
          >
            <p>第 {{ listState.page }} / {{ totalPages }} 页，共 {{ listState.total }} 条</p>
            <div class="flex items-center gap-2">
              <BaseButton variant="secondary" size="sm" :disabled="listState.page <= 1" @click="listState.page--; fetchReports()">上一页</BaseButton>
              <BaseButton variant="secondary" size="sm" :disabled="listState.page >= totalPages" @click="listState.page++; fetchReports()">下一页</BaseButton>
            </div>
          </div>
        </div>
      </div>
    </SurfacePanel>

    <!-- 上传弹窗 -->
    <UploadReportModal
      :visible="showUploadModal"
      @close="showUploadModal = false"
      @uploaded="handleUploadComplete"
    />
  </div>
</template>
