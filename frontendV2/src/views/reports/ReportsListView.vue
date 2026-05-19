<script setup>
/**
 * 财报记录列表页
 * 功能描述：展示已上传的财报记录列表，支持上传文件、查看详情、删除记录
 * 依赖组件：SurfacePanel, AppEmptyState, UploadReportModal
 */
import { ref, reactive, computed, onMounted } from 'vue'
import { getReportList } from '@/api/financialReports'
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

const isLoading = ref(false)
const isRefreshing = ref(false)
const errorMessage = ref('')
const showUploadModal = ref(false)
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
  2: { label: '解析失败', tone: 'danger' }
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
    const response = await getReportList({
      page: listState.page,
      page_size: listState.pageSize
    })
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
          <button
            type="button"
            class="shell-button-secondary mt-4"
            @click="fetchReports()"
          >
            重试
          </button>
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
                  <th class="w-[28%]">文件信息</th>
                  <th class="w-[22%]">股票代码</th>
                  <th class="w-[22%]">报告标题</th>
                  <th class="w-[16%]">解析状态</th>
                  <th class="w-[12%]">上传时间</th>
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
                  <td>
                    <p class="font-mono text-sm text-ink-900">{{ report.stockCode || '-' }}</p>
                    <p v-if="report.stockAbbr" class="mt-0.5 text-xs text-ink-500">{{ report.stockAbbr }}</p>
                  </td>
                  <td>
                    <p class="text-sm leading-6 text-ink-700">{{ report.reportTitle }}</p>
                  </td>
                  <td>
                    <div class="space-y-1.5">
                      <span
                        class="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium"
                        :class="{
                          'bg-yellow-50 text-yellow-700': report.parseStatus.tone === 'warning',
                          'bg-green-50 text-green-700': report.parseStatus.tone === 'success',
                          'bg-red-50 text-red-700': report.parseStatus.tone === 'danger',
                          'bg-ink-50 text-ink-600': report.parseStatus.tone === 'neutral'
                        }"
                      >
                        {{ report.parseStatus.label }}
                      </span>
                    </div>
                  </td>
                  <td>
                    <p class="text-sm text-ink-500">{{ report.uploadedAt }}</p>
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
              <button
                type="button"
                class="shell-button-secondary"
                :disabled="listState.page <= 1"
                @click="listState.page--; fetchReports()"
              >
                上一页
              </button>
              <button
                type="button"
                class="shell-button-secondary"
                :disabled="listState.page >= totalPages"
                @click="listState.page++; fetchReports()"
              >
                下一页
              </button>
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
