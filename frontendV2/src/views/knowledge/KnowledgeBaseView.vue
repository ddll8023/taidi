<script setup>
/**
 * 知识库管理页面
 * 功能描述：系统初始化（研报元数据导入）+ 状态查询/文档管理
 * 依赖组件：SurfacePanel, BaseButton, BaseSelect, AppEmptyState
 */
import { ref, computed, onMounted } from 'vue'
import BaseButton from '@/components/ui/BaseButton.vue'
import BaseSelect from '@/components/ui/BaseSelect.vue'
import AppEmptyState from '@/components/ui/AppEmptyState.vue'
import { initKnowledgeBase, getInitStatus } from '@/api/knowledgeBase'

// ── 常量 ──

const DOC_TYPE_OPTIONS = [
  { value: 'RESEARCH_REPORT', label: '个股研报' },
  { value: 'INDUSTRY_REPORT', label: '行业研报' }
]

const docTypeLabelMap = {
  RESEARCH_REPORT: '个股研报',
  INDUSTRY_REPORT: '行业研报'
}

// ── 状态 ──

const fileInput = ref(null)
const selectedDocType = ref('RESEARCH_REPORT')

const isUploading = ref(false)
const uploadMessage = ref({ type: '', text: '' })
const importResult = ref(null)

const isStatusLoading = ref(false)
const statusData = ref(null)
const isRefreshing = ref(false)

// 筛选
const filterDocType = ref('')
const filterStatus = ref('')

// 选中的文档（空列表占位）
const selectedIds = ref([])
const documents = ref([])
const total = ref(0)
const currentPage = ref(1)
const pageSize = ref(20)
const loading = ref(false)

// ── 计算属性 ──

const isInitialized = computed(() => statusData.value?.initialized ?? false)
const totalMetadataCount = computed(() => statusData.value?.total_metadata_count ?? 0)
const stockMetadataCount = computed(() => statusData.value?.stock_metadata_count ?? 0)
const industryMetadataCount = computed(() => statusData.value?.industry_metadata_count ?? 0)

const noticeClass = computed(() => {
  if (!uploadMessage.value.text) return ''
  return uploadMessage.value.type === 'success'
    ? 'border-green-200 bg-green-50/80 text-green-700'
    : 'border-red-200 bg-red-50/80 text-red-700'
})

const totalPages = computed(() => Math.ceil(total.value / pageSize.value) || 1)

// ── 数据请求 ──

const loadInitStatus = async ({ silent = false } = {}) => {
  if (silent) {
    isRefreshing.value = true
  } else {
    isStatusLoading.value = true
  }
  try {
    const result = await getInitStatus()
    statusData.value = result.data
  } catch {
    statusData.value = null
  } finally {
    isStatusLoading.value = false
    isRefreshing.value = false
  }
}

// ── 事件处理 ──

const triggerFileInput = () => {
  fileInput.value?.click()
}

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
  } catch (error) {
    uploadMessage.value = { type: 'error', text: error.message || '导入失败' }
  } finally {
    isUploading.value = false
  }
}

const handleFilterChange = () => {
  currentPage.value = 1
}

const handlePageChange = (page) => {
  currentPage.value = page
}

const getDocTypeLabel = (type) => docTypeLabelMap[type] || type || '-'

onMounted(() => {
  loadInitStatus()
})
</script>

<template>
  <div class="flex flex-col gap-4 h-full overflow-y-auto">
    <!-- ═══ 页头 ═══ -->
    <div class="flex items-center justify-between shrink-0 rounded-2xl border border-black/5 bg-white/80 p-4">
      <div>
        <h2 class="text-lg font-semibold text-ink-900">知识库管理</h2>
        <p class="mt-1 text-sm text-ink-500">系统初始化 → 研报元数据导入</p>
      </div>
      <div class="flex items-center gap-2">
        <span
          v-if="isInitialized"
          class="inline-flex items-center gap-1.5 rounded-full bg-green-50 px-3 py-1 text-xs font-medium text-green-700 border border-green-200"
        >
          <FontAwesomeIcon :icon="['fas', 'check-circle']" aria-hidden="true" />
          已初始化（{{ totalMetadataCount }} 条）
        </span>
        <span
          v-else-if="statusData"
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
          :loading="isRefreshing"
          class="h-9 w-9 !p-0"
          @click="loadInitStatus({ silent: true })"
        />
      </div>
    </div>

    <!-- ═══ 统计卡片 ═══ -->
    <div class="grid grid-cols-1 gap-4 sm:grid-cols-3 shrink-0">
      <article class="rounded-2xl border border-black/5 bg-white p-5 shadow-sm">
        <p class="text-sm font-medium text-ink-500">个股研报</p>
        <p class="mt-2 text-3xl font-semibold tracking-tight text-ink-900">{{ stockMetadataCount }}</p>
        <p class="mt-1 text-xs text-ink-400">条元数据</p>
      </article>
      <article class="rounded-2xl border border-black/5 bg-white p-5 shadow-sm">
        <p class="text-sm font-medium text-ink-500">行业研报</p>
        <p class="mt-2 text-3xl font-semibold tracking-tight text-ink-900">{{ industryMetadataCount }}</p>
        <p class="mt-1 text-xs text-ink-400">条元数据</p>
      </article>
      <article class="rounded-2xl border border-black/5 bg-gradient-to-br from-accent-500 to-accent-600 p-5 shadow-sm">
        <p class="text-sm font-medium text-white/80">合计</p>
        <p class="mt-2 text-3xl font-semibold tracking-tight text-white">{{ totalMetadataCount }}</p>
        <p class="mt-1 text-xs text-white/60">条元数据</p>
      </article>
    </div>

    <!-- ═══ 操作按钮栏 ═══ -->
    <div class="flex flex-wrap items-center gap-3 shrink-0 rounded-2xl border border-black/5 bg-white/80 p-3">
      <BaseSelect
        v-model="selectedDocType"
        :options="DOC_TYPE_OPTIONS"
        size="sm"
      />
      <BaseButton icon="file-excel" :loading="isUploading" @click="triggerFileInput">
        {{ isUploading ? '导入中...' : '选择Excel文件' }}
      </BaseButton>
      <input
        ref="fileInput"
        type="file"
        accept=".xlsx,.xls"
        class="hidden"
        @change="handleFileChange"
      />

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

    <!-- ═══ 筛选栏 ═══ -->
    <div class="flex flex-wrap items-center gap-3 shrink-0 rounded-xl border border-black/5 bg-slate-50 p-3">
      <span class="text-sm text-ink-500">筛选：</span>
      <BaseSelect
        v-model="filterDocType"
        :options="[
          { value: '', label: '全部类型' },
          { value: 'RESEARCH_REPORT', label: '个股研报' },
          { value: 'INDUSTRY_REPORT', label: '行业研报' }
        ]"
        placeholder="全部类型"
        size="sm"
        @change="handleFilterChange"
      />
      <BaseSelect
        v-model="filterStatus"
        :options="[
          { value: '', label: '全部状态' },
          { value: 'loaded', label: '已加载' },
          { value: 'pending', label: '待处理' }
        ]"
        placeholder="全部状态"
        size="sm"
        @change="handleFilterChange"
      />
    </div>

    <!-- ═══ 文档表格 ═══ -->
    <div class="flex flex-col rounded-2xl border border-black/5 bg-white/80 overflow-hidden" style="height: 480px;">
      <!-- 加载中 -->
      <div v-if="loading" class="flex items-center justify-center py-16">
        <FontAwesomeIcon :icon="['fas', 'spinner']" spin class="text-2xl text-ink-400" aria-hidden="true" />
      </div>

      <!-- 表格 -->
      <div v-else class="overflow-x-auto overflow-y-auto h-full">
        <table class="w-full text-sm">
          <thead class="border-b border-black/5 bg-slate-50 sticky top-0">
            <tr>
              <th class="w-10 px-4 py-3 text-left font-semibold text-ink-600">
                <input type="checkbox" class="rounded border-ink-300" disabled />
              </th>
              <th class="px-4 py-3 text-left font-semibold text-ink-600">ID</th>
              <th class="px-4 py-3 text-left font-semibold text-ink-600">标题</th>
              <th class="px-4 py-3 text-left font-semibold text-ink-600">文档类型</th>
              <th class="px-4 py-3 text-left font-semibold text-ink-600">股票代码</th>
              <th class="px-4 py-3 text-center font-semibold text-ink-600">状态</th>
              <th class="px-4 py-3 text-left font-semibold text-ink-600">操作</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-black/5">
            <!-- 空状态 -->
            <tr v-if="!isInitialized">
              <td colspan="7" class="px-4 py-16">
                <div class="flex flex-col items-center justify-center text-center">
                  <FontAwesomeIcon :icon="['fas', 'database']" class="text-3xl text-ink-200 mb-3" aria-hidden="true" />
                  <p class="text-sm font-medium text-ink-500">尚未初始化</p>
                  <p class="mt-1 text-xs text-ink-400">请上传 Excel 文件完成系统初始化</p>
                </div>
              </td>
            </tr>
            <tr v-else-if="isInitialized">
              <td colspan="7" class="px-4 py-16">
                <div class="flex flex-col items-center justify-center text-center">
                  <FontAwesomeIcon :icon="['fas', 'check-circle']" class="text-3xl text-green-300 mb-3" aria-hidden="true" />
                  <p class="text-sm font-medium text-ink-500">初始化已完成</p>
                  <p class="mt-1 text-xs text-ink-400">
                    已加载 {{ totalMetadataCount }} 条元数据（个股 {{ stockMetadataCount }} 条，行业 {{ industryMetadataCount }} 条）
                  </p>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- 分页 -->
      <div class="flex items-center justify-between border-t border-black/5 px-4 py-3 text-sm text-ink-500">
        <p>暂无文档列表 — 共 {{ totalMetadataCount }} 条元数据</p>
        <div class="flex items-center gap-2">
          <BaseButton variant="secondary" size="sm" disabled>上一页</BaseButton>
          <BaseButton variant="secondary" size="sm" disabled>下一页</BaseButton>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.hidden {
  display: none;
}
</style>