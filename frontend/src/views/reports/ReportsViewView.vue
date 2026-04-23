<script setup>
/**
 * 查看页
 * 功能描述：查看财报详情，包括基本信息、四张财务报表的结构化数据和JSON文件查看
 * 依赖组件：SurfacePanel、AppEmptyState、AppLoadingState、AppErrorState、StatusBadge
 */
import { computed, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { getFinancialReportDetail, getJsonFileContent } from '@/api/financialReports'
import AppEmptyState from '@/components/common/AppEmptyState.vue'
import AppErrorState from '@/components/common/AppErrorState.vue'
import AppLoadingState from '@/components/common/AppLoadingState.vue'
import StatusBadge from '@/components/common/StatusBadge.vue'
import SurfacePanel from '@/components/common/SurfacePanel.vue'

const route = useRoute()
const router = useRouter()

const reportId = computed(() => route.params.reportId)

const isLoading = ref(false)
const errorMessage = ref('')
const reportDetail = ref(null)

const jsonContent = ref(null)
const jsonLoading = ref(false)
const jsonError = ref('')

const hasReport = computed(() => reportDetail.value !== null)

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

const formatNumber = (value, unit = '') => {
  if (value === null || value === undefined) return '-'
  const num = Number(value)
  if (Number.isNaN(num)) return '-'
  const formatted = num.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 4 })
  return `${formatted}${unit}`
}

const formatPercent = (value) => {
  if (value === null || value === undefined) return '-'
  const num = Number(value)
  if (Number.isNaN(num)) return '-'
  return `${num.toFixed(2)}%`
}

const formatReportPeriod = (value) => {
  const periodMap = {
    Q1: '一季度报告',
    HY: '半年度报告',
    Q3: '三季度报告',
    FY: '年度报告'
  }
  return periodMap[value] || value
}

const formatReportType = (value) => {
  const typeMap = {
    REPORT: '正式报告',
    SUMMARY: '报告摘要'
  }
  return typeMap[value] || value
}

const formatExchange = (value) => {
  const exchangeMap = {
    SH: '上海证券交易所',
    SZ: '深圳证券交易所',
    BJ: '北京证券交易所'
  }
  return exchangeMap[value] || value
}

const resolveParseStatusMeta = (value) => {
  const statusMap = {
    0: { label: '待处理', tone: 'warning' },
    1: { label: '解析成功', tone: 'success' },
    2: { label: '解析失败', tone: 'danger' }
  }
  return statusMap[value] || { label: '未知', tone: 'neutral' }
}

const resolveReviewStatusMeta = (value) => {
  const statusMap = {
    0: { label: '待审核', tone: 'warning' },
    1: { label: '已通过', tone: 'success' },
    2: { label: '已驳回', tone: 'danger' }
  }
  return statusMap[value] || { label: '未知', tone: 'neutral' }
}

const resolveValidateStatusMeta = (value) => {
  const statusMap = {
    0: { label: '待校验', tone: 'warning' },
    1: { label: '已通过', tone: 'success' },
    2: { label: '已失败', tone: 'danger' }
  }
  return statusMap[value] || { label: '未知', tone: 'neutral' }
}

const resolveImportStatusMeta = (value) => {
  const statusMap = {
    0: { label: '待入库', tone: 'warning' },
    1: { label: '入库成功', tone: 'success' },
    2: { label: '入库失败', tone: 'danger' }
  }
  return statusMap[value] || { label: '未知', tone: 'neutral' }
}

const resolveVectorStatusMeta = (value) => {
  const statusMap = {
    0: { label: '待向量化', tone: 'warning' },
    1: { label: '向量化中', tone: 'accent' },
    2: { label: '已完成', tone: 'success' },
    3: { label: '失败', tone: 'danger' },
    4: { label: '已跳过', tone: 'neutral' }
  }
  return statusMap[value] || { label: '未知', tone: 'neutral' }
}

const fetchReportDetail = async () => {
  if (!reportId.value) {
    errorMessage.value = '缺少报告ID参数'
    return
  }

  isLoading.value = true
  errorMessage.value = ''

  try {
    const response = await getFinancialReportDetail(reportId.value)
    reportDetail.value = response.data || response
  } catch (error) {
    errorMessage.value = error.message || '加载财报详情失败，请稍后重试。'
  } finally {
    isLoading.value = false
  }
}

const goBack = () => {
  router.push('/reports/list')
}

const corePerformanceFields = computed(() => {
  if (!reportDetail.value?.core_performance_indicators) return []
  const data = reportDetail.value.core_performance_indicators
  return [
    { label: '每股收益', value: formatNumber(data.eps, '元') },
    { label: '营业总收入', value: formatNumber(data.total_operating_revenue, '万元') },
    { label: '营业总收入同比增长', value: formatPercent(data.operating_revenue_yoy_growth) },
    { label: '营业总收入季度环比增长', value: formatPercent(data.operating_revenue_qoq_growth) },
    { label: '净利润', value: formatNumber(data.net_profit_10k_yuan, '万元') },
    { label: '净利润同比增长', value: formatPercent(data.net_profit_yoy_growth) },
    { label: '净利润季度环比增长', value: formatPercent(data.net_profit_qoq_growth) },
    { label: '每股净资产', value: formatNumber(data.net_asset_per_share, '元') },
    { label: '净资产收益率', value: formatPercent(data.roe) },
    { label: '每股经营现金流量', value: formatNumber(data.operating_cf_per_share, '元') },
    { label: '扣非净利润', value: formatNumber(data.net_profit_excl_non_recurring, '万元') },
    { label: '扣非净利润同比增长', value: formatPercent(data.net_profit_excl_non_recurring_yoy) },
    { label: '销售毛利率', value: formatPercent(data.gross_profit_margin) },
    { label: '销售净利率', value: formatPercent(data.net_profit_margin) },
    { label: '加权平均净资产收益率（扣非）', value: formatPercent(data.roe_weighted_excl_non_recurring) }
  ]
})

const balanceSheetFields = computed(() => {
  if (!reportDetail.value?.balance_sheet) return []
  const data = reportDetail.value.balance_sheet
  return [
    { label: '货币资金', value: formatNumber(data.asset_cash_and_cash_equivalents, '万元') },
    { label: '应收账款', value: formatNumber(data.asset_accounts_receivable, '万元') },
    { label: '存货', value: formatNumber(data.asset_inventory, '万元') },
    { label: '交易性金融资产', value: formatNumber(data.asset_trading_financial_assets, '万元') },
    { label: '在建工程', value: formatNumber(data.asset_construction_in_progress, '万元') },
    { label: '总资产', value: formatNumber(data.asset_total_assets, '万元') },
    { label: '总资产同比', value: formatPercent(data.asset_total_assets_yoy_growth) },
    { label: '应付账款', value: formatNumber(data.liability_accounts_payable, '万元') },
    { label: '预收账款', value: formatNumber(data.liability_advance_from_customers, '万元') },
    { label: '总负债', value: formatNumber(data.liability_total_liabilities, '万元') },
    { label: '总负债同比', value: formatPercent(data.liability_total_liabilities_yoy_growth) },
    { label: '合同负债', value: formatNumber(data.liability_contract_liabilities, '万元') },
    { label: '短期借款', value: formatNumber(data.liability_short_term_loans, '万元') },
    { label: '资产负债率', value: formatPercent(data.asset_liability_ratio) },
    { label: '未分配利润', value: formatNumber(data.equity_unappropriated_profit, '万元') },
    { label: '股东权益合计', value: formatNumber(data.equity_total_equity, '万元') }
  ]
})

const cashFlowFields = computed(() => {
  if (!reportDetail.value?.cash_flow_sheet) return []
  const data = reportDetail.value.cash_flow_sheet
  return [
    { label: '净现金流', value: formatNumber(data.net_cash_flow, '元') },
    { label: '净现金流同比增长', value: formatPercent(data.net_cash_flow_yoy_growth) },
    { label: '经营性现金流净额', value: formatNumber(data.operating_cf_net_amount, '万元') },
    { label: '经营性现金流净现金流占比', value: formatPercent(data.operating_cf_ratio_of_net_cf) },
    { label: '销售商品收到的现金', value: formatNumber(data.operating_cf_cash_from_sales, '万元') },
    { label: '投资性现金流净额', value: formatNumber(data.investing_cf_net_amount, '万元') },
    { label: '投资性现金流净现金流占比', value: formatPercent(data.investing_cf_ratio_of_net_cf) },
    { label: '投资支付的现金', value: formatNumber(data.investing_cf_cash_for_investments, '万元') },
    { label: '收回投资收到的现金', value: formatNumber(data.investing_cf_cash_from_investment_recovery, '万元') },
    { label: '取得借款收到的现金', value: formatNumber(data.financing_cf_cash_from_borrowing, '万元') },
    { label: '偿还债务支付的现金', value: formatNumber(data.financing_cf_cash_for_debt_repayment, '万元') },
    { label: '融资性现金流净额', value: formatNumber(data.financing_cf_net_amount, '万元') },
    { label: '融资性现金流净现金流占比', value: formatPercent(data.financing_cf_ratio_of_net_cf) }
  ]
})

const incomeSheetFields = computed(() => {
  if (!reportDetail.value?.income_sheet) return []
  const data = reportDetail.value.income_sheet
  return [
    { label: '净利润', value: formatNumber(data.net_profit, '万元') },
    { label: '净利润同比', value: formatPercent(data.net_profit_yoy_growth) },
    { label: '其他收益', value: formatNumber(data.other_income, '万元') },
    { label: '营业总收入', value: formatNumber(data.total_operating_revenue, '万元') },
    { label: '营业总收入同比', value: formatPercent(data.operating_revenue_yoy_growth) },
    { label: '营业支出', value: formatNumber(data.operating_expense_cost_of_sales, '万元') },
    { label: '销售费用', value: formatNumber(data.operating_expense_selling_expenses, '万元') },
    { label: '管理费用', value: formatNumber(data.operating_expense_administrative_expenses, '万元') },
    { label: '财务费用', value: formatNumber(data.operating_expense_financial_expenses, '万元') },
    { label: '研发费用', value: formatNumber(data.operating_expense_rnd_expenses, '万元') },
    { label: '税金及附加', value: formatNumber(data.operating_expense_taxes_and_surcharges, '万元') },
    { label: '营业总支出', value: formatNumber(data.total_operating_expenses, '万元') },
    { label: '营业利润', value: formatNumber(data.operating_profit, '万元') },
    { label: '利润总额', value: formatNumber(data.total_profit, '万元') },
    { label: '资产减值损失', value: formatNumber(data.asset_impairment_loss, '万元') },
    { label: '信用减值损失', value: formatNumber(data.credit_impairment_loss, '万元') }
  ]
})

const hasStructuredData = computed(() => {
  return (
    reportDetail.value?.core_performance_indicators ||
    reportDetail.value?.balance_sheet ||
    reportDetail.value?.cash_flow_sheet ||
    reportDetail.value?.income_sheet
  )
})

const fetchJsonContent = async () => {
  if (!reportId.value) return
  jsonLoading.value = true
  jsonError.value = ''
  try {
    const response = await getJsonFileContent(reportId.value)
    jsonContent.value = response.data || response
  } catch (error) {
    jsonError.value = error.message || '获取JSON文件内容失败'
  } finally {
    jsonLoading.value = false
  }
}

const copyJsonContent = async () => {
  if (!jsonContent.value?.content) return
  try {
    await navigator.clipboard.writeText(JSON.stringify(jsonContent.value.content, null, 2))
    alert('JSON 内容已复制到剪贴板')
  } catch {
    alert('复制失败，请手动复制')
  }
}

onMounted(() => {
  fetchReportDetail()
  fetchJsonContent()
})
</script>

<template>
  <div class="space-y-6">
    <AppLoadingState
      v-if="isLoading"
      title="正在加载财报详情"
      description="正在获取财报详细信息，请稍候..."
    />

    <AppErrorState
      v-else-if="errorMessage && !hasReport"
      title="财报详情加载失败"
      :description="errorMessage"
      @retry="fetchReportDetail"
    />

    <template v-else-if="hasReport">
      <SurfacePanel
        title="财报基本信息"
        description="展示财报的基本信息、处理状态和向量化信息"
        eyebrow="Detail"
      >
        <div
          class="mb-6 flex flex-col gap-3 rounded-[28px] border border-black/5 bg-ink-50/80 p-5 lg:flex-row lg:items-center lg:justify-between"
        >
          <div>
            <p class="shell-kicker">当前记录</p>
            <p class="mt-2 text-lg font-semibold text-ink-900">{{ reportDetail.report_title }}</p>
          </div>
          <button type="button" class="shell-button-secondary" @click="goBack">
            <FontAwesomeIcon :icon="['fas', 'arrow-left']" aria-hidden="true" />
            <span>返回记录中心</span>
          </button>
        </div>

        <div class="grid gap-6 md:grid-cols-2 xl:grid-cols-3">
          <div class="rounded-[24px] border border-black/5 bg-white/85 p-5">
            <p class="shell-kicker">身份信息</p>
            <h3 class="mt-2 text-base font-semibold text-ink-900">报告身份</h3>
            <div class="mt-4 space-y-3 text-sm">
              <div class="flex justify-between">
                <span class="text-ink-500">股票代码</span>
                <span class="font-medium text-ink-900">{{ reportDetail.stock_code }}</span>
              </div>
              <div class="flex justify-between">
                <span class="text-ink-500">股票简称</span>
                <span class="font-medium text-ink-900">{{ reportDetail.stock_abbr }}</span>
              </div>
              <div class="flex justify-between">
                <span class="text-ink-500">报告年份</span>
                <span class="font-medium text-ink-900">{{ reportDetail.report_year }}</span>
              </div>
              <div class="flex justify-between">
                <span class="text-ink-500">报告期</span>
                <span class="font-medium text-ink-900">{{
                  formatReportPeriod(reportDetail.report_period)
                }}</span>
              </div>
              <div class="flex justify-between">
                <span class="text-ink-500">报告类型</span>
                <span class="font-medium text-ink-900">{{
                  formatReportType(reportDetail.report_type)
                }}</span>
              </div>
              <div class="flex justify-between">
                <span class="text-ink-500">交易所</span>
                <span class="font-medium text-ink-900">{{
                  formatExchange(reportDetail.exchange)
                }}</span>
              </div>
              <div class="flex justify-between">
                <span class="text-ink-500">披露日期</span>
                <span class="font-medium text-ink-900">{{
                  formatDate(reportDetail.report_date)
                }}</span>
              </div>
            </div>
          </div>

          <div class="rounded-[24px] border border-black/5 bg-white/85 p-5">
            <p class="shell-kicker">处理状态</p>
            <h3 class="mt-2 text-base font-semibold text-ink-900">状态信息</h3>
            <div class="mt-4 space-y-3 text-sm">
              <div class="flex items-center justify-between">
                <span class="text-ink-500">解析状态</span>
                <StatusBadge v-bind="resolveParseStatusMeta(reportDetail.parse_status)" />
              </div>
              <div class="flex items-center justify-between">
                <span class="text-ink-500">审核状态</span>
                <StatusBadge v-bind="resolveReviewStatusMeta(reportDetail.review_status)" />
              </div>
              <div class="flex items-center justify-between">
                <span class="text-ink-500">校验状态</span>
                <StatusBadge v-bind="resolveValidateStatusMeta(reportDetail.validate_status)" />
              </div>
              <div class="flex items-center justify-between">
                <span class="text-ink-500">入库状态</span>
                <StatusBadge v-bind="resolveImportStatusMeta(reportDetail.import_status)" />
              </div>
              <div class="flex items-center justify-between">
                <span class="text-ink-500">向量化状态</span>
                <StatusBadge v-bind="resolveVectorStatusMeta(reportDetail.vector_status)" />
              </div>
            </div>
          </div>

          <div class="rounded-[24px] border border-black/5 bg-white/85 p-5">
            <p class="shell-kicker">文件信息</p>
            <h3 class="mt-2 text-base font-semibold text-ink-900">文件详情</h3>
            <div class="mt-4 space-y-3 text-sm">
              <div class="flex justify-between">
                <span class="text-ink-500">源文件名</span>
                <span class="max-w-[200px] truncate font-medium text-ink-900" :title="reportDetail.file_name">{{ reportDetail.file_name }}</span>
              </div>
              <div class="flex justify-between">
                <span class="text-ink-500">创建时间</span>
                <span class="font-medium text-ink-900">{{
                  formatDateTime(reportDetail.created_at)
                }}</span>
              </div>
              <div class="flex justify-between">
                <span class="text-ink-500">更新时间</span>
                <span class="font-medium text-ink-900">{{
                  formatDateTime(reportDetail.updated_at)
                }}</span>
              </div>
              <div v-if="reportDetail.validate_message" class="mt-3 rounded-xl bg-ink-50 p-3">
                <p class="text-xs text-ink-500">校验结果说明</p>
                <p class="mt-1 text-sm text-ink-700">{{ reportDetail.validate_message }}</p>
              </div>
            </div>
          </div>
        </div>
      </SurfacePanel>

      <SurfacePanel title="财务报表数据" description="展示四张财务报表的结构化数据">
        <div class="grid gap-6 md:grid-cols-2">
          <section class="rounded-[32px] border border-black/5 bg-white/85 p-6">
            <p class="shell-kicker">Core Performance</p>
            <h3 class="mt-2 text-lg font-semibold text-ink-900">核心业绩指标表</h3>
            <p class="mt-3 text-sm leading-6 text-ink-600">
              展示每股收益、营业总收入、净利润等核心业绩指标数据。
            </p>
            <div v-if="corePerformanceFields.length > 0" class="mt-5 space-y-3 text-sm">
              <div
                v-for="field in corePerformanceFields"
                :key="field.label"
                class="flex items-center justify-between rounded-xl bg-ink-50/50 px-4 py-3"
              >
                <span class="text-ink-600">{{ field.label }}</span>
                <span class="font-medium text-ink-900">{{ field.value }}</span>
              </div>
            </div>
            <div v-else class="mt-5">
              <AppEmptyState
                title="暂无核心业绩指标数据"
                description="该财报尚未提取核心业绩指标数据，请等待系统处理完成。"
              />
            </div>
          </section>

          <section class="rounded-[32px] border border-black/5 bg-white/85 p-6">
            <p class="shell-kicker">Balance Sheet</p>
            <h3 class="mt-2 text-lg font-semibold text-ink-900">资产负债表</h3>
            <p class="mt-3 text-sm leading-6 text-ink-600">
              展示资产、负债和股东权益等资产负债表数据。
            </p>
            <div v-if="balanceSheetFields.length > 0" class="mt-5 space-y-3 text-sm">
              <div
                v-for="field in balanceSheetFields"
                :key="field.label"
                class="flex items-center justify-between rounded-xl bg-ink-50/50 px-4 py-3"
              >
                <span class="text-ink-600">{{ field.label }}</span>
                <span class="font-medium text-ink-900">{{ field.value }}</span>
              </div>
            </div>
            <div v-else class="mt-5">
              <AppEmptyState
                title="暂无资产负债表数据"
                description="该财报尚未提取资产负债表数据，请等待系统处理完成。"
              />
            </div>
          </section>

          <section class="rounded-[32px] border border-black/5 bg-white/85 p-6">
            <p class="shell-kicker">Cash Flow</p>
            <h3 class="mt-2 text-lg font-semibold text-ink-900">现金流量表</h3>
            <p class="mt-3 text-sm leading-6 text-ink-600">
              展示经营性、投资性、融资性现金流等现金流量表数据。
            </p>
            <div v-if="cashFlowFields.length > 0" class="mt-5 space-y-3 text-sm">
              <div
                v-for="field in cashFlowFields"
                :key="field.label"
                class="flex items-center justify-between rounded-xl bg-ink-50/50 px-4 py-3"
              >
                <span class="text-ink-600">{{ field.label }}</span>
                <span class="font-medium text-ink-900">{{ field.value }}</span>
              </div>
            </div>
            <div v-else class="mt-5">
              <AppEmptyState
                title="暂无现金流量表数据"
                description="该财报尚未提取现金流量表数据，请等待系统处理完成。"
              />
            </div>
          </section>

          <section class="rounded-[32px] border border-black/5 bg-white/85 p-6">
            <p class="shell-kicker">Income</p>
            <h3 class="mt-2 text-lg font-semibold text-ink-900">利润表</h3>
            <p class="mt-3 text-sm leading-6 text-ink-600">
              展示营业收入、营业利润、净利润等利润表数据。
            </p>
            <div v-if="incomeSheetFields.length > 0" class="mt-5 space-y-3 text-sm">
              <div
                v-for="field in incomeSheetFields"
                :key="field.label"
                class="flex items-center justify-between rounded-xl bg-ink-50/50 px-4 py-3"
              >
                <span class="text-ink-600">{{ field.label }}</span>
                <span class="font-medium text-ink-900">{{ field.value }}</span>
              </div>
            </div>
            <div v-else class="mt-5">
              <AppEmptyState
                title="暂无利润表数据"
                description="该财报尚未提取利润表数据，请等待系统处理完成。"
              />
            </div>
          </section>
        </div>
      </SurfacePanel>

      <SurfacePanel title="JSON 文件内容" description="查看结构化JSON文件">
        <div class="rounded-[32px] border border-black/5 bg-white/85 p-6">
          <div v-if="jsonLoading" class="flex items-center justify-center py-12">
            <svg class="h-8 w-8 animate-spin text-ink-400" fill="none" viewBox="0 0 24 24">
              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
              <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            <span class="ml-3 text-ink-600">正在加载JSON文件内容...</span>
          </div>
          <div v-else-if="jsonError" class="rounded-xl bg-red-50 p-4 text-sm text-red-700">
            {{ jsonError }}
          </div>
          <template v-else-if="jsonContent">
            <div class="mb-4 flex items-center justify-between">
              <p class="text-sm text-ink-500">
                文件：{{ jsonContent.file_name }}（{{ (jsonContent.file_size / 1024).toFixed(2) }} KB）
              </p>
              <button
                type="button"
                class="inline-flex items-center gap-2 rounded-xl bg-ink-100 px-4 py-2 text-sm font-medium text-ink-700 transition-colors hover:bg-ink-200"
                @click="copyJsonContent"
              >
                <FontAwesomeIcon :icon="['fas', 'copy']" aria-hidden="true" />
                <span>复制 JSON</span>
              </button>
            </div>
            <div class="relative">
              <pre class="max-h-[600px] overflow-auto rounded-2xl bg-ink-900 p-5 text-sm leading-relaxed text-ink-100"><code>{{ JSON.stringify(jsonContent.content, null, 2) }}</code></pre>
            </div>
          </template>
          <AppEmptyState
            v-else
            title="暂无JSON文件"
            description="该财报尚未生成结构化JSON文件，请先完成解析。"
          />
        </div>
      </SurfacePanel>
    </template>

    <AppEmptyState
      v-else
      title="未找到财报记录"
      description="请从记录中心选择一条财报记录进行查看。"
    />
  </div>
</template>
