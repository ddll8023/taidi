<script setup>
/**
 * 财报详情页
 * 展示财报基础信息及四张结构化事实表数据（核心指标、资产负债表、利润表、现金流量表）
 */
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import BaseButton from '@/components/ui/BaseButton.vue'
import { getReportDetail } from '@/api/financialReports'
import SurfacePanel from '@/components/ui/SurfacePanel.vue'

const route = useRoute()
const router = useRouter()
const reportId = Number(route.params.id)

// ── 状态 ──
const report = ref(null)
const isLoading = ref(true)
const errorMessage = ref('')

// ── 格式化 ──
const formatDateTime = (value) => {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit'
  }).format(date)
}

const formatNumber = (value) => {
  if (value === null || value === undefined) return '-'
  const num = Number(value)
  if (Number.isNaN(num)) return value
  return num.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 4 })
}

const formatPercent = (value) => {
  if (value === null || value === undefined) return '-'
  const num = Number(value)
  if (Number.isNaN(num)) return value
  return `${num.toFixed(2)}%`
}

const parseStatusMeta = computed(() => {
  if (!report.value) return { label: '-', cls: 'bg-ink-50 text-ink-600' }
  const map = {
    0: { label: '待处理', cls: 'bg-yellow-50 text-yellow-700' },
    1: { label: '解析成功', cls: 'bg-green-50 text-green-700' },
    2: { label: '解析失败', cls: 'bg-red-50 text-red-700' },
    3: { label: '解析中', cls: 'bg-blue-50 text-blue-700' }
  }
  return map[report.value.parse_status] || { label: String(report.value.parse_status), cls: 'bg-ink-50 text-ink-600' }
})

// ── 四张表字段定义（label + key + format） ──
const TABLE_DEFS = {
  core_performance_indicators: {
    title: '核心业绩指标',
    icon: 'chart-line',
    fields: [
      { label: '每股收益(元)', key: 'eps', fmt: 'number' },
      { label: '营业总收入(万元)', key: 'total_operating_revenue', fmt: 'number' },
      { label: '营业总收入-同比增长(%)', key: 'operating_revenue_yoy_growth', fmt: 'percent' },
      { label: '营业总收入-季度环比增长(%)', key: 'operating_revenue_qoq_growth', fmt: 'percent' },
      { label: '净利润(万元)', key: 'net_profit_10k_yuan', fmt: 'number' },
      { label: '净利润-同比增长(%)', key: 'net_profit_yoy_growth', fmt: 'percent' },
      { label: '净利润-季度环比增长(%)', key: 'net_profit_qoq_growth', fmt: 'percent' },
      { label: '每股净资产(元)', key: 'net_asset_per_share', fmt: 'number' },
      { label: '净资产收益率(%)', key: 'roe', fmt: 'percent' },
      { label: '每股经营现金流量(元)', key: 'operating_cf_per_share', fmt: 'number' },
      { label: '扣非净利润（万元）', key: 'net_profit_excl_non_recurring', fmt: 'number' },
      { label: '扣非净利润同比增长(%)', key: 'net_profit_excl_non_recurring_yoy', fmt: 'percent' },
      { label: '销售毛利率(%)', key: 'gross_profit_margin', fmt: 'percent' },
      { label: '销售净利率(%)', key: 'net_profit_margin', fmt: 'percent' },
      { label: '加权平均净资产收益率（扣非）(%)', key: 'roe_weighted_excl_non_recurring', fmt: 'percent' }
    ]
  },
  balance_sheet: {
    title: '资产负债表',
    icon: 'scale-balanced',
    fields: [
      { label: '货币资金(万元)', key: 'asset_cash_and_cash_equivalents', fmt: 'number' },
      { label: '应收账款(万元)', key: 'asset_accounts_receivable', fmt: 'number' },
      { label: '存货(万元)', key: 'asset_inventory', fmt: 'number' },
      { label: '交易性金融资产(万元)', key: 'asset_trading_financial_assets', fmt: 'number' },
      { label: '在建工程(万元)', key: 'asset_construction_in_progress', fmt: 'number' },
      { label: '总资产(万元)', key: 'asset_total_assets', fmt: 'number' },
      { label: '总资产同比(%)', key: 'asset_total_assets_yoy_growth', fmt: 'percent' },
      { label: '应付账款(万元)', key: 'liability_accounts_payable', fmt: 'number' },
      { label: '预收账款(万元)', key: 'liability_advance_from_customers', fmt: 'number' },
      { label: '总负债(万元)', key: 'liability_total_liabilities', fmt: 'number' },
      { label: '总负债同比(%)', key: 'liability_total_liabilities_yoy_growth', fmt: 'percent' },
      { label: '合同负债(万元)', key: 'liability_contract_liabilities', fmt: 'number' },
      { label: '短期借款(万元)', key: 'liability_short_term_loans', fmt: 'number' },
      { label: '资产负债率(%)', key: 'asset_liability_ratio', fmt: 'percent' },
      { label: '未分配利润(万元)', key: 'equity_unappropriated_profit', fmt: 'number' },
      { label: '股东权益合计(万元)', key: 'equity_total_equity', fmt: 'number' }
    ]
  },
  income_sheet: {
    title: '利润表',
    icon: 'chart-simple',
    fields: [
      { label: '净利润(万元)', key: 'net_profit', fmt: 'number' },
      { label: '净利润同比(%)', key: 'net_profit_yoy_growth', fmt: 'percent' },
      { label: '其他收益（万元）', key: 'other_income', fmt: 'number' },
      { label: '营业总收入(万元)', key: 'total_operating_revenue', fmt: 'number' },
      { label: '营业总收入同比(%)', key: 'operating_revenue_yoy_growth', fmt: 'percent' },
      { label: '营业支出(万元)', key: 'operating_expense_cost_of_sales', fmt: 'number' },
      { label: '销售费用(万元)', key: 'operating_expense_selling_expenses', fmt: 'number' },
      { label: '管理费用(万元)', key: 'operating_expense_administrative_expenses', fmt: 'number' },
      { label: '财务费用(万元)', key: 'operating_expense_financial_expenses', fmt: 'number' },
      { label: '研发费用(万元)', key: 'operating_expense_rnd_expenses', fmt: 'number' },
      { label: '税金及附加(万元)', key: 'operating_expense_taxes_and_surcharges', fmt: 'number' },
      { label: '营业总支出(万元)', key: 'total_operating_expenses', fmt: 'number' },
      { label: '营业利润(万元)', key: 'operating_profit', fmt: 'number' },
      { label: '利润总额(万元)', key: 'total_profit', fmt: 'number' },
      { label: '资产减值损失(万元)', key: 'asset_impairment_loss', fmt: 'number' },
      { label: '信用减值损失(万元)', key: 'credit_impairment_loss', fmt: 'number' }
    ]
  },
  cash_flow_sheet: {
    title: '现金流量表',
    icon: 'money-bills',
    fields: [
      { label: '净现金流(元)', key: 'net_cash_flow', fmt: 'number' },
      { label: '净现金流-同比增长(%)', key: 'net_cash_flow_yoy_growth', fmt: 'percent' },
      { label: '经营活动-现金流量净额(万元)', key: 'operating_cf_net_amount', fmt: 'number' },
      { label: '经营活动-净现金流占比(%)', key: 'operating_cf_ratio_of_net_cf', fmt: 'percent' },
      { label: '经营活动-销售商品收到的现金(万元)', key: 'operating_cf_cash_from_sales', fmt: 'number' },
      { label: '投资性-现金流量净额(万元)', key: 'investing_cf_net_amount', fmt: 'number' },
      { label: '投资性-净现金流占比(%)', key: 'investing_cf_ratio_of_net_cf', fmt: 'percent' },
      { label: '投资性-投资支付的现金(万元)', key: 'investing_cf_cash_for_investments', fmt: 'number' },
      { label: '投资性-收回投资收到的现金(万元)', key: 'investing_cf_cash_from_investment_recovery', fmt: 'number' },
      { label: '融资性-取得借款收到的现金(万元)', key: 'financing_cf_cash_from_borrowing', fmt: 'number' },
      { label: '融资性-偿还债务支付的现金(万元)', key: 'financing_cf_cash_for_debt_repayment', fmt: 'number' },
      { label: '融资性-现金流量净额(万元)', key: 'financing_cf_net_amount', fmt: 'number' },
      { label: '融资性-净现金流占比(%)', key: 'financing_cf_ratio_of_net_cf', fmt: 'percent' }
    ]
  }
}

const fieldFormatter = (value, fmt) => {
  if (fmt === 'percent') return formatPercent(value)
  return formatNumber(value)
}

// ── 获取数据 ──
const fetchDetail = async () => {
  isLoading.value = true
  errorMessage.value = ''
  try {
    const response = await getReportDetail(reportId)
    const payload = response?.data || response
    report.value = payload
  } catch (error) {
    errorMessage.value = error.message || '加载详情失败'
  } finally {
    isLoading.value = false
  }
}

const goBack = () => router.push('/reports/list')

onMounted(fetchDetail)
</script>

<template>
  <div class="space-y-6">
    <!-- 头部 -->
    <SurfacePanel :padded="false">
      <div class="border-b border-black/5 px-5 py-5 sm:px-6">
        <BaseButton variant="ghost" icon="arrow-left" size="sm" @click="goBack">返回列表</BaseButton>

        <div v-if="report" class="mt-3 flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <h2 class="text-xl font-semibold text-ink-900">{{ report.report_title || '财报详情' }}</h2>
            <p class="mt-1 text-sm text-ink-500">
              {{ report.stock_code }} · {{ report.stock_abbr }} ·
              {{ report.report_year }} {{ report.report_period }} ·
              {{ report.report_type }}
            </p>
          </div>
          <span class="inline-flex shrink-0 items-center rounded-full px-3 py-1 text-xs font-medium" :class="parseStatusMeta.cls">
            <FontAwesomeIcon v-if="report.parse_status === 3" :icon="['fas', 'spinner']" spin class="mr-1" aria-hidden="true" />
            <FontAwesomeIcon v-else-if="report.parse_status === 1" :icon="['fas', 'check-circle']" class="mr-1" aria-hidden="true" />
            <FontAwesomeIcon v-else-if="report.parse_status === 2" :icon="['fas', 'circle-exclamation']" class="mr-1" aria-hidden="true" />
            <FontAwesomeIcon v-else :icon="['fas', 'clock']" class="mr-1" aria-hidden="true" />
            {{ parseStatusMeta.label }}
          </span>
        </div>
      </div>

      <!-- 加载中 -->
      <div v-if="isLoading" class="flex flex-col items-center justify-center py-20">
        <FontAwesomeIcon :icon="['fas', 'spinner']" spin class="text-3xl text-accent-500" />
        <p class="mt-4 text-sm text-ink-500">加载中...</p>
      </div>

      <!-- 错误 -->
      <div v-else-if="errorMessage" class="flex flex-col items-center justify-center py-20">
        <p class="text-sm text-danger">{{ errorMessage }}</p>
        <BaseButton variant="secondary" @click="fetchDetail">重试</BaseButton>
      </div>

      <!-- 基础信息 -->
      <div v-else-if="report" class="p-5 sm:p-6 space-y-6">
        <!-- 概览卡片 -->
        <div class="grid grid-cols-2 gap-4 sm:grid-cols-4">
          <div class="rounded-2xl bg-ink-50/60 px-4 py-3">
            <p class="text-xs text-ink-500">文件名称</p>
            <p class="mt-1 truncate text-sm font-medium text-ink-900">{{ report.file_name || '-' }}</p>
          </div>
          <div class="rounded-2xl bg-ink-50/60 px-4 py-3">
            <p class="text-xs text-ink-500">报告期间</p>
            <p class="mt-1 text-sm font-medium text-ink-900">{{ report.report_period || '-' }}</p>
          </div>
          <div class="rounded-2xl bg-ink-50/60 px-4 py-3">
            <p class="text-xs text-ink-500">创建时间</p>
            <p class="mt-1 text-sm font-medium text-ink-900">{{ formatDateTime(report.created_at) }}</p>
          </div>
          <div class="rounded-2xl bg-ink-50/60 px-4 py-3">
            <p class="text-xs text-ink-500">更新时间</p>
            <p class="mt-1 text-sm font-medium text-ink-900">{{ formatDateTime(report.updated_at) }}</p>
          </div>
        </div>

        <!-- 四张事实表 -->
        <div
          v-for="(tableDef, tableKey) in TABLE_DEFS"
          :key="tableKey"
          class="overflow-hidden rounded-2xl border border-black/5"
        >
          <div class="flex items-center gap-2 border-b border-black/5 bg-ink-50/40 px-5 py-3">
            <FontAwesomeIcon :icon="['fas', tableDef.icon]" class="text-accent-600" aria-hidden="true" />
            <h3 class="text-sm font-semibold text-ink-900">{{ tableDef.title }}</h3>
            <span
              v-if="!report[tableKey]"
              class="ml-auto text-xs text-ink-400"
            >暂无数据</span>
          </div>
          <div v-if="report[tableKey]" class="grid grid-cols-2 gap-px bg-black/5 sm:grid-cols-3 lg:grid-cols-4">
            <div
              v-for="field in tableDef.fields"
              :key="field.key"
              class="bg-white px-4 py-3"
            >
              <p class="text-xs text-ink-500">{{ field.label }}</p>
              <p class="mt-0.5 text-sm font-medium text-ink-900">
                {{ fieldFormatter(report[tableKey][field.key], field.fmt) }}
              </p>
            </div>
          </div>
        </div>
      </div>
    </SurfacePanel>
  </div>
</template>