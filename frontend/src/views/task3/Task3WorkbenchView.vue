<script setup>
/**
 * 任务三结果工作台
 * 功能：上传附件6、题目管理、单题/批量回答、导出 result_3.xlsx
 * 依赖组件：Task3ReferencesModal, Toast
 */
import { ref, onMounted, computed } from 'vue'
import { useTask3Store } from '@/stores/task3'
import { useToast } from '@/composables/useToast'
import { renderMarkdown } from '@/utils/markdown'
import Toast from '@/components/base/Toast.vue'
import Task3ReferencesModal from '@/components/task3/Task3ReferencesModal.vue'

const task3Store = useTask3Store()
const toast = useToast()

const selectedStatus = ref(null)
const selectedQuestionId = ref(null)
const fileInput = ref(null)

const statusOptions = [
  { value: null, label: '全部' },
  { value: 0, label: '待处理' },
  { value: 2, label: '已完成' },
  { value: 3, label: '失败' }
]

const statusMap = {
  0: { text: '待处理', class: 'bg-gray-100 text-gray-600' },
  1: { text: '回答中', class: 'bg-blue-100 text-blue-600' },
  2: { text: '已完成', class: 'bg-green-100 text-green-600' },
  3: { text: '失败', class: 'bg-red-100 text-red-600' }
}

const currentQuestion = computed(() => {
  if (!selectedQuestionId.value) return null
  return task3Store.questions.find(q => q.id === selectedQuestionId.value)
})

const hasReferences = computed(() => {
  if (!currentQuestion.value?.answer_json) return false
  return currentQuestion.value.answer_json.some(qa => qa.A?.references?.length > 0)
})

const planData = computed(() => currentQuestion.value?.execution_plan || null)
const verificationData = computed(() => currentQuestion.value?.verification || null)
const retrievalData = computed(() => currentQuestion.value?.retrieval_summary || null)

onMounted(async () => {
  await task3Store.loadWorkspace()
  await task3Store.loadQuestions()
})

async function handleFileSelect(event) {
  const file = event.target.files[0]
  if (!file) return

  if (!file.name.endsWith('.xlsx')) {
    toast.error('请上传xlsx格式的文件')
    return
  }

  try {
    await task3Store.importFujian6(file)
    toast.success('导入成功')
  } catch (err) {
    toast.error('导入失败: ' + err.message)
  }

  if (fileInput.value) {
    fileInput.value.value = ''
  }
}

async function handleRefresh() {
  await task3Store.loadWorkspace()
  await task3Store.loadQuestions(selectedStatus.value)
}

async function handleStatusChange() {
  await task3Store.loadQuestions(selectedStatus.value)
}

function handleSelectQuestion(question) {
  selectedQuestionId.value = question.id
}

async function handleAnswerQuestion(question) {
  if (!confirm(`确定要回答题目 ${question.question_code} 吗？`)) return

  try {
    await task3Store.answerQuestion(question.id)
    toast.success('回答完成')
  } catch (err) {
    toast.error('回答失败: ' + err.message)
  }
}

async function handleDeleteAnswer(question) {
  if (!confirm(`确定要删除题目 ${question.question_code} 的回答吗？`)) return

  try {
    await task3Store.deleteAnswer(question.id)
    toast.success('删除成功')
  } catch (err) {
    toast.error('删除失败: ' + err.message)
  }
}

async function handleRerunQuestion(question) {
  if (!confirm(`确定要重新回答题目 ${question.question_code} 吗？`)) return

  try {
    await task3Store.rerunQuestion(question.id)
    toast.success('重新回答完成')
  } catch (err) {
    toast.error('重新回答失败: ' + err.message)
  }
}

async function handleBatchAnswer(scope) {
  const scopeText = scope === 'all' ? '全部' : scope === 'failed' ? '失败的' : '未完成的'
  if (!confirm(`确定要批量回答${scopeText}题目吗？`)) return

  try {
    const result = await task3Store.batchAnswer(scope)
    toast.success(`批量回答完成：成功 ${result.data?.success || 0} 个，失败 ${result.data?.failed || 0} 个`)
  } catch (err) {
    toast.error('批量回答失败: ' + err.message)
  }
}

async function handleExport() {
  try {
    const result = await task3Store.exportResult()
    toast.success(`导出成功：${result.data?.xlsx_path}`)
  } catch (err) {
    toast.error('导出失败: ' + err.message)
  }
}

function handleOpenReferences() {
  task3Store.openReferencesModal()
}

function formatTime(dateStr) {
  if (!dateStr) return '-'
  const d = new Date(dateStr)
  return d.toLocaleString('zh-CN')
}

function checkStatus(status, field) {
  if (!status) return 'unknown'
  return status[field] ? 'pass' : 'fail'
}
</script>

<template>
  <div class="flex h-full flex-col overflow-hidden gap-4">
    <!-- Toast消息容器 -->
    <div class="fixed bottom-4 right-4 z-50 flex flex-col gap-2">
      <Toast
        v-for="t in toast.toasts.value"
        :key="t.id"
        :id="t.id"
        :message="t.message"
        :type="t.type"
        :duration="t.duration"
        @close="toast.removeToast"
      />
    </div>

    <!-- 参考文献弹窗 -->
    <Task3ReferencesModal
      :visible="task3Store.showReferencesModal"
      :references="task3Store.aggregatedReferences"
      @close="task3Store.closeReferencesModal()"
    />

    <!-- 顶部工具栏 -->
    <div class="flex items-center justify-between shrink-0 rounded-[20px] border border-black/5 bg-white/80 px-3 py-2">
      <div class="flex items-center gap-4">
        <h3 class="text-lg font-semibold text-ink-900">任务三结果工作台</h3>
        <div v-if="task3Store.workspace" class="flex items-center gap-4 text-sm text-ink-600">
          <span>题目总数: {{ task3Store.workspace.total_questions }}</span>
          <span class="text-green-600">已完成: {{ task3Store.workspace.answered_count }}</span>
          <span class="text-gray-500">待处理: {{ task3Store.workspace.pending_count }}</span>
          <span class="text-red-500">失败: {{ task3Store.workspace.failed_count }}</span>
        </div>
      </div>

      <div class="flex items-center gap-2">
        <input
          ref="fileInput"
          type="file"
          accept=".xlsx"
          class="hidden"
          @change="handleFileSelect"
        />
        <button
          class="flex items-center gap-2 rounded-xl bg-ink-900 px-4 py-2 text-sm text-white transition-colors hover:bg-ink-700"
          @click="fileInput?.click()"
          :disabled="task3Store.isUploading"
        >
          <FontAwesomeIcon :icon="['fas', 'upload']" />
          <span>{{ task3Store.isUploading ? '导入中...' : '上传附件6' }}</span>
        </button>

        <button
          class="flex items-center gap-2 rounded-xl border border-ink-200 px-4 py-2 text-sm text-ink-600 transition-colors hover:bg-ink-50"
          @click="handleBatchAnswer('unfinished')"
          :disabled="task3Store.isProcessing || !task3Store.workspace?.total_questions"
        >
          <FontAwesomeIcon :icon="['fas', 'play']" />
          <span>批量回答未完成</span>
        </button>

        <button
          class="flex items-center gap-2 rounded-xl border border-ink-200 px-4 py-2 text-sm text-ink-600 transition-colors hover:bg-ink-50"
          @click="handleBatchAnswer('all')"
          :disabled="task3Store.isProcessing || !task3Store.workspace?.total_questions"
        >
          <FontAwesomeIcon :icon="['fas', 'redo']" />
          <span>批量回答全部</span>
        </button>

        <button
          class="flex items-center gap-2 rounded-xl bg-green-600 px-4 py-2 text-sm text-white transition-colors hover:bg-green-700"
          @click="handleExport"
          :disabled="task3Store.isExporting || !task3Store.workspace?.total_questions"
        >
          <FontAwesomeIcon :icon="['fas', 'download']" />
          <span>{{ task3Store.isExporting ? '导出中...' : '导出结果' }}</span>
        </button>

        <button
          class="flex h-9 w-9 items-center justify-center rounded-xl border border-ink-200 text-ink-600 transition-colors hover:bg-ink-50"
          @click="handleRefresh"
          :disabled="task3Store.isLoading"
        >
          <FontAwesomeIcon :icon="['fas', 'refresh']" :class="{ 'animate-spin': task3Store.isLoading }" />
        </button>
      </div>
    </div>

    <div class="flex flex-1 items-stretch gap-4 overflow-hidden">
      <!-- 左侧题目列表 -->
      <div class="flex w-80 shrink-0 flex-col overflow-hidden rounded-[28px] border border-black/5 bg-white/80 p-4">
        <div class="mb-4 flex shrink-0 items-center justify-between">
          <h4 class="text-sm font-semibold text-ink-900">题目列表</h4>
          <select
            v-model="selectedStatus"
            class="rounded-lg border border-ink-200 px-2 py-1 text-xs text-ink-600 outline-none focus:border-ink-400"
            @change="handleStatusChange"
          >
            <option v-for="opt in statusOptions" :key="opt.value" :value="opt.value">
              {{ opt.label }}
            </option>
          </select>
        </div>

        <div class="flex-1 overflow-y-auto">
          <div
            v-for="question in task3Store.questions"
            :key="question.id"
            class="mb-1.5 cursor-pointer rounded-xl p-2.5 transition-colors"
            :class="{
              'bg-ink-900 text-white': selectedQuestionId === question.id,
              'bg-ink-50 text-ink-700 hover:bg-ink-100': selectedQuestionId !== question.id
            }"
            @click="handleSelectQuestion(question)"
          >
            <div class="flex items-center justify-between">
              <span class="font-semibold text-sm">{{ question.question_code }}</span>
              <span
                class="rounded-full border px-2 py-0.5 text-xs font-medium"
                :class="statusMap[question.status]?.class"
              >
                {{ statusMap[question.status]?.text }}
              </span>
            </div>
            <div class="mt-1 text-xs opacity-60">
              {{ question.question_type || '-' }}
            </div>
          </div>

          <p v-if="task3Store.questions.length === 0" class="py-8 text-center text-xs text-ink-400">
            暂无题目，请先上传附件6
          </p>
        </div>
      </div>

      <!-- 右侧内容区 -->
      <div class="flex flex-1 flex-col overflow-hidden rounded-[28px] border border-black/5 bg-white/80 p-4">
        <!-- 空状态 -->
        <div v-if="!currentQuestion" class="flex flex-1 flex-col items-center justify-center">
          <div class="flex h-16 w-16 items-center justify-center rounded-[24px] bg-ink-900 text-white shadow-soft">
            <FontAwesomeIcon :icon="['fas', 'list-check']" class="text-xl" />
          </div>
          <h3 class="mt-6 text-xl font-semibold text-ink-900">任务三结果工作台</h3>
          <p class="mt-3 max-w-md text-center text-sm leading-6 text-ink-600">
            上传附件6导入题目列表，支持单题回答、批量回答、重新回答，最终导出 result_3.xlsx
          </p>
        </div>

        <!-- 题目详情 -->
        <div v-else class="flex flex-1 flex-col overflow-hidden">
          <!-- 头部信息区 -->
          <div class="mb-4 flex shrink-0 items-center justify-between border-b border-black/5 pb-4">
            <div>
              <h4 class="text-lg font-semibold text-ink-900">{{ currentQuestion.question_code }}</h4>
              <p class="text-sm text-ink-500">{{ currentQuestion.question_type || '-' }}</p>
            </div>
            <div class="flex items-center gap-2">
              <button
                v-if="currentQuestion.status === 0"
                class="flex items-center gap-1 rounded-lg bg-ink-900 px-3 py-1.5 text-xs text-white transition-colors hover:bg-ink-700"
                @click="handleAnswerQuestion(currentQuestion)"
                :disabled="task3Store.isProcessing"
              >
                <FontAwesomeIcon :icon="['fas', 'play']" />
                <span>回答本题</span>
              </button>

              <button
                v-if="currentQuestion.status === 2 || currentQuestion.status === 3"
                class="flex items-center gap-1 rounded-lg bg-blue-600 px-3 py-1.5 text-xs text-white transition-colors hover:bg-blue-700"
                @click="handleRerunQuestion(currentQuestion)"
                :disabled="task3Store.isProcessing"
              >
                <FontAwesomeIcon :icon="['fas', 'redo']" />
                <span>重新回答</span>
              </button>

              <button
                v-if="currentQuestion.status === 1 || currentQuestion.status === 2 || currentQuestion.status === 3"
                class="flex items-center gap-1 rounded-lg border border-red-200 px-3 py-1.5 text-xs text-red-600 transition-colors hover:bg-red-50"
                @click="handleDeleteAnswer(currentQuestion)"
                :disabled="task3Store.isProcessing"
              >
                <FontAwesomeIcon :icon="['fas', 'trash']" />
                <span>删除回答</span>
              </button>

              <button
                v-if="hasReferences"
                class="flex items-center gap-1 rounded-lg border border-amber-200 bg-amber-50 px-3 py-1.5 text-xs text-amber-700 transition-colors hover:bg-amber-100"
                @click="handleOpenReferences"
              >
                <FontAwesomeIcon :icon="['fas', 'book-open']" />
                <span>查看参考文献</span>
              </button>
            </div>
          </div>

          <!-- 详情滚动区 -->
          <div class="flex-1 overflow-y-auto">
            <!-- 原始问题 -->
            <div class="mb-4">
              <h5 class="mb-2 flex items-center gap-2 text-sm font-semibold text-ink-700">
                <FontAwesomeIcon :icon="['fas', 'question-circle']" />
                原始问题
              </h5>
              <pre class="rounded-lg bg-slate-50 p-3 text-xs text-ink-600 whitespace-pre-wrap border border-slate-200">{{ currentQuestion.question_raw_json }}</pre>
            </div>

            <!-- 回答结果 -->
            <div class="mb-4">
              <h5 class="mb-2 flex items-center gap-2 text-sm font-semibold text-ink-700">
                <FontAwesomeIcon :icon="['fas', 'comments']" />
                回答结果
              </h5>
              <div v-if="currentQuestion.status === 0" class="py-4 text-center text-sm text-ink-400">
                暂无回答结果，请点击"回答本题"开始
              </div>
              <div v-else-if="currentQuestion.status === 1" class="py-4 text-center text-sm text-blue-500">
                正在回答中...
              </div>
              <div v-else-if="!currentQuestion.answer_json || currentQuestion.answer_json.length === 0" class="py-4 text-center text-sm text-ink-400">
                暂无回答结果
              </div>
              <div v-else class="space-y-3">
                <div
                  v-for="(qa, idx) in currentQuestion.answer_json"
                  :key="idx"
                  class="rounded-lg border border-blue-100 bg-blue-50/30 p-3"
                >
                  <div class="mb-2 text-xs font-medium text-ink-500">Q: {{ qa.Q }}</div>
                  <div v-if="qa.A?.content" class="prose prose-sm prose-ink max-w-none text-sm text-ink-700" v-html="renderMarkdown(qa.A.content)"></div>
                </div>
              </div>
            </div>

            <!-- 执行计划（有数据再展示） -->
            <div v-if="planData" class="mb-4">
              <h5 class="mb-2 flex items-center gap-2 text-sm font-semibold text-ink-700">
                <FontAwesomeIcon :icon="['fas', 'sitemap']" />
                执行计划
              </h5>
              <div class="rounded-lg border border-purple-100 bg-purple-50/30 p-3">
                <div v-if="planData.summary" class="mb-2 text-xs text-ink-500">{{ planData.summary }}</div>
                <div v-if="planData.total_steps" class="mb-2 text-xs text-ink-400">步骤总数: {{ planData.total_steps }}</div>
                <div v-if="planData.steps && planData.steps.length > 0" class="space-y-2">
                  <div
                    v-for="(step, idx) in planData.steps"
                    :key="idx"
                    class="rounded-lg border border-purple-200 bg-white p-2 text-xs"
                  >
                    <div class="flex items-center gap-2">
                      <span class="font-medium text-ink-700">Step {{ step.step_id || idx + 1 }}</span>
                      <span class="rounded-full bg-ink-100 px-2 py-0.5 text-ink-500">{{ step.step_type }}</span>
                      <span
                        v-if="step.status"
                        class="rounded-full px-2 py-0.5"
                        :class="{
                          'bg-green-100 text-green-700': step.status === 'completed' || step.status === 'success',
                          'bg-yellow-100 text-yellow-700': step.status === 'running' || step.status === 'pending',
                          'bg-red-100 text-red-700': step.status === 'failed'
                        }"
                      >{{ step.status }}</span>
                    </div>
                    <div v-if="step.goal" class="mt-1 text-ink-600">{{ step.goal }}</div>
                    <div v-if="step.depends_on && step.depends_on.length > 0" class="mt-1 text-ink-400">
                      依赖: {{ step.depends_on.join(', ') }}
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <!-- 校验结果（有数据再展示） -->
            <div v-if="verificationData" class="mb-4">
              <h5 class="mb-2 flex items-center gap-2 text-sm font-semibold text-ink-700">
                <FontAwesomeIcon :icon="['fas', 'shield-halved']" />
                校验结果
              </h5>
              <div class="rounded-lg border border-green-100 bg-green-50/30 p-3">
                <div class="flex items-center gap-3 mb-2">
                  <span
                    class="rounded-full px-3 py-1 text-xs font-medium"
                    :class="{
                      'bg-green-100 text-green-700': verificationData.passed,
                      'bg-red-100 text-red-700': !verificationData.passed
                    }"
                  >
                    {{ verificationData.passed ? '校验通过' : '校验未通过' }}
                  </span>
                </div>
                <div v-if="verificationData.errors && verificationData.errors.length > 0" class="mb-2">
                  <div class="text-xs font-medium text-red-600 mb-1">错误:</div>
                  <div v-for="(err, idx) in verificationData.errors" :key="'err-'+idx" class="text-xs text-red-500 ml-2">
                    - {{ err }}
                  </div>
                </div>
                <div v-if="verificationData.warnings && verificationData.warnings.length > 0" class="mb-2">
                  <div class="text-xs font-medium text-yellow-600 mb-1">警告:</div>
                  <div v-for="(warn, idx) in verificationData.warnings" :key="'warn-'+idx" class="text-xs text-yellow-500 ml-2">
                    - {{ warn }}
                  </div>
                </div>
                <div v-if="verificationData.summary" class="flex flex-wrap gap-2 text-xs">
                  <span
                    v-for="(val, key) in verificationData.summary"
                    :key="key"
                    class="rounded-full px-2 py-0.5"
                    :class="{
                      'bg-green-100 text-green-700': checkStatus(verificationData.summary, key) === 'pass',
                      'bg-red-100 text-red-700': checkStatus(verificationData.summary, key) === 'fail',
                      'bg-gray-100 text-gray-600': checkStatus(verificationData.summary, key) === 'unknown'
                    }"
                  >{{ key }}: {{ val }}</span>
                </div>
              </div>
            </div>

            <!-- 知识库检索摘要（有数据再展示） -->
            <div v-if="retrievalData" class="mb-4">
              <h5 class="mb-2 flex items-center gap-2 text-sm font-semibold text-ink-700">
                <FontAwesomeIcon :icon="['fas', 'magnifying-glass']" />
                知识库检索摘要
              </h5>
              <div class="rounded-lg border border-blue-100 bg-blue-50/30 p-3">
                <div class="grid grid-cols-2 gap-2 text-xs">
                  <div v-if="retrievalData.triggered !== undefined" class="flex items-center gap-2">
                    <span class="text-ink-400">触发检索:</span>
                    <span :class="retrievalData.triggered ? 'text-green-600' : 'text-gray-400'">{{ retrievalData.triggered ? '是' : '否' }}</span>
                  </div>
                  <div v-if="retrievalData.hit_count !== undefined" class="flex items-center gap-2">
                    <span class="text-ink-400">命中文档:</span>
                    <span class="text-ink-700">{{ retrievalData.hit_count }}</span>
                  </div>
                  <div v-if="retrievalData.doc_types" class="flex items-center gap-2">
                    <span class="text-ink-400">文档类型:</span>
                    <span class="text-ink-700">{{ Array.isArray(retrievalData.doc_types) ? retrievalData.doc_types.join(', ') : retrievalData.doc_types }}</span>
                  </div>
                  <div v-if="retrievalData.stock_filter" class="flex items-center gap-2">
                    <span class="text-ink-400">股票过滤:</span>
                    <span class="text-ink-700">{{ retrievalData.stock_filter }}</span>
                  </div>
                  <div v-if="retrievalData.generated_references !== undefined" class="flex items-center gap-2">
                    <span class="text-ink-400">生成参考文献:</span>
                    <span :class="retrievalData.generated_references ? 'text-green-600' : 'text-gray-400'">{{ retrievalData.generated_references ? '是' : '否' }}</span>
                  </div>
                </div>
              </div>
            </div>

            <!-- SQL语句 -->
            <div v-if="currentQuestion.sql_text" class="mb-4">
              <h5 class="mb-2 flex items-center gap-2 text-sm font-semibold text-ink-700">
                <FontAwesomeIcon :icon="['fas', 'database']" />
                SQL语句
              </h5>
              <pre class="overflow-x-auto rounded-lg bg-ink-900 p-3 text-xs text-green-400">{{ currentQuestion.sql_text }}</pre>
            </div>

            <!-- 错误信息 -->
            <div v-if="currentQuestion.last_error" class="mb-4">
              <h5 class="mb-2 flex items-center gap-2 text-sm font-semibold text-red-600">
                <FontAwesomeIcon :icon="['fas', 'exclamation-triangle']" />
                错误信息
              </h5>
              <pre class="rounded-lg border border-red-200 bg-red-50 p-3 text-xs text-red-600 whitespace-pre-wrap">{{ currentQuestion.last_error }}</pre>
            </div>

            <!-- 时间信息 -->
            <div class="text-xs text-ink-400">
              <span>创建时间: {{ formatTime(currentQuestion.created_at) }}</span>
              <span v-if="currentQuestion.answered_at" class="ml-4">回答时间: {{ formatTime(currentQuestion.answered_at) }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
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

.prose :deep(p) {
  margin: 0.5em 0;
}

.prose :deep(ul),
.prose :deep(ol) {
  margin: 0.5em 0;
  padding-left: 1.5em;
}

.prose :deep(li) {
  margin: 0.25em 0;
}

.prose :deep(strong) {
  font-weight: 600;
  color: #1a1a2e;
}

.prose :deep(code) {
  background: #f1f5f9;
  padding: 0.125em 0.375em;
  border-radius: 0.25em;
  font-size: 0.875em;
}

.prose :deep(pre) {
  background: #1a1a2e;
  color: #a5f3fc;
  padding: 0.75em 1em;
  border-radius: 0.5em;
  overflow-x: auto;
  margin: 0.75em 0;
}

.prose :deep(pre code) {
  background: transparent;
  padding: 0;
}

.prose :deep(table) {
  width: 100%;
  border-collapse: collapse;
  margin: 0.75em 0;
}

.prose :deep(th),
.prose :deep(td) {
  border: 1px solid #e2e8f0;
  padding: 0.5em 0.75em;
  text-align: left;
}

.prose :deep(th) {
  background: #f8fafc;
  font-weight: 600;
}

.prose :deep(blockquote) {
  border-left: 3px solid #94a3b8;
  padding-left: 1em;
  margin: 0.75em 0;
  color: #64748b;
}
</style>
