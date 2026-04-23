<script setup>
/**
 * 任务二工作台
 * 功能：上传附件4、题目管理、批量回答、导出结果
 */
import { ref, onMounted, computed } from 'vue'
import { useTask2Store } from '@/stores/task2'
import { useToast } from '@/composables/useToast'
import { renderMarkdown } from '@/utils/markdown'
import Toast from '@/components/base/Toast.vue'

const task2Store = useTask2Store()
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
  return task2Store.questions.find(q => q.id === selectedQuestionId.value)
})

onMounted(async () => {
  await task2Store.loadWorkspace()
  await task2Store.loadQuestions()
})

async function handleFileSelect(event) {
  const file = event.target.files[0]
  if (!file) return
  
  if (!file.name.endsWith('.xlsx')) {
    alert('请上传xlsx格式的文件')
    return
  }

  try {
    await task2Store.importFujian4(file)
    toast.success('导入成功')
  } catch (err) {
    toast.error('导入失败: ' + err.message)
  }
  
  if (fileInput.value) {
    fileInput.value.value = ''
  }
}

async function handleRefresh() {
  await task2Store.loadWorkspace()
  await task2Store.loadQuestions(selectedStatus.value)
}

async function handleStatusChange() {
  await task2Store.loadQuestions(selectedStatus.value)
}

function handleSelectQuestion(question) {
  selectedQuestionId.value = question.id
}

async function handleAnswerQuestion(question) {
  if (!confirm(`确定要回答题目 ${question.question_code} 吗？`)) return

  try {
    await task2Store.answerQuestion(question.id)
    toast.success('回答完成')
  } catch (err) {
    toast.error('回答失败: ' + err.message)
  }
}

async function handleDeleteAnswer(question) {
  if (!confirm(`确定要删除题目 ${question.question_code} 的回答吗？`)) return

  try {
    await task2Store.deleteAnswer(question.id)
    toast.success('删除成功')
  } catch (err) {
    toast.error('删除失败: ' + err.message)
  }
}

async function handleRerunQuestion(question) {
  if (!confirm(`确定要重新回答题目 ${question.question_code} 吗？`)) return

  try {
    await task2Store.rerunQuestion(question.id)
    toast.success('重新回答完成')
  } catch (err) {
    toast.error('重新回答失败: ' + err.message)
  }
}

async function handleBatchAnswer(scope) {
  const scopeText = scope === 'all' ? '全部' : scope === 'failed' ? '失败的' : '未完成的'
  if (!confirm(`确定要批量回答${scopeText}题目吗？`)) return

  try {
    const result = await task2Store.batchAnswer(scope)
    toast.success(`批量回答完成：成功 ${result.data?.success || 0} 个，失败 ${result.data?.failed || 0} 个`)
  } catch (err) {
    toast.error('批量回答失败: ' + err.message)
  }
}

async function handleExport() {
  try {
    const result = await task2Store.exportResult()
    toast.success(`导出成功：${result.data?.xlsx_path}`)
  } catch (err) {
    toast.error('导出失败: ' + err.message)
  }
}

function formatTime(dateStr) {
  if (!dateStr) return '-'
  const d = new Date(dateStr)
  return d.toLocaleString('zh-CN')
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

    <!-- 顶部工具栏 -->
    <div class="flex items-center justify-between shrink-0 rounded-[20px] border border-black/5 bg-white/80 px-3 py-2">
      <div class="flex items-center gap-4">
        <h3 class="text-lg font-semibold text-ink-900">任务二工作台</h3>
        <div v-if="task2Store.workspace" class="flex items-center gap-4 text-sm text-ink-600">
          <span>题目总数: {{ task2Store.workspace.total_questions }}</span>
          <span class="text-green-600">已完成: {{ task2Store.workspace.answered_count }}</span>
          <span class="text-gray-500">待处理: {{ task2Store.workspace.pending_count }}</span>
          <span class="text-red-500">失败: {{ task2Store.workspace.failed_count }}</span>
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
          :disabled="task2Store.isUploading"
        >
          <FontAwesomeIcon :icon="['fas', 'upload']" />
          <span>{{ task2Store.isUploading ? '导入中...' : '上传附件4' }}</span>
        </button>
        
        <button
          class="flex items-center gap-2 rounded-xl border border-ink-200 px-4 py-2 text-sm text-ink-600 transition-colors hover:bg-ink-50"
          @click="handleBatchAnswer('unfinished')"
          :disabled="task2Store.isProcessing || !task2Store.workspace?.total_questions"
        >
          <FontAwesomeIcon :icon="['fas', 'play']" />
          <span>批量回答未完成</span>
        </button>
        
        <button
          class="flex items-center gap-2 rounded-xl border border-ink-200 px-4 py-2 text-sm text-ink-600 transition-colors hover:bg-ink-50"
          @click="handleBatchAnswer('all')"
          :disabled="task2Store.isProcessing || !task2Store.workspace?.total_questions"
        >
          <FontAwesomeIcon :icon="['fas', 'redo']" />
          <span>批量回答全部</span>
        </button>
        
        <button
          class="flex items-center gap-2 rounded-xl bg-green-600 px-4 py-2 text-sm text-white transition-colors hover:bg-green-700"
          @click="handleExport"
          :disabled="task2Store.isExporting || !task2Store.workspace?.total_questions"
        >
          <FontAwesomeIcon :icon="['fas', 'download']" />
          <span>{{ task2Store.isExporting ? '导出中...' : '导出结果' }}</span>
        </button>
        
        <button
          class="flex h-9 w-9 items-center justify-center rounded-xl border border-ink-200 text-ink-600 transition-colors hover:bg-ink-50"
          @click="handleRefresh"
          :disabled="task2Store.isLoading"
        >
          <FontAwesomeIcon :icon="['fas', 'refresh']" :class="{ 'animate-spin': task2Store.isLoading }" />
        </button>
      </div>
    </div>

    <div class="flex flex-1 items-stretch gap-4 overflow-hidden">
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
            v-for="question in task2Store.questions"
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

          <p v-if="task2Store.questions.length === 0" class="py-8 text-center text-xs text-ink-400">
            暂无题目，请先上传附件4
          </p>
        </div>
      </div>

      <div class="flex flex-1 flex-col overflow-hidden rounded-[28px] border border-black/5 bg-white/80 p-4">
        <div v-if="!currentQuestion" class="flex flex-1 flex-col items-center justify-center">
          <div class="flex h-16 w-16 items-center justify-center rounded-[24px] bg-ink-900 text-white shadow-soft">
            <FontAwesomeIcon :icon="['fas', 'list-check']" class="text-xl" />
          </div>
          <h3 class="mt-6 text-xl font-semibold text-ink-900">任务二工作台</h3>
          <p class="mt-3 max-w-md text-center text-sm leading-6 text-ink-600">
            上传附件4导入题目列表，支持单题回答、批量回答、重新回答，最终导出 result_2.xlsx
          </p>
        </div>

        <div v-else class="flex flex-1 flex-col overflow-hidden">
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
                :disabled="task2Store.isProcessing"
              >
                <FontAwesomeIcon :icon="['fas', 'play']" />
                <span>回答本题</span>
              </button>
              
              <button
                v-if="currentQuestion.status === 2 || currentQuestion.status === 3"
                class="flex items-center gap-1 rounded-lg bg-blue-600 px-3 py-1.5 text-xs text-white transition-colors hover:bg-blue-700"
                @click="handleRerunQuestion(currentQuestion)"
                :disabled="task2Store.isProcessing"
              >
                <FontAwesomeIcon :icon="['fas', 'redo']" />
                <span>重新回答</span>
              </button>
              
              <button
                v-if="currentQuestion.status === 1 || currentQuestion.status === 2 || currentQuestion.status === 3"
                class="flex items-center gap-1 rounded-lg border border-red-200 px-3 py-1.5 text-xs text-red-600 transition-colors hover:bg-red-50"
                @click="handleDeleteAnswer(currentQuestion)"
                :disabled="task2Store.isProcessing"
              >
                <FontAwesomeIcon :icon="['fas', 'trash']" />
                <span>删除回答</span>
              </button>
            </div>
          </div>

          <div class="flex-1 overflow-y-auto">
            <div class="mb-4">
              <h5 class="mb-2 flex items-center gap-2 text-sm font-semibold text-ink-700">
                <FontAwesomeIcon :icon="['fas', 'question-circle']" />
                原始问题
              </h5>
              <pre class="rounded-lg bg-slate-50 p-3 text-xs text-ink-600 whitespace-pre-wrap border border-slate-200">{{ currentQuestion.question_raw_json }}</pre>
            </div>

            <div class="mb-4">
              <h5 class="mb-2 flex items-center gap-2 text-sm font-semibold text-ink-700">
                <FontAwesomeIcon :icon="['fas', 'comments']" />
                回答结果
              </h5>
              <div class="space-y-3">
                <div
                  v-for="(qa, idx) in currentQuestion.answer_json"
                  :key="idx"
                  class="rounded-lg border border-blue-100 bg-blue-50/30 p-3"
                >
                  <div class="mb-2 text-xs font-medium text-ink-500">Q: {{ qa.Q }}</div>
                  <div class="prose prose-sm prose-ink max-w-none text-sm text-ink-700" v-html="renderMarkdown(qa.A?.content)"></div>
                  <div v-if="qa.A?.image && qa.A.image.length > 0" class="mt-2 flex flex-wrap gap-2">
                    <img
                      v-for="(img, imgIdx) in qa.A.image"
                      :key="imgIdx"
                      :src="img.replace('./result/', '/api/v1/chat/images/')"
                      class="max-w-xs rounded-lg border border-black/5"
                    />
                  </div>
                </div>
              </div>
            </div>

            <div v-if="currentQuestion.sql_text" class="mb-4">
              <h5 class="mb-2 flex items-center gap-2 text-sm font-semibold text-ink-700">
                <FontAwesomeIcon :icon="['fas', 'database']" />
                SQL语句
              </h5>
              <pre class="overflow-x-auto rounded-lg bg-ink-900 p-3 text-xs text-green-400">{{ currentQuestion.sql_text }}</pre>
            </div>

            <div v-if="currentQuestion.chart_type" class="mb-4">
              <h5 class="mb-2 flex items-center gap-2 text-sm font-semibold text-ink-700">
                <FontAwesomeIcon :icon="['fas', 'chart-bar']" />
                图表类型
              </h5>
              <span class="inline-flex items-center gap-1.5 rounded-full bg-blue-100 px-3 py-1 text-xs font-medium text-blue-700 border border-blue-200">
                <FontAwesomeIcon :icon="['fas', 'chart-pie']" class="text-xs" />
                {{ currentQuestion.chart_type }}
              </span>
            </div>

            <div v-if="currentQuestion.last_error" class="mb-4">
              <h5 class="mb-2 flex items-center gap-2 text-sm font-semibold text-red-600">
                <FontAwesomeIcon :icon="['fas', 'exclamation-triangle']" />
                错误信息
              </h5>
              <pre class="rounded-lg border border-red-200 bg-red-50 p-3 text-xs text-red-600 whitespace-pre-wrap">{{ currentQuestion.last_error }}</pre>
            </div>

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
