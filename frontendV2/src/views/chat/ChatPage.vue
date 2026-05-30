<script setup>
/**
 * 智能问数主页
 * 功能描述：左右分栏布局，左侧会话列表 + 右侧对话区域
 * 依赖组件：BaseButton, BaseInput, SurfacePanel, AppEmptyState
 */
import {
  ref, reactive, computed, nextTick, watch, onMounted
} from 'vue'
import BaseButton from '@/components/ui/BaseButton.vue'
import BaseInput from '@/components/ui/BaseInput.vue'
import SurfacePanel from '@/components/ui/SurfacePanel.vue'
import AppEmptyState from '@/components/ui/AppEmptyState.vue'
import ConfirmDialog from '@/components/ui/ConfirmDialog.vue'
import { sendChatMessageStream, getChatList, getChatDetail, deleteChatSession } from '@/api/chat'
import { renderMarkdown } from '@/utils/markdown'

// ── 步骤配置 ──

const STEP_CONFIG = {
  intent: { label: '意图识别', icon: 'brain', group: 'base' },
  sql: { label: 'SQL 生成', icon: 'database', group: 'sql' },
  query: { label: '数据查询', icon: 'magnifying-glass', group: 'sql' },
  rag: { label: '知识库检索', icon: 'microchip', group: 'rag' },
  answer: { label: '综合分析', icon: 'wand-magic-sparkles', group: 'base' },
}

const STEP_ORDER = ['intent', 'sql', 'query', 'rag', 'answer']

const STEP_GROUP_CONFIG = {
  sql: { label: '财报数据查询', color: 'accent' },
  rag: { label: '研报知识库检索', color: 'emerald' },
}

// ── 左侧：会话列表状态 ──

const sessions = reactive({
  items: [],
  page: 1,
  pageSize: 50,
  total: 0,
})
const isListLoading = ref(false)
const listError = ref('')
const isHistoryLoading = ref(false)
const activeSessionId = ref(null)
const confirmDialogVisible = ref(false)
const sessionToDelete = ref(null)
const isDeleting = ref(false)

const fetchSessions = async () => {
  isListLoading.value = true
  listError.value = ''
  try {
    const response = await getChatList({ page: sessions.page, page_size: sessions.pageSize })
    const payload = response?.data || response
    const items = payload?.lists || payload?.items || []
    const pagination = payload?.pagination || {}
    sessions.items = items
    sessions.total = pagination?.total || items.length
  } catch (error) {
    listError.value = error.message || '加载失败'
  } finally {
    isListLoading.value = false
  }
}

const hasSessions = computed(() => sessions.items.length > 0)

// ── 右侧：对话状态 ──

const messages = reactive([])
const currentInput = ref('')
const isLoading = ref(false)
const sessionId = ref(null)
const copiedId = ref(null)
const streamingContent = ref('')
const progressSteps = reactive({
  intent: { status: 'pending' },
  sql: { status: 'pending' },
  query: { status: 'pending' },
  rag: { status: 'pending' },
  answer: { status: 'pending' },
})

const hasMessages = computed(() => messages.length > 0)
const canSend = computed(() => currentInput.value.trim().length > 0 && !isLoading.value)

const activeGroups = computed(() => {
  const groups = new Set()
  STEP_ORDER.forEach((key) => {
    if (progressSteps[key].status !== 'pending') {
      groups.add(STEP_CONFIG[key].group)
    }
  })
  const active = []
  if (groups.has('sql')) active.push('sql')
  if (groups.has('rag')) active.push('rag')
  return active
})

const progressMessage = computed(() => {
  const active = STEP_ORDER.find((key) => progressSteps[key].status === 'active')
  return active ? STEP_CONFIG[active].label : '正在分析...'
})

// ── 消息操作 ──

const addMessage = (role, content, extra = {}) => {
  messages.push({
    id: Date.now() + Math.random(),
    role,
    content,
    renderedHtml: role === 'assistant' ? renderMarkdown(content) : '',
    sql: extra.sql || null,
    showSql: false,
  })
}

const scrollToBottom = async () => {
  await nextTick()
  const container = document.querySelector('.chat-messages')
  if (container) container.scrollTop = container.scrollHeight
}

const toggleSql = (msg) => { msg.showSql = !msg.showSql }

const copyText = async (text, msgId) => {
  try {
    await navigator.clipboard.writeText(text)
    copiedId.value = msgId
    setTimeout(() => { copiedId.value = null }, 2000)
  } catch { /* fallback */ }
}

const resetProgress = () => {
  STEP_ORDER.forEach((key) => { progressSteps[key].status = 'pending' })
  streamingContent.value = ''
}

// ── SSE 回调 ──

const handleStep = (data) => {
  const step = data.step
  if (!step) return
  const stepKey = step.replace('_done', '')
  if (!STEP_ORDER.includes(stepKey)) return
  progressSteps[stepKey].status = step.endsWith('_done') ? 'done' : 'active'
}

const handleToken = (data) => {
  if (data.content) streamingContent.value += data.content
}

const handleResult = (data) => {
  sessionId.value = data.session_id
  const answerContent = data.answer?.content || '暂无回答'
  const sql = data.sql || null
  if (data.answer?.image && data.answer.image.length > 0) {
    const imageHtml = data.answer.image.map((img) => `\n\n![图表](${img})`).join('')
    addMessage('assistant', answerContent + imageHtml, { sql })
  } else {
    addMessage('assistant', answerContent, { sql })
  }
  isLoading.value = false
  resetProgress()
  scrollToBottom()
  fetchSessions()
}

const handleError = (data) => {
  addMessage('assistant', `**出错了**\n\n${data.message || '请求失败，请稍后重试'}`)
  isLoading.value = false
  resetProgress()
  scrollToBottom()
}

// ── 对话逻辑 ──

const sendMessage = async () => {
  const question = currentInput.value.trim()
  if (!question || isLoading.value) return
  addMessage('user', question)
  currentInput.value = ''
  isLoading.value = true
  resetProgress()
  await scrollToBottom()
  const payload = { question }
  if (sessionId.value) payload.session_id = sessionId.value
  sendChatMessageStream(payload, handleStep, handleToken, handleResult, handleError)
}

const handleKeydown = (event) => {
  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault()
    sendMessage()
  }
}

const newConversation = () => {
  messages.splice(0, messages.length)
  sessionId.value = null
  activeSessionId.value = null
}

// ── 选择会话 ──

const selectSession = async (session) => {
  activeSessionId.value = session.id
  sessionId.value = session.id
  messages.splice(0, messages.length)
  isHistoryLoading.value = true
  try {
    const response = await getChatDetail(session.id)
    const payload = response?.data || response
    const historyMessages = payload?.messages || []
    for (const msg of historyMessages) {
      if (msg.query) {
        addMessage('user', msg.query)
      }
      if (msg.answer) {
        addMessage('assistant', msg.answer, { sql: msg.sql_query || null })
      }
    }
    await scrollToBottom()
  } catch (error) {
    addMessage('system', `加载历史消息失败：${error.message || '请稍后重试'}`)
  } finally {
    isHistoryLoading.value = false
  }
}

// ── 删除会话 ──

const deleteSession = (session, event) => {
  event.stopPropagation()
  sessionToDelete.value = session
  confirmDialogVisible.value = true
}

const confirmDelete = async () => {
  if (!sessionToDelete.value) return
  isDeleting.value = true
  try {
    await deleteChatSession(sessionToDelete.value.id)
    if (activeSessionId.value === sessionToDelete.value.id) {
      newConversation()
    }
    sessions.items = sessions.items.filter((s) => s.id !== sessionToDelete.value.id)
    confirmDialogVisible.value = false
  } catch (error) {
    alert(error.message || '删除失败')
  } finally {
    isDeleting.value = false
  }
}

const cancelDelete = () => {
  confirmDialogVisible.value = false
  sessionToDelete.value = null
}

// ── 格式化 ──

const formatDateTime = (value) => {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date)
}

onMounted(() => {
  fetchSessions()
})
</script>

<template>
  <div class="flex h-full gap-4">
    <!-- 左侧：会话列表 -->
    <div class="flex w-72 shrink-0 flex-col overflow-hidden rounded-2xl border border-black/5 bg-white shadow-soft">
      <!-- 列表头部 -->
      <div class="flex items-center justify-between border-b border-black/5 px-4 py-3">
        <h3 class="text-sm font-semibold text-ink-900">历史对话</h3>
        <BaseButton icon="plus" size="xs" @click="newConversation">新对话</BaseButton>
      </div>

      <!-- 列表内容 -->
      <div class="min-h-0 flex-1 overflow-y-auto">
        <!-- 加载中 -->
        <div v-if="isListLoading && !hasSessions" class="flex items-center justify-center py-12">
          <FontAwesomeIcon :icon="['fas', 'spinner']" spin class="text-lg text-accent-500" aria-hidden="true" />
        </div>

        <!-- 空状态 -->
        <div v-else-if="!hasSessions" class="flex flex-col items-center justify-center px-4 py-12 text-center">
          <FontAwesomeIcon :icon="['fas', 'comments']" class="mb-3 text-2xl text-ink-300" aria-hidden="true" />
          <p class="text-sm text-ink-500">暂无对话记录</p>
          <p class="mt-1 text-xs text-ink-400">开始新对话后，记录会出现在这里</p>
        </div>

        <!-- 会话条目 -->
        <div v-for="session in sessions.items" :key="session.id">
          <button
            class="group flex w-full items-center gap-3 border-b border-black/5 px-4 py-3.5 text-left transition-all duration-150 hover:bg-accent-50/50"
            :class="activeSessionId === session.id ? 'bg-accent-50 border-l-2 border-l-accent-500' : 'border-l-2 border-l-transparent'"
            @click="selectSession(session)"
          >
            <div
              class="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg"
              :class="activeSessionId === session.id ? 'bg-accent-500 text-white' : 'bg-ink-100 text-ink-500'"
            >
              <FontAwesomeIcon :icon="['fas', 'message']" class="text-xs" aria-hidden="true" />
            </div>
            <div class="min-w-0 flex-1">
              <p class="truncate text-sm font-medium" :class="activeSessionId === session.id ? 'text-accent-700' : 'text-ink-900'">
                {{ session.session_name || '未命名对话' }}
              </p>
              <p class="mt-0.5 text-xs text-ink-400">{{ formatDateTime(session.created_at) }}</p>
            </div>
            <span
              v-if="session.status === 0"
              class="inline-block h-2 w-2 shrink-0 rounded-full bg-green-500"
              title="活跃"
            ></span>
            <button
              class="ml-1 flex h-6 w-6 shrink-0 items-center justify-center rounded-md text-ink-300 opacity-0 transition-opacity hover:bg-danger/10 hover:text-danger group-hover:opacity-100"
              title="删除会话"
              @click="deleteSession(session, $event)"
            >
              <FontAwesomeIcon :icon="['fas', 'trash']" class="text-[0.6em]" aria-hidden="true" />
            </button>
          </button>
        </div>
      </div>
    </div>

    <!-- 右侧：对话区域 -->
    <SurfacePanel :padded="false" class="flex min-w-0 flex-1 flex-col overflow-hidden">
      <!-- 对话头部 -->
      <div class="flex items-center justify-between border-b border-black/5 px-5 py-3 sm:px-6">
        <div>
          <p class="shell-kicker">Chat</p>
          <h2 class="text-base font-semibold text-ink-900">智能问数</h2>
        </div>
        <div class="flex items-center gap-2">
          <BaseButton
            v-if="hasMessages"
            variant="ghost"
            icon="rotate-right"
            size="xs"
            @click="newConversation"
          >
            新对话
          </BaseButton>
        </div>
      </div>

      <!-- 消息列表 -->
      <div class="chat-messages flex-1 overflow-y-auto px-5 py-5 sm:px-6">
        <!-- 加载历史消息 -->
        <div
          v-if="isHistoryLoading"
          class="flex flex-col items-center justify-center py-20 text-center"
        >
          <FontAwesomeIcon :icon="['fas', 'spinner']" spin class="text-2xl text-accent-500" aria-hidden="true" />
          <p class="mt-3 text-sm text-ink-500">加载历史消息...</p>
        </div>

        <!-- 空状态 -->
        <div
          v-else-if="!hasMessages"
          class="flex flex-col items-center justify-center py-20 text-center"
        >
          <div class="mb-5 flex h-16 w-16 items-center justify-center rounded-2xl bg-accent-50">
            <FontAwesomeIcon :icon="['fas', 'robot']" class="text-2xl text-accent-500" aria-hidden="true" />
          </div>
          <h3 class="text-base font-semibold text-ink-900">开始提问</h3>
          <p class="mt-1 max-w-md text-sm text-ink-500">
            输入关于上市公司财务数据的问题，例如：<br />
            "贵州茅台2023年净利润是多少？"
          </p>
          <div class="mt-4 flex flex-wrap gap-2">
            <button
              v-for="suggestion in ['贵州茅台2023年净利润', '营收排名前10的公司', '对比五粮液和茅台', '新能源汽车行业研报']"
              :key="suggestion"
              class="rounded-lg border border-black/10 px-3 py-1.5 text-xs text-ink-500 transition-colors hover:border-accent-200 hover:bg-accent-50 hover:text-accent-600"
              @click="currentInput = suggestion"
            >
              {{ suggestion }}
            </button>
          </div>
        </div>

        <!-- 消息气泡 -->
        <div v-for="msg in messages" :key="msg.id" class="mb-4 last:mb-0">
          <div class="flex gap-3" :class="msg.role === 'user' ? 'flex-row-reverse' : ''">
            <div
              class="mt-1 flex h-8 w-8 shrink-0 items-center justify-center rounded-xl"
              :class="msg.role === 'user' ? 'bg-accent-500' : 'bg-ink-100'"
            >
              <FontAwesomeIcon
                :icon="['fas', msg.role === 'user' ? 'user' : 'robot']"
                class="text-sm"
                :class="msg.role === 'user' ? 'text-white' : 'text-ink-600'"
                aria-hidden="true"
              />
            </div>
            <div
              class="max-w-[75%] rounded-2xl px-4 py-3"
              :class="msg.role === 'user'
                ? 'bg-accent-500 text-white'
                : 'border border-black/5 bg-white text-ink-900'"
            >
              <div
                v-if="msg.role === 'assistant'"
                class="prose prose-sm max-w-none prose-headings:text-ink-900 prose-p:text-ink-700 prose-a:text-accent-600 prose-code:text-accent-700 prose-code:bg-ink-50 prose-code:px-1 prose-code:rounded prose-strong:text-ink-900"
                v-html="msg.renderedHtml"
              ></div>
              <p v-else class="text-sm leading-6">{{ msg.content }}</p>
              <div v-if="msg.sql" class="mt-3 border-t border-black/10 pt-2">
                <button
                  class="inline-flex items-center gap-1.5 text-xs font-medium text-accent-600 hover:text-accent-700"
                  @click="toggleSql(msg)"
                >
                  <FontAwesomeIcon :icon="['fas', msg.showSql ? 'chevron-down' : 'chevron-right']" class="text-[0.6em]" aria-hidden="true" />
                  {{ msg.showSql ? '隐藏 SQL' : '查看 SQL' }}
                </button>
                <pre v-if="msg.showSql" class="mt-2 overflow-x-auto rounded-lg bg-ink-50 p-3 text-xs text-ink-600"><code>{{ msg.sql }}</code></pre>
              </div>
            </div>
          </div>
          <div v-if="msg.role === 'assistant'" class="flex gap-2 pl-11 pt-1">
            <button
              class="inline-flex items-center gap-1 text-xs text-ink-400 hover:text-ink-600 transition-colors"
              @click="copyText(msg.content, msg.id)"
            >
              <FontAwesomeIcon :icon="['fas', copiedId === msg.id ? 'check' : 'copy']" class="text-[0.65em]" aria-hidden="true" />
              {{ copiedId === msg.id ? '已复制' : '复制' }}
            </button>
          </div>
        </div>

        <!-- 加载中 - 步骤进度 -->
        <div v-if="isLoading" class="mb-4 flex gap-3">
          <div class="mt-1 flex h-8 w-8 shrink-0 items-center justify-center rounded-xl bg-ink-100">
            <FontAwesomeIcon :icon="['fas', 'robot']" class="text-sm text-ink-600" aria-hidden="true" />
          </div>
          <div class="min-w-[200px] rounded-2xl border border-black/5 bg-white px-4 py-3">
            <div class="flex flex-col gap-3">
              <!-- 基础步（intent / answer） -->
              <div
                v-for="stepKey in STEP_ORDER.filter(k => STEP_CONFIG[k].group === 'base')"
                :key="stepKey"
                class="flex items-center gap-2"
                :class="progressSteps[stepKey].status === 'active' ? 'text-accent-600' : progressSteps[stepKey].status === 'done' ? 'text-green-600' : 'text-ink-300'"
              >
                <FontAwesomeIcon
                  v-if="progressSteps[stepKey].status === 'done'"
                  :icon="['fas', 'circle-check']"
                  class="w-4 text-xs"
                  aria-hidden="true"
                />
                <FontAwesomeIcon
                  v-else-if="progressSteps[stepKey].status === 'active'"
                  :icon="['fas', 'spinner']"
                  spin
                  class="w-4 text-xs"
                  aria-hidden="true"
                />
                <div v-else class="flex w-4 items-center justify-center">
                  <span class="block h-2 w-2 rounded-full border border-current"></span>
                </div>
                <span class="text-xs font-medium">{{ STEP_CONFIG[stepKey].label }}</span>
              </div>

              <!-- SQL 组 -->
              <div v-if="activeGroups.includes('sql')">
                <div class="mb-1.5 flex items-center gap-1.5">
                  <span class="h-px flex-1 bg-accent-200"></span>
                  <span class="text-[10px] font-medium uppercase tracking-wider text-accent-500">{{ STEP_GROUP_CONFIG.sql.label }}</span>
                  <span class="h-px flex-1 bg-accent-200"></span>
                </div>
                <div
                  v-for="stepKey in ['sql', 'query']"
                  :key="stepKey"
                  class="flex items-center gap-2"
                  :class="progressSteps[stepKey].status === 'active' ? 'text-accent-600' : progressSteps[stepKey].status === 'done' ? 'text-green-600' : 'text-ink-300'"
                >
                  <FontAwesomeIcon
                    v-if="progressSteps[stepKey].status === 'done'"
                    :icon="['fas', 'circle-check']"
                    class="w-4 text-xs"
                    aria-hidden="true"
                  />
                  <FontAwesomeIcon
                    v-else-if="progressSteps[stepKey].status === 'active'"
                    :icon="['fas', 'spinner']"
                    spin
                    class="w-4 text-xs"
                    aria-hidden="true"
                  />
                  <div v-else class="flex w-4 items-center justify-center">
                    <span class="block h-2 w-2 rounded-full border border-current"></span>
                  </div>
                  <span class="text-xs font-medium">{{ STEP_CONFIG[stepKey].label }}</span>
                </div>
              </div>

              <!-- RAG 组 -->
              <div v-if="activeGroups.includes('rag')">
                <div class="mb-1.5 flex items-center gap-1.5">
                  <span class="h-px flex-1 bg-emerald-200"></span>
                  <span class="text-[10px] font-medium uppercase tracking-wider text-emerald-600">{{ STEP_GROUP_CONFIG.rag.label }}</span>
                  <span class="h-px flex-1 bg-emerald-200"></span>
                </div>
                <div
                  class="flex items-center gap-2"
                  :class="progressSteps['rag'].status === 'active' ? 'text-emerald-600' : progressSteps['rag'].status === 'done' ? 'text-green-600' : 'text-ink-300'"
                >
                  <FontAwesomeIcon
                    v-if="progressSteps['rag'].status === 'done'"
                    :icon="['fas', 'circle-check']"
                    class="w-4 text-xs"
                    aria-hidden="true"
                  />
                  <FontAwesomeIcon
                    v-else-if="progressSteps['rag'].status === 'active'"
                    :icon="['fas', 'spinner']"
                    spin
                    class="w-4 text-xs"
                    aria-hidden="true"
                  />
                  <div v-else class="flex w-4 items-center justify-center">
                    <span class="block h-2 w-2 rounded-full border border-current"></span>
                  </div>
                  <FontAwesomeIcon :icon="['fas', 'microchip']" class="w-3.5 text-[0.65em]" aria-hidden="true" />
                  <span class="text-xs font-medium">{{ STEP_CONFIG['rag'].label }}</span>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- 流式回答 -->
        <div v-if="isLoading && streamingContent" class="mb-4 flex gap-3">
          <div class="mt-1 flex h-8 w-8 shrink-0 items-center justify-center rounded-xl bg-ink-100">
            <FontAwesomeIcon :icon="['fas', 'robot']" class="text-sm text-ink-600" aria-hidden="true" />
          </div>
          <div class="max-w-[75%] rounded-2xl border border-black/5 bg-white px-4 py-3">
            <div
              class="streaming-cursor prose prose-sm max-w-none prose-headings:text-ink-900 prose-p:text-ink-700 prose-a:text-accent-600 prose-code:text-accent-700 prose-code:bg-ink-50 prose-code:px-1 prose-code:rounded prose-strong:text-ink-900"
              v-html="renderMarkdown(streamingContent)"
            ></div>
          </div>
        </div>
      </div>

      <!-- 输入区域 -->
      <div class="border-t border-black/5 px-5 py-4 sm:px-6">
        <div class="flex items-end gap-3">
          <BaseInput
            v-model="currentInput"
            class="flex-1"
            placeholder="输入财务数据问题..."
            :disabled="isLoading"
            @keydown="handleKeydown"
          />
          <BaseButton
            icon="paper-plane"
            icon-only
            :disabled="!canSend"
            :loading="isLoading"
            aria-label="发送"
            @click="sendMessage"
          />
        </div>
        <p class="mt-2 text-xs text-ink-400">
          支持查询：净利润、营业收入、总资产等财务指标，支持同比/环比分析、排名对比、研报知识库检索
        </p>
      </div>
    </SurfacePanel>
  </div>

  <ConfirmDialog
    :visible="confirmDialogVisible"
    title="删除会话"
    :message="`确定要删除会话「${sessionToDelete?.session_name || '未命名对话'}」吗？此操作不可撤销。`"
    confirm-text="删除"
    cancel-text="取消"
    :loading="isDeleting"
    @confirm="confirmDelete"
    @close="cancelDelete"
  />
</template>

<style scoped>
.streaming-cursor > :last-child::after {
  content: '';
  display: inline-block;
  width: 2px;
  height: 1em;
  margin-left: 2px;
  background-color: #3b82f6;
  animation: blink 0.8s ease-in-out infinite;
  vertical-align: baseline;
}
</style>
