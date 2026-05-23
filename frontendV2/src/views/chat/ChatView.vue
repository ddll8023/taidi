<script setup>
/**
 * 智能问数页面
 * 功能描述：对话式财务数据查询，支持多轮问答、Markdown 渲染、SQL 展示、SSE 流式进度
 * 依赖组件：SurfacePanel, BaseInput, BaseButton
 */
import { ref, reactive, computed, nextTick } from 'vue'
import BaseInput from '@/components/ui/BaseInput.vue'
import BaseButton from '@/components/ui/BaseButton.vue'
import SurfacePanel from '@/components/ui/SurfacePanel.vue'
import { sendChatMessageStream } from '@/api/chat'
import { renderMarkdown } from '@/utils/markdown'

// ── 步骤配置 ──

const STEP_CONFIG = {
  intent: { label: '意图识别', icon: 'brain' },
  sql: { label: '生成查询语句', icon: 'database' },
  query: { label: '查询数据', icon: 'magnifying-glass' },
  answer: { label: '综合分析', icon: 'wand-magic-sparkles' },
}

const STEP_ORDER = ['intent', 'sql', 'query', 'answer']

// ── 状态 ──

const messages = reactive([])
const currentInput = ref('')
const isLoading = ref(false)
const sessionId = ref(null)
const copiedId = ref(null)
const progressSteps = reactive({
  intent: { status: 'pending' },
  sql: { status: 'pending' },
  query: { status: 'pending' },
  answer: { status: 'pending' },
})

// ── 计算属性 ──

const hasMessages = computed(() => messages.length > 0)

const canSend = computed(() => currentInput.value.trim().length > 0 && !isLoading.value)

const progressMessage = computed(() => {
  const active = STEP_ORDER.find((key) => progressSteps[key].status === 'active')
  if (active) {
    return STEP_CONFIG[active].label
  }
  return '正在分析...'
})

// ── 消息操作 ──

const addMessage = (role, content, extra = {}) => {
  messages.push({
    id: Date.now() + Math.random(),
    role,
    content,
    renderedHtml: role === 'assistant' ? renderMarkdown(content) : '',
    sql: extra.sql || null,
    chartType: extra.chartType || null,
    showSql: false,
  })
}

const scrollToBottom = async () => {
  await nextTick()
  const container = document.querySelector('.chat-messages')
  if (container) {
    container.scrollTop = container.scrollHeight
  }
}

const toggleSql = (msg) => {
  msg.showSql = !msg.showSql
}

const copyText = async (text, msgId) => {
  try {
    await navigator.clipboard.writeText(text)
    copiedId.value = msgId
    setTimeout(() => { copiedId.value = null }, 2000)
  } catch {
    // fallback
  }
}

const resetProgress = () => {
  STEP_ORDER.forEach((key) => {
    progressSteps[key].status = 'pending'
  })
}

// ── SSE 回调 ──

const handleStep = (data) => {
  const step = data.step
  if (!step) return

  // 去除 _done 后缀得到步骤 key
  const stepKey = step.replace('_done', '')
  if (!STEP_ORDER.includes(stepKey)) return

  if (step.endsWith('_done')) {
    progressSteps[stepKey].status = 'done'
  } else {
    progressSteps[stepKey].status = 'active'
  }
}

const handleResult = (data) => {
  sessionId.value = data.session_id

  const answerContent = data.answer?.content || '暂无回答'
  const sql = data.sql || null

  if (data.answer?.image && data.answer.image.length > 0) {
    const imageHtml = data.answer.image
      .map((img) => `\n\n![图表](${img})`)
      .join('')
    addMessage('assistant', answerContent + imageHtml, { sql })
  } else {
    addMessage('assistant', answerContent, { sql })
  }

  isLoading.value = false
  resetProgress()
  scrollToBottom()
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
  if (sessionId.value) {
    payload.session_id = sessionId.value
  }

  sendChatMessageStream(payload, handleStep, handleResult, handleError)
}

const handleKeydown = (event) => {
  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault()
    sendMessage()
  }
}

const clearConversation = () => {
  messages.splice(0, messages.length)
  sessionId.value = null
}
</script>

<template>
  <div class="flex h-full flex-col">
    <!-- 页头 -->
    <div class="mb-5 flex items-center justify-between">
      <div>
        <p class="shell-kicker">Chat</p>
        <h2 class="mt-2 text-xl font-semibold text-ink-900">智能问数</h2>
        <p class="mt-2 max-w-3xl text-sm leading-6 text-ink-600">
          输入自然语言问题，AI 自动查询财务数据并生成分析回答。
        </p>
      </div>
      <BaseButton
        v-if="hasMessages"
        variant="ghost"
        icon="rotate-right"
        size="sm"
        @click="clearConversation"
      >
        新对话
      </BaseButton>
    </div>

    <!-- 聊天区域 -->
    <SurfacePanel :padded="false" class="flex flex-1 flex-col overflow-hidden">
      <!-- 消息列表 -->
      <div class="chat-messages flex-1 overflow-y-auto px-5 py-5 sm:px-6">
        <!-- 空状态 -->
        <div
          v-if="!hasMessages"
          class="flex flex-col items-center justify-center py-20 text-center"
        >
          <div class="mb-5 flex h-16 w-16 items-center justify-center rounded-2xl bg-accent-50">
            <FontAwesomeIcon
              :icon="['fas', 'robot']"
              class="text-2xl text-accent-500"
              aria-hidden="true"
            />
          </div>
          <h3 class="text-base font-semibold text-ink-900">开始提问</h3>
          <p class="mt-1 max-w-md text-sm text-ink-500">
            输入关于上市公司财务数据的问题，例如：<br />
            "贵州茅台2023年净利润是多少？"
          </p>
          <div class="mt-4 flex flex-wrap gap-2">
            <button
              v-for="suggestion in ['贵州茅台2023年净利润', '营收排名前10的公司', '对比五粮液和茅台']"
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
          <div
            class="flex gap-3"
            :class="msg.role === 'user' ? 'flex-row-reverse' : ''"
          >
            <!-- 头像 -->
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

            <!-- 内容 -->
            <div
              class="max-w-[75%] rounded-2xl px-4 py-3"
              :class="msg.role === 'user'
                ? 'bg-accent-500 text-white'
                : 'border border-black/5 bg-white text-ink-900'"
            >
              <!-- 助手回答（Markdown） -->
              <div
                v-if="msg.role === 'assistant'"
                class="prose prose-sm max-w-none prose-headings:text-ink-900 prose-p:text-ink-700 prose-a:text-accent-600 prose-code:text-accent-700 prose-code:bg-ink-50 prose-code:px-1 prose-code:rounded prose-strong:text-ink-900"
                v-html="msg.renderedHtml"
              ></div>

              <!-- 用户消息 -->
              <p v-else class="text-sm leading-6">{{ msg.content }}</p>

              <!-- SQL 区块 -->
              <div v-if="msg.sql" class="mt-3 border-t border-black/10 pt-2">
                <button
                  class="inline-flex items-center gap-1.5 text-xs font-medium text-accent-600 hover:text-accent-700"
                  @click="toggleSql(msg)"
                >
                  <FontAwesomeIcon
                    :icon="['fas', msg.showSql ? 'chevron-down' : 'chevron-right']"
                    class="text-[0.6em]"
                    aria-hidden="true"
                  />
                  {{ msg.showSql ? '隐藏 SQL' : '查看 SQL' }}
                </button>
                <pre
                  v-if="msg.showSql"
                  class="mt-2 overflow-x-auto rounded-lg bg-ink-50 p-3 text-xs text-ink-600"
                ><code>{{ msg.sql }}</code></pre>
              </div>
            </div>
          </div>

          <!-- 操作按钮（仅助手消息） -->
          <div
            v-if="msg.role === 'assistant'"
            class="flex gap-2 pl-11 pt-1"
          >
            <button
              class="inline-flex items-center gap-1 text-xs text-ink-400 hover:text-ink-600 transition-colors"
              @click="copyText(msg.content, msg.id)"
            >
              <FontAwesomeIcon
                :icon="['fas', copiedId === msg.id ? 'check' : 'copy']"
                class="text-[0.65em]"
                aria-hidden="true"
              />
              {{ copiedId === msg.id ? '已复制' : '复制' }}
            </button>
          </div>
        </div>

        <!-- 加载中 - 步骤进度 -->
        <div v-if="isLoading" class="mb-4 flex gap-3">
          <div class="mt-1 flex h-8 w-8 shrink-0 items-center justify-center rounded-xl bg-ink-100">
            <FontAwesomeIcon
              :icon="['fas', 'robot']"
              class="text-sm text-ink-600"
              aria-hidden="true"
            />
          </div>
          <div class="min-w-[200px] rounded-2xl border border-black/5 bg-white px-4 py-3">
            <div class="flex flex-col gap-2.5">
              <div
                v-for="(stepKey, idx) in STEP_ORDER"
                :key="stepKey"
                class="flex items-center gap-2"
                :class="progressSteps[stepKey].status === 'active' ? 'text-accent-600' : progressSteps[stepKey].status === 'done' ? 'text-green-600' : 'text-ink-300'"
              >
                <!-- 状态图标 -->
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
                <div
                  v-else
                  class="flex w-4 items-center justify-center"
                >
                  <span class="block h-2 w-2 rounded-full border border-current"></span>
                </div>
                <!-- 步骤名称 -->
                <span class="text-xs font-medium">{{ STEP_CONFIG[stepKey].label }}</span>
              </div>
            </div>
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
          支持查询：净利润、营业收入、总资产等财务指标，支持同比/环比分析、排名对比
        </p>
      </div>
    </SurfacePanel>
  </div>
</template>
