<script setup>
import { ref, nextTick, onMounted, onUnmounted, watch, computed } from 'vue'
import { renderMarkdown } from '@/utils/markdown'
import { useChatStore } from '@/stores/chat'

const chatStore = useChatStore()

const inputText = ref('')
const messagesContainer = ref(null)
const showSqlMap = ref({})
const showImageMap = ref({})
const editingSessionId = ref(null)
const editingName = ref('')
const showMenuMap = ref({})


onMounted(() => {
  chatStore.loadSessions()
  document.addEventListener('click', closeAllMenus)
})

onUnmounted(() => {
  document.removeEventListener('click', closeAllMenus)
})

watch(
  () => chatStore.messages.length,
  () => {
    nextTick(() => {
      scrollToBottom()
    })
  }
)

function scrollToBottom() {
  if (messagesContainer.value) {
    messagesContainer.value.scrollTop = messagesContainer.value.scrollHeight
  }
}

async function handleSend() {
  const question = inputText.value.trim()
  if (!question || chatStore.isSending) return

  inputText.value = ''
  await chatStore.sendMessage(question)
  scrollToBottom()
}

function handleKeydown(event) {
  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault()
    handleSend()
  }
}

function handleNewSession() {
  chatStore.startNewSession()
}

async function handleSelectSession(session) {
  await chatStore.loadHistory(session.id)
  scrollToBottom()
}

function toggleSql(index) {
  showSqlMap.value[index] = !showSqlMap.value[index]
}

function toggleImage(index) {
  showImageMap.value[index] = !showImageMap.value[index]
}

function toggleMenu(sessionId) {
  showMenuMap.value[sessionId] = !showMenuMap.value[sessionId]
}

function closeAllMenus() {
  showMenuMap.value = {}
}

function startRename(session) {
  editingSessionId.value = session.id
  editingName.value = session.name || session.id.slice(0, 8)
  closeAllMenus()
}

async function confirmRename(sessionId) {
  if (editingName.value.trim()) {
    await chatStore.renameSessionById(sessionId, editingName.value.trim())
  }
  editingSessionId.value = null
  editingName.value = ''
}

function cancelRename() {
  editingSessionId.value = null
  editingName.value = ''
}

async function handleDelete(sessionId) {
  if (confirm('确定要删除这个会话吗？')) {
    await chatStore.deleteSessionById(sessionId)
  }
  closeAllMenus()
}

function formatTime(dateStr) {
  if (!dateStr) return ''
  const d = new Date(dateStr)
  return d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })
}
</script>

<template>
  <div class="flex h-full items-stretch gap-4">
    <div class="flex w-64 shrink-0 flex-col overflow-hidden rounded-[28px] border border-black/5 bg-white/80 p-4">
      <div class="mb-4 flex shrink-0 items-center justify-between">
        <h3 class="text-sm font-semibold text-ink-900">会话列表</h3>
        <button
          class="flex h-8 w-8 items-center justify-center rounded-full bg-ink-900 text-white transition-colors hover:bg-ink-700"
          @click="handleNewSession"
          title="新建会话"
        >
          <FontAwesomeIcon :icon="['fas', 'plus']" class="text-xs" />
        </button>
      </div>

      <div class="flex-1 overflow-y-auto">
        <div
          v-for="session in chatStore.activeSessions"
          :key="session.id"
          class="relative mb-2"
        >
          <div
            v-if="editingSessionId === session.id"
            class="w-full rounded-xl bg-ink-50 p-2"
          >
            <input
              v-model="editingName"
              type="text"
              class="w-full rounded-lg border border-ink-200 px-2 py-1 text-sm outline-none focus:border-ink-400"
              @keydown.enter="confirmRename(session.id)"
              @keydown.escape="cancelRename"
            />
            <div class="mt-2 flex gap-2">
              <button
                class="flex-1 rounded-lg bg-ink-900 px-2 py-1 text-xs text-white hover:bg-ink-700"
                @click="confirmRename(session.id)"
              >
                确认
              </button>
              <button
                class="flex-1 rounded-lg border border-ink-200 px-2 py-1 text-xs text-ink-600 hover:bg-ink-100"
                @click="cancelRename"
              >
                取消
              </button>
            </div>
          </div>

          <div
            v-else
            class="flex w-full items-center rounded-xl px-3 py-2 text-left text-sm transition-colors"
            :class="{
              'bg-ink-900 text-white': chatStore.currentSessionId === session.id,
              'bg-ink-50 text-ink-700 hover:bg-ink-100': chatStore.currentSessionId !== session.id
            }"
          >
            <button
              class="min-w-0 flex-1"
              @click="handleSelectSession(session)"
            >
              <p class="truncate font-medium">{{ session.name || session.id.slice(0, 8) }}...</p>
              <p class="mt-1 text-xs opacity-60">{{ formatTime(session.updated_at) }}</p>
            </button>

            <div class="relative ml-2">
              <button
                class="flex h-6 w-6 items-center justify-center rounded-full transition-colors"
                :class="{
                  'hover:bg-white/20': chatStore.currentSessionId === session.id,
                  'hover:bg-ink-200': chatStore.currentSessionId !== session.id
                }"
                @click.stop="toggleMenu(session.id)"
              >
                <FontAwesomeIcon :icon="['fas', 'ellipsis-vertical']" class="text-xs" />
              </button>

              <div
                v-if="showMenuMap[session.id]"
                class="absolute right-0 top-full z-10 mt-1 w-24 rounded-lg border border-black/5 bg-white py-1 shadow-lg"
              >
                <button
                  class="flex w-full items-center gap-2 px-3 py-2 text-left text-xs text-ink-700 hover:bg-ink-50"
                  @click="startRename(session)"
                >
                  <FontAwesomeIcon :icon="['fas', 'edit']" />
                  <span>重命名</span>
                </button>
                <button
                  class="flex w-full items-center gap-2 px-3 py-2 text-left text-xs text-red-600 hover:bg-red-50"
                  @click="handleDelete(session.id)"
                >
                  <FontAwesomeIcon :icon="['fas', 'trash']" />
                  <span>删除</span>
                </button>
              </div>
            </div>
          </div>
        </div>

        <p v-if="chatStore.activeSessions.length === 0" class="py-4 text-center text-xs text-ink-400">
          暂无会话
        </p>
      </div>
    </div>

    <div class="flex flex-1 flex-col overflow-hidden rounded-[28px] border border-black/5 bg-white/80">
      <div ref="messagesContainer" class="flex-1 overflow-y-auto p-6">
        <div v-if="chatStore.messages.length === 0" class="flex h-full flex-col items-center justify-center">
          <div class="flex h-16 w-16 items-center justify-center rounded-[24px] bg-ink-900 text-white shadow-soft">
            <FontAwesomeIcon :icon="['fas', 'comments']" class="text-xl" />
          </div>
          <h3 class="mt-6 text-xl font-semibold text-ink-900">智能问数助手</h3>
          <p class="mt-3 max-w-md text-center text-sm leading-6 text-ink-600">
            输入自然语言问题，查询上市公司财务数据。支持单值查询、趋势分析、公司对比等。
          </p>
          <div class="mt-6 flex flex-wrap justify-center gap-2">
            <button
              v-for="suggestion in ['金花股份2025年Q3利润总额是多少', '近几年净利润变化趋势', '2024年营业收入最高的top5企业']"
              :key="suggestion"
              class="rounded-full border border-ink-200 px-4 py-2 text-xs text-ink-600 transition-colors hover:bg-ink-50"
              @click="inputText = suggestion"
            >
              {{ suggestion }}
            </button>
          </div>
        </div>

        <div v-for="(msg, index) in chatStore.messages" :key="index" class="mb-4">
          <div v-if="msg.role === 'user'" class="flex justify-end">
            <div class="max-w-[70%] rounded-2xl rounded-tr-sm bg-ink-900 px-4 py-3 text-sm text-white">
              {{ msg.content }}
            </div>
          </div>

          <div v-else class="flex justify-start">
            <div class="max-w-[70%] rounded-2xl rounded-tl-sm border border-black/5 bg-ink-50 px-4 py-3">
              <div class="prose prose-sm prose-ink max-w-none text-sm leading-6 text-ink-800" v-html="renderMarkdown(msg.content)"></div>

              <div v-if="msg.image && msg.image.length > 0" class="mt-3">
                <button
                  class="flex items-center gap-1 text-xs text-ink-500 transition-colors hover:text-ink-700"
                  @click="toggleImage(index)"
                >
                  <FontAwesomeIcon :icon="['fas', 'chart-line']" />
                  <span>{{ showImageMap[index] ? '收起图表' : '查看图表' }}</span>
                </button>
                <div v-if="showImageMap[index]" class="mt-2 space-y-2">
                  <img
                    v-for="(img, imgIdx) in msg.image"
                    :key="imgIdx"
                    :src="img"
                    :alt="`图表 ${imgIdx + 1}`"
                    class="max-w-full rounded-lg border border-black/5"
                  />
                </div>
              </div>

              <div v-if="msg.sql" class="mt-3">
                <button
                  class="flex items-center gap-1 text-xs text-ink-500 transition-colors hover:text-ink-700"
                  @click="toggleSql(index)"
                >
                  <FontAwesomeIcon :icon="['fas', 'database']" />
                  <span>{{ showSqlMap[index] ? '收起SQL' : '查看SQL' }}</span>
                </button>
                <div v-if="showSqlMap[index]" class="mt-2 overflow-x-auto rounded-lg bg-ink-900 p-3">
                  <pre class="text-xs text-green-400">{{ msg.sql }}</pre>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div v-if="chatStore.isSending" class="flex justify-start">
          <div class="max-w-[70%] rounded-2xl rounded-tl-sm border border-black/5 bg-ink-50 px-4 py-3">
            <div class="flex items-center gap-2 text-sm text-ink-500">
              <FontAwesomeIcon :icon="['fas', 'spinner']" class="animate-spin" />
              <span>正在思考...</span>
            </div>
          </div>
        </div>
      </div>

      <div class="border-t border-black/5 p-4 shrink-0">
        <div class="flex gap-3">
          <input
            v-model="inputText"
            type="text"
            placeholder="输入您的财务数据问题..."
            class="flex-1 rounded-xl border border-ink-200 bg-white px-4 py-3 text-sm text-ink-900 outline-none transition-colors placeholder:text-ink-400 focus:border-ink-400"
            :disabled="chatStore.isSending"
            @keydown="handleKeydown"
          />
          <button
            class="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-ink-900 text-white transition-colors hover:bg-ink-700 disabled:opacity-50"
            :disabled="chatStore.isSending || !inputText.trim()"
            @click="handleSend"
          >
            <FontAwesomeIcon :icon="['fas', 'paper-plane']" class="text-sm" />
          </button>
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

</style>
