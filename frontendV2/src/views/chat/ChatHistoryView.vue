<script setup>
/**
 * 智能问数历史列表页
 * 功能描述：展示历史对话会话列表，支持分页、点击进入对话
 * 依赖组件：SurfacePanel, AppEmptyState, BaseButton
 */
import { ref, reactive, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import BaseButton from '@/components/ui/BaseButton.vue'
import SurfacePanel from '@/components/ui/SurfacePanel.vue'
import AppEmptyState from '@/components/ui/AppEmptyState.vue'
import ConfirmDialog from '@/components/ui/ConfirmDialog.vue'
import { getChatList, deleteChatSession } from '@/api/chat'

const router = useRouter()

// ── 状态 ──

const listState = reactive({
  items: [],
  page: 1,
  pageSize: 10,
  total: 0
})

const isLoading = ref(false)
const errorMessage = ref('')
const confirmDialogVisible = ref(false)
const sessionToDelete = ref(null)
const isDeleting = ref(false)

// ── 计算属性 ──

const hasRecords = computed(() => listState.items.length > 0)

const totalPages = computed(() => {
  const pageSize = Number(listState.pageSize) || 10
  const total = Number(listState.total) || 0
  return Math.max(1, Math.ceil(total / pageSize))
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

// ── 数据请求 ──

const fetchChatList = async () => {
  isLoading.value = true
  errorMessage.value = ''

  try {
    const params = {
      page: listState.page,
      page_size: listState.pageSize
    }
    const response = await getChatList(params)
    const payload = response?.data || response
    const items = payload?.lists || payload?.items || []
    const pagination = payload?.pagination || {}
    listState.items = items
    listState.total = pagination?.total || items.length
  } catch (error) {
    errorMessage.value = error.message || '加载会话列表失败'
  } finally {
    isLoading.value = false
  }
}

// ── 事件处理 ──

const goToChat = (session) => {
  router.push({ name: 'Chat', query: { session_id: session.id } })
}

const goToNewChat = () => {
  router.push({ name: 'Chat' })
}

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
    listState.items = listState.items.filter((s) => s.id !== sessionToDelete.value.id)
    listState.total = Math.max(0, listState.total - 1)
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

onMounted(() => {
  fetchChatList()
})
</script>

<template>
  <div class="flex h-full flex-col">
    <!-- 页头 -->
    <div class="mb-5 flex items-center justify-between">
      <div>
        <p class="shell-kicker">Chat</p>
        <h2 class="mt-2 text-xl font-semibold text-ink-900">历史对话</h2>
        <p class="mt-2 max-w-3xl text-sm leading-6 text-ink-600">
          查看历史对话记录，点击任一会话继续对话。
        </p>
      </div>
      <BaseButton icon="plus" @click="goToNewChat">新对话</BaseButton>
    </div>

    <!-- 列表区域 -->
    <SurfacePanel :padded="false" class="flex flex-1 flex-col overflow-hidden">
      <!-- 加载中 -->
      <div
        v-if="isLoading && !hasRecords"
        class="flex flex-col items-center justify-center py-20"
      >
        <FontAwesomeIcon
          :icon="['fas', 'spinner']"
          spin
          class="text-3xl text-accent-500"
          aria-hidden="true"
        />
        <p class="mt-4 text-sm text-ink-500">正在加载会话列表...</p>
      </div>

      <!-- 错误状态 -->
      <div
        v-else-if="errorMessage && !hasRecords"
        class="flex flex-col items-center justify-center py-20"
      >
        <p class="text-sm text-danger">{{ errorMessage }}</p>
        <BaseButton variant="secondary" class="mt-4" @click="fetchChatList">重试</BaseButton>
      </div>

      <!-- 空状态 -->
      <AppEmptyState
        v-else-if="!hasRecords"
        title="暂无对话记录"
        description="开始一次新的智能问数对话，记录会出现在这里。"
      >
        <BaseButton icon="plus" @click="goToNewChat">开始新对话</BaseButton>
      </AppEmptyState>

      <!-- 会话列表 -->
      <div v-else class="flex flex-1 flex-col overflow-hidden">
        <div class="min-h-0 flex-1 overflow-auto px-5 py-5 sm:px-6">
          <div class="space-y-3">
            <div
              v-for="session in listState.items"
              :key="session.id"
              class="group flex cursor-pointer items-center gap-4 rounded-2xl border border-black/5 bg-white px-5 py-4 transition-all duration-200 hover:border-accent-200 hover:shadow-soft"
              @click="goToChat(session)"
            >
              <!-- 图标 -->
              <div class="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-accent-50 text-accent-500">
                <FontAwesomeIcon :icon="['fas', 'robot']" class="text-base" aria-hidden="true" />
              </div>

              <!-- 内容 -->
              <div class="min-w-0 flex-1">
                <p class="truncate text-sm font-medium text-ink-900">
                  {{ session.session_name || '未命名对话' }}
                </p>
                <div class="mt-1 flex items-center gap-3 text-xs text-ink-500">
                  <span>{{ formatDateTime(session.created_at) }}</span>
                  <span
                    class="inline-flex items-center gap-1"
                    :class="session.status === 0 ? 'text-green-600' : 'text-ink-400'"
                  >
                    <span class="inline-block h-1.5 w-1.5 rounded-full" :class="session.status === 0 ? 'bg-green-500' : 'bg-ink-300'"></span>
                    {{ session.status === 0 ? '活跃' : '已关闭' }}
                  </span>
                </div>
              </div>

              <!-- 箭头 -->
              <FontAwesomeIcon
                :icon="['fas', 'chevron-right']"
                class="shrink-0 text-sm text-ink-300 transition-all duration-200 group-hover:text-accent-500 group-hover:translate-x-0.5"
                aria-hidden="true"
              />
              <button
                class="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg text-ink-300 opacity-0 transition-opacity hover:bg-danger/10 hover:text-danger group-hover:opacity-100"
                title="删除会话"
                @click="deleteSession(session, $event)"
              >
                <FontAwesomeIcon :icon="['fas', 'trash']" class="text-xs" aria-hidden="true" />
              </button>
            </div>
          </div>
        </div>

        <!-- 分页 -->
        <div
          class="flex flex-col gap-4 border-t border-black/5 px-5 py-4 text-sm text-ink-500 sm:flex-row sm:items-center sm:justify-between"
        >
          <p>第 {{ listState.page }} / {{ totalPages }} 页，共 {{ listState.total }} 条</p>
          <div class="flex items-center gap-2">
            <BaseButton variant="secondary" size="sm" :disabled="listState.page <= 1" @click="listState.page--; fetchChatList()">上一页</BaseButton>
            <BaseButton variant="secondary" size="sm" :disabled="listState.page >= totalPages" @click="listState.page++; fetchChatList()">下一页</BaseButton>
          </div>
        </div>
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
