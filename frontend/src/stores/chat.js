import { ref, computed } from 'vue'
import { defineStore } from 'pinia'
import { sendMessage as apiSendMessage, getSessions, getChatHistory, closeSession as apiCloseSession, deleteSession as apiDeleteSession, renameSession as apiRenameSession } from '@/api/chat'

export const useChatStore = defineStore('chat', () => {
  const currentSessionId = ref(null)
  const messages = ref([])
  const sessions = ref([])
  const isLoading = ref(false)
  const isSending = ref(false)
  const error = ref(null)

  const activeSessions = computed(() => sessions.value)

  async function sendMessage(question) {
    if (!question.trim() || isSending.value) return

    const isNewSession = !currentSessionId.value
    isSending.value = true
    error.value = null

    messages.value.push({
      role: 'user',
      content: question,
      created_at: new Date().toISOString()
    })

    try {
      const result = await apiSendMessage(currentSessionId.value, question)

      if (!currentSessionId.value) {
        currentSessionId.value = result.data?.session_id
      }

      messages.value.push({
        role: 'assistant',
        content: result.data?.answer?.content || '',
        image: result.data?.answer?.image || [],
        sql: result.data?.sql || null,
        need_clarification: result.data?.need_clarification || false,
        created_at: new Date().toISOString()
      })

      if (isNewSession) {
        await loadSessions()
      }

      return result
    } catch (err) {
      error.value = err.message || '发送消息失败'
      messages.value.push({
        role: 'assistant',
        content: `抱歉，处理您的问题时出现错误：${error.value}`,
        image: [],
        sql: null,
        created_at: new Date().toISOString()
      })
    } finally {
      isSending.value = false
    }
  }

  async function loadSessions() {
    isLoading.value = true
    try {
      const result = await getSessions({ page: 1, page_size: 20 })
      sessions.value = result.data?.lists || []
    } catch (err) {
      error.value = err.message || '加载会话列表失败'
    } finally {
      isLoading.value = false
    }
  }

  async function loadHistory(sessionId) {
    isLoading.value = true
    try {
      const result = await getChatHistory(sessionId)
      currentSessionId.value = sessionId
      messages.value = (result.data || []).map(m => ({
        role: m.role,
        content: m.content,
        image: m.image || [],
        sql: m.sql || null,
        created_at: m.created_at
      }))
    } catch (err) {
      error.value = err.message || '加载会话历史失败'
    } finally {
      isLoading.value = false
    }
  }

  function startNewSession() {
    currentSessionId.value = null
    messages.value = []
    error.value = null
  }

  async function closeCurrentSession() {
    if (!currentSessionId.value) return
    try {
      await apiCloseSession(currentSessionId.value)
      startNewSession()
      await loadSessions()
    } catch (err) {
      error.value = err.message || '关闭会话失败'
    }
  }

  async function deleteCurrentSession() {
    if (!currentSessionId.value) return
    try {
      await apiDeleteSession(currentSessionId.value)
      startNewSession()
      await loadSessions()
    } catch (err) {
      error.value = err.message || '删除会话失败'
    }
  }

  async function deleteSessionById(sessionId) {
    try {
      await apiDeleteSession(sessionId)
      if (currentSessionId.value === sessionId) {
        startNewSession()
      }
      await loadSessions()
    } catch (err) {
      error.value = err.message || '删除会话失败'
    }
  }

  async function renameSessionById(sessionId, name) {
    try {
      await apiRenameSession(sessionId, name)
      await loadSessions()
    } catch (err) {
      error.value = err.message || '重命名会话失败'
    }
  }

  return {
    currentSessionId,
    messages,
    sessions,
    isLoading,
    isSending,
    error,
    activeSessions,
    sendMessage,
    loadSessions,
    loadHistory,
    startNewSession,
    closeCurrentSession,
    deleteCurrentSession,
    deleteSessionById,
    renameSessionById
  }
})
