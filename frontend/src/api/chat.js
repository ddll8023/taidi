import request from '@/api/request'

export function sendMessage(sessionId, question) {
  return request.post('/chat', {
    session_id: sessionId || null,
    question
  })
}

export function getSessions(params) {
  return request.get('/chat/sessions', { params })
}

export function getChatHistory(sessionId) {
  return request.get(`/chat/history/${sessionId}`)
}

export function closeSession(sessionId) {
  return request.put(`/chat/sessions/${sessionId}/close`)
}

export function deleteSession(sessionId) {
  return request.delete(`/chat/sessions/${sessionId}`)
}

export function renameSession(sessionId, name) {
  return request.put(`/chat/sessions/${sessionId}/rename`, { name })
}

export function exportResult(questions) {
  return request.post('/chat/export', { questions })
}
