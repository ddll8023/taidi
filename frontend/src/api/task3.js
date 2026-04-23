import request from '@/api/request'

export function getWorkspace() {
  return request.get('/task3/workspace')
}

export function importFujian6(file) {
  const formData = new FormData()
  formData.append('file', file)
  return request.post('/task3/workspace/import', formData)
}

export function getQuestions(params) {
  return request.get('/task3/questions', { params })
}

export function getQuestionDetail(questionId) {
  return request.get(`/task3/questions/${questionId}`)
}

export function answerQuestion(questionId) {
  return request.post(`/task3/questions/${questionId}/answer`)
}

export function deleteAnswer(questionId) {
  return request.delete(`/task3/questions/${questionId}/answer`)
}

export function rerunQuestion(questionId) {
  return request.post(`/task3/questions/${questionId}/rerun`)
}

export function batchAnswer(scope = 'unfinished') {
  return request.post('/task3/questions/batch-answer', null, {
    params: { scope }
  })
}

export function exportResult() {
  return request.post('/task3/export')
}

export function getLatestExport() {
  return request.get('/task3/export/latest')
}
