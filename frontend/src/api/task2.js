import request from '@/api/request'

export function getWorkspace() {
  return request.get('/task2/workspace')
}

export function importFujian4(file) {
  const formData = new FormData()
  formData.append('file', file)
  return request.post('/task2/workspace/import', formData)
}

export function getQuestions(params) {
  return request.get('/task2/questions', { params })
}

export function getQuestionDetail(questionId) {
  return request.get(`/task2/questions/${questionId}`)
}

export function answerQuestion(questionId) {
  return request.post(`/task2/questions/${questionId}/answer`)
}

export function deleteAnswer(questionId) {
  return request.delete(`/task2/questions/${questionId}/answer`)
}

export function rerunQuestion(questionId) {
  return request.post(`/task2/questions/${questionId}/rerun`)
}

export function batchAnswer(scope = 'unfinished') {
  return request.post('/task2/questions/batch-answer', null, {
    params: { scope }
  })
}

export function exportResult() {
  return request.post('/task2/export')
}

export function getLatestExport() {
  return request.get('/task2/export/latest')
}
