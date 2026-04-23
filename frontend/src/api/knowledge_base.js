import request from '@/api/request'

// ─────────────────────────────────────────────────────────────────────────────
// 切块处理接口
// ─────────────────────────────────────────────────────────────────────────────

/**
 * 提交单个文档的切块任务
 * POST /api/v1/knowledge-base/chunk/{document_id}
 */
export function chunkDocument(documentId, force = false) {
  return request.post(`/knowledge-base/chunk/${documentId}`, null, {
    params: { force }
  })
}

/**
 * 提交批量切块任务
 * POST /api/v1/knowledge-base/chunk/batch
 */
export function chunkDocumentsBatch(documentIds) {
  return request.post('/knowledge-base/chunk/batch', { document_ids: documentIds })
}

/**
 * 提交所有待处理文档的切块任务
 * POST /api/v1/knowledge-base/chunk/all
 */
export function chunkAllPending(params = {}) {
  return request.post('/knowledge-base/chunk/all', params)
}

// ─────────────────────────────────────────────────────────────────────────────
// 向量化接口
// ─────────────────────────────────────────────────────────────────────────────

/**
 * 向量化单个文档
 * POST /api/v1/knowledge-base/vectorize/{document_id}
 */
export function vectorizeDocument(documentId, { batchSize = 20, force = false } = {}) {
  return request.post(`/knowledge-base/vectorize/${documentId}`, null, {
    params: { batch_size: batchSize, force }
  })
}

/**
 * 批量向量化
 * POST /api/v1/knowledge-base/vectorize
 */
export function vectorizeChunks(params = {}) {
  return request.post('/knowledge-base/vectorize', params)
}

/**
 * 重置文档向量状态
 * POST /api/v1/knowledge-base/reset-vector-status/{document_id}
 */
export function resetVectorStatus(documentId, targetStatus = 0) {
  return request.post(`/knowledge-base/reset-vector-status/${documentId}`, null, {
    params: { target_status: targetStatus }
  })
}

// ─────────────────────────────────────────────────────────────────────────────
// 状态查询接口
// ─────────────────────────────────────────────────────────────────────────────

/**
 * 获取知识库统计
 * GET /api/v1/knowledge-base/stats
 */
export function getStats() {
  return request.get('/knowledge-base/stats')
}

/**
 * 获取文档列表
 * GET /api/v1/knowledge-base/documents
 */
export function getDocuments(params) {
  return request.get('/knowledge-base/documents', { params })
}

/**
 * 批量查询文档状态
 * POST /api/v1/knowledge-base/documents/status/batch
 */
export function getDocumentsStatusBatch(documentIds) {
  return request.post('/knowledge-base/documents/status/batch', { document_ids: documentIds })
}

// ─────────────────────────────────────────────────────────────────────────────
// 切块列表接口
// ─────────────────────────────────────────────────────────────────────────────

/**
 * 查询切块列表
 * GET /api/v1/knowledge-base/chunks
 */
export function getChunks(params) {
  return request.get('/knowledge-base/chunks', { params })
}

// ─────────────────────────────────────────────────────────────────────────────
// 知识检索接口
// ─────────────────────────────────────────────────────────────────────────────

/**
 * 知识检索
 * POST /api/v1/knowledge-base/search
 */
export function searchKnowledge(data) {
  return request.post('/knowledge-base/search', data)
}

// ─────────────────────────────────────────────────────────────────────────────
// 增量处理模式接口
// ─────────────────────────────────────────────────────────────────────────────

/**
 * 系统初始化：加载Excel元数据
 * POST /api/v1/knowledge-base/init
 */
export function initSystem(formData, forceReload = false) {
  return request.post('/knowledge-base/init', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
    params: { force_reload: forceReload }
  })
}

/**
 * 增量上传PDF文件
 * POST /api/v1/knowledge-base/upload-pdf
 */
export function uploadPdfIncremental(formData, docType = 'RESEARCH_REPORT') {
  return request.post('/knowledge-base/upload-pdf', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
    params: { doc_type: docType },
    timeout: 300000
  })
}

/**
 * 查询增量处理进度
 * GET /api/v1/knowledge-base/progress
 */
export function getProgress() {
  return request.get('/knowledge-base/progress')
}

/**
 * 查询系统初始化状态
 * GET /api/v1/knowledge-base/init-status
 */
export function getInitStatus() {
  return request.get('/knowledge-base/init-status')
}

/**
 * 上传单个文档的PDF
 * POST /api/v1/knowledge-base/upload-single-pdf
 */
export function uploadSinglePdf(documentId, formData) {
  return request.post('/knowledge-base/upload-single-pdf', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
    params: { document_id: documentId }
  })
}

/**
 * 重试失败的文档
 * POST /api/v1/knowledge-base/retry-failed
 */
export function retryFailed(documentId, formData) {
  return request.post('/knowledge-base/retry-failed', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
    params: { document_id: documentId }
  })
}


