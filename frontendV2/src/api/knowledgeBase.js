/**
 * 知识库管理 API
 * 功能描述：系统初始化（研报元数据导入）、初始化状态查询、整体统计信息、文档切块、PDF上传
 */
import request from './request'

/**
 * 系统初始化：加载研报元数据到知识库
 * @param {File} file - 研报 Excel 文件
 * @param {string} docType - 文档类型：RESEARCH_REPORT 个股研报 / INDUSTRY_REPORT 行业研报
 * @returns {Promise} { success, message, total_count }
 */
export function initKnowledgeBase(file, docType) {
  const formData = new FormData()
  formData.append('file', file)
  formData.append('doc_type', docType)
  return request.post('/knowledge-base/init', formData)
}

/**
 * 查询系统初始化状态
 * @returns {Promise} { initialized, stock_metadata_count, industry_metadata_count, total_metadata_count }
 */
export function getInitStatus() {
  return request.post('/knowledge-base/init-status')
}

/**
 * 获取知识库整体统计信息
 * @returns {Promise} { documents: { total, by_chunk_status, by_vector_status, by_doc_type }, chunks: { total, by_vector_status } }
 */
export function getKnowledgeBaseStats() {
  return request.post('/knowledge-base/stats')
}

/**
 * 获取知识库文档列表（分页 + 筛选 + 排序）
 * @param {Object} params - 查询参数
 * @param {number} params.page - 页码（≥1）
 * @param {number} params.page_size - 每页数量（≥10）
 * @param {string} [params.keyword] - 标题关键词搜索
 * @param {string} [params.doc_type] - 文档类型筛选：RESEARCH_REPORT / INDUSTRY_REPORT
 * @param {string} [params.stock_code] - 股票代码筛选
 * @param {number} [params.chunk_status] - 切块状态筛选：0待切块/1切块中/2完成/3失败
 * @param {number} [params.vector_status] - 向量状态筛选：0未向量化/1向量化中/2已向量化/3失败
 * @param {string} [params.sort_by] - 排序字段：created_at / updated_at
 * @param {string} [params.sort_order] - 排序方式：desc / asc
 * @returns {Promise} { lists: [...], pagination: { page, page_size, total, total_pages } }
 */
export function getKnowledgeDocumentList(params) {
  return request.post('/knowledge-base/list', params)
}

/**
 * 提交文档切块任务
 * @param {number[]} documentIds - 待切块的文档ID列表
 * @returns {Promise} { total, success_count, failed_count, results: [{ document_id, title, chunk_count, success, error }] }
 */
export function chunkDocuments(documentIds) {
  return request.post('/knowledge-base/chunk', { document_ids: documentIds })
}

/**
 * 批量解析文档（MinerU结构化解析）
 * @param {number[]} documentIds - 待解析的文档ID列表
 * @returns {Promise} { total, success_count, failed_count, results: [{ document_id, title, success, error, block_count }] }
 */
export function parseDocuments(documentIds) {
  return request.post('/knowledge-base/parse', { document_ids: documentIds })
}

/**
 * 获取文档解析结果
 * @param {number} documentId - 文档ID
 * @returns {Promise} { document_id, title, markdown_content, page_count }
 */
export function getParseResult(documentId) {
  return request.post('/knowledge-base/parse-result', { document_id: documentId })
}

/**
 * 保存清洗后的Markdown解析结果
 * @param {number} documentId - 文档ID
 * @param {string} markdownContent - 清洗后的 Markdown 内容
 * @returns {Promise} { document_id, title, saved }
 */
export function saveParseResult(documentId, markdownContent) {
  return request.post('/knowledge-base/save-parse-result', {
    document_id: documentId,
    markdown_content: markdownContent
  })
}

/**
 * 批量上传知识库文档PDF
 * @param {File[]} files - PDF 文件列表
 * @returns {Promise} { total, success_count, failed_count, success_documents: [...], failed_files: [...] }
 */
export function uploadKnowledgeDocuments(files) {
  const formData = new FormData()
  files.forEach((file) => {
    formData.append('file_list', file)
  })
  return request.post('/knowledge-base/upload', formData)
}