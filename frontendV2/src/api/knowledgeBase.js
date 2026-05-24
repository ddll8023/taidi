/**
 * 知识库管理 API
 * 功能描述：系统初始化（研报元数据导入）、初始化状态查询
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