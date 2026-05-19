/**
 * 财报管理 API
 * 功能描述：上传文件、列表查询等接口
 */
import request from './request'

/**
 * 上传财报 PDF 文件（批量）
 * @param {File[]} fileList - PDF 文件数组
 * @returns {Promise} { total_count, success_count, failed_count, success_reports, failed_files }
 */
export function uploadReportFiles(fileList) {
  const formData = new FormData()
  fileList.forEach((file) => formData.append('file_list', file))
  return request.post('/analyze-data/upload', formData)
}

/**
 * 获取财报记录列表
 * @param {Object} params - 查询参数
 * @param {number} params.page - 页码
 * @param {number} params.page_size - 每页条数
 * @returns {Promise} { lists: [...], pagination: {...} }
 */
export function getReportList(params) {
  return request.get('/data', { params })
}
