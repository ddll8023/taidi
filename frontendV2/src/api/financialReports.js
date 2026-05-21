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
 * 提交财报解析任务（异步后台执行）
 * @param {number[]} reportIds - 待解析的财报ID列表
 * @returns {Promise} { total, start_parse_count, skip_report_ids }
 */
export function parseReports(reportIds) {
  return request.post('/analyze-data/parse', { report_ids: reportIds })
}

/**
 * 获取财报记录列表
 * @param {Object} params - 查询参数
 * @param {number} params.page - 页码
 * @param {number} params.page_size - 每页条数
 * @param {string} [params.keyword] - 报告标题关键词搜索
 * @param {number} [params.report_type] - 报告类型筛选
 * @param {number} [params.report_year] - 报告年份筛选
 * @param {number} [params.parse_status] - 解析状态筛选 0/1/2
 * @param {number} [params.import_status] - 入库状态筛选 0/1/2
 * @param {string} [params.sort_by] - 排序字段 created_at/updated_at
 * @param {string} [params.sort_order] - 排序方式 desc/asc
 * @returns {Promise} { lists: [...], pagination: {...} }
 */
export function getReportList(params) {
  return request.post('/analyze-data/list', params)
}
