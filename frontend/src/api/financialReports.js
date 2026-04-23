import request from '@/api/request'

export function getFinancialReports(params) {
  return request.get('/data', { params })
}

export function getFinancialReportDetail(reportId) {
  return request.get(`/data/${reportId}`)
}

export function deleteFinancialReport(reportId) {
  return request.delete(`/data/${reportId}`)
}

export function parseReport(reportId, force = false) {
  return request.post(`/data/parse/${reportId}`, null, {
    params: { force }
  })
}

export function parseReportsBatch(reportIds) {
  return request.post('/data/parse/batch', { report_ids: reportIds })
}

export function getBatchParseStatus(reportIds) {
  return request.post('/data/parse/status/batch', { report_ids: reportIds })
}

export function getJsonFileContent(reportId) {
  return request.get(`/data/${reportId}/json`)
}


