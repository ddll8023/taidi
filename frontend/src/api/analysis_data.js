import request from '@/api/request'

/**
 * 上传财报PDF文件
 * @param {File} file - 文件对象
 */
export function uploadData(file) {
  const formData = new FormData()
  formData.append('file', file)
  return request.post('/data/upload', formData)
}

/**
 * 导入附件1公司基本信息
 * @param {File} file - Excel文件对象
 */
export function importCompanies(file) {
  const formData = new FormData()
  formData.append('file', file)
  return request.post('/data/import-companies', formData)
}
