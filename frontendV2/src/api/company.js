import request from './request'

export function importCompanyBaseInfo(file) {
  const formData = new FormData()
  formData.append('file', file)
  return request.post('/company_base_info/import', formData)
}
