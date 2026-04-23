import axios from 'axios'

const request = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || '/api/v1',
  timeout: 30000000000000000,
  headers: {
    'Content-Type': 'application/json'
  }
})

request.interceptors.request.use(
  (config) => {
    const token = window.localStorage.getItem('financial_reports_token')

    if (token) {
      config.headers.Authorization = `Bearer ${token}`
    }

    // 当 data 是 FormData 实例时，不设置 Content-Type
    // 让浏览器自动设置 multipart/form-data 并生成分隔符
    if (config.data instanceof FormData) {
      delete config.headers['Content-Type']
    }

    return config
  },
  (error) => Promise.reject(error)
)

request.interceptors.response.use(
  (response) => response.data,
  (error) => {
    const message =
      error.response?.data?.message ||
      error.response?.data?.detail ||
      error.message ||
      '请求失败，请稍后重试'

    return Promise.reject(new Error(message))
  }
)

export default request
