/**
 * Toast组合式函数
 * 功能：全局Toast消息管理
 * Source: 前端规范文档 6.4节 API调用规范
 */
import { ref } from 'vue'
import Toast from '@/components/base/Toast.vue'

const toasts = ref([])
let toastId = 0

export function useToast() {
  const addToast = (message, type = 'info', duration = 3000) => {
    const id = ++toastId
    toasts.value.push({
      id,
      message,
      type,
      duration,
      component: Toast
    })
    return id
  }

  const removeToast = (id) => {
    const index = toasts.value.findIndex(t => t.id === id)
    if (index !== -1) {
      toasts.value.splice(index, 1)
    }
  }

  const success = (message, duration = 3000) => {
    return addToast(message, 'success', duration)
  }

  const error = (message, duration = 3000) => {
    return addToast(message, 'error', duration)
  }

  const warning = (message, duration = 3000) => {
    return addToast(message, 'warning', duration)
  }

  const info = (message, duration = 3000) => {
    return addToast(message, 'info', duration)
  }

  return {
    toasts,
    addToast,
    removeToast,
    success,
    error,
    warning,
    info
  }
}