<script setup>
/**
 * Toast消息组件
 * 功能：轻量级通知提示，支持多种类型和自动消失
 * Source: 前端规范文档 7.2节 错误处理模式
 */
import { ref, computed, onMounted } from 'vue'

const props = defineProps({
  id: { type: Number, required: true },
  message: { type: String, required: true },
  type: {
    type: String,
    default: 'info',
    validator: (value) => ['success', 'error', 'warning', 'info'].includes(value)
  },
  duration: { type: Number, default: 3000 }
})

const emit = defineEmits(['close'])

const visible = ref(false)

// 类型对应的样式映射
const typeStyles = {
  success: {
    bg: 'bg-green-500',
    icon: ['fas', 'check-circle']
  },
  error: {
    bg: 'bg-red-500',
    icon: ['fas', 'times-circle']
  },
  warning: {
    bg: 'bg-yellow-500',
    icon: ['fas', 'exclamation-triangle']
  },
  info: {
    bg: 'bg-blue-500',
    icon: ['fas', 'info-circle']
  }
}

const currentStyle = computed(() => typeStyles[props.type] || typeStyles.info)

onMounted(() => {
  // 进入动画
  requestAnimationFrame(() => {
    visible.value = true
  })

  // 自动关闭
  if (props.duration > 0) {
    setTimeout(() => {
      handleClose()
    }, props.duration)
  }
})

const handleClose = () => {
  visible.value = false
  setTimeout(() => {
    emit('close', props.id)
  }, 300)
}
</script>

<template>
  <div
    class="flex items-center gap-2 px-4 py-2 rounded-lg shadow-lg text-white transition-all duration-300"
    :class="[
      currentStyle.bg,
      visible ? 'opacity-100 translate-x-0' : 'opacity-0 translate-x-full'
    ]"
  >
    <font-awesome-icon :icon="currentStyle.icon" class="text-sm" />
    <span class="text-sm font-medium">{{ message }}</span>
    <button
      @click="handleClose"
      class="ml-2 text-white/80 hover:text-white transition-colors"
      aria-label="关闭"
    >
      <font-awesome-icon :icon="['fas', 'times']" class="text-xs" />
    </button>
  </div>
</template>