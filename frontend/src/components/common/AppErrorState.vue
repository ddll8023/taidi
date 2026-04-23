<script setup>
/**
 * 统一错误状态组件
 * 功能描述：展示请求失败、业务异常和重试入口
 * 依赖组件：无
 */
defineProps({
  title: {
    type: String,
    default: '处理失败'
  },
  description: {
    type: String,
    default: '请检查当前数据状态，或等待后端接口接入后重试。'
  },
  showRetry: {
    type: Boolean,
    default: true
  }
})

defineEmits(['retry'])
</script>

<template>
  <div class="rounded-3xl border border-danger/20 bg-danger/5 p-5">
    <div class="flex items-start gap-4">
      <div class="flex h-11 w-11 items-center justify-center rounded-2xl bg-danger/10 text-danger">
        <FontAwesomeIcon :icon="['fas', 'triangle-exclamation']" />
      </div>
      <div class="flex-1">
        <h3 class="text-base font-semibold text-ink-900">{{ title }}</h3>
        <p class="mt-2 text-sm leading-6 text-ink-600">{{ description }}</p>
        <button v-if="showRetry" type="button" class="mt-4 shell-button-secondary" @click="$emit('retry')">
          <FontAwesomeIcon :icon="['fas', 'rotate-right']" />
          <span>重新尝试</span>
        </button>
      </div>
    </div>
  </div>
</template>
