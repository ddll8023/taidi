<script setup>
/**
 * 统一加载状态组件
 * 功能描述：展示批次执行中、列表加载中等统一等待状态
 * 依赖组件：无
 */
defineProps({
  title: {
    type: String,
    default: '处理中'
  },
  description: {
    type: String,
    default: '当前内容为静态演示，后续可替换为真实接口返回的加载态。'
  }
})
</script>

<template>
  <div class="rounded-3xl border border-accent-200 bg-accent-50/70 p-5">
    <div class="flex items-start gap-4">
      <div class="flex h-11 w-11 items-center justify-center rounded-2xl bg-white text-accent-700 shadow-soft">
        <FontAwesomeIcon :icon="['fas', 'clock']" class="animate-pulse" />
      </div>
      <div>
        <h3 class="text-base font-semibold text-ink-900">{{ title }}</h3>
        <p class="mt-2 text-sm leading-6 text-ink-600">{{ description }}</p>
      </div>
    </div>
  </div>
</template>
