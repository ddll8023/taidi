<script setup>
/**
 * 统一空状态组件
 * 功能描述：展示暂无数据、待接入接口和待补内容等空状态
 * 依赖组件：无
 */
defineProps({
  title: {
    type: String,
    default: '暂无内容'
  },
  description: {
    type: String,
    default: '当前区域已预留结构，待接口接入后承载真实数据。'
  }
})
</script>

<template>
  <div class="rounded-3xl border border-dashed border-ink-300 bg-white/70 p-6 text-center">
    <div class="mx-auto flex h-12 w-12 items-center justify-center rounded-2xl bg-ink-100 text-ink-700">
      <FontAwesomeIcon :icon="['fas', 'file-lines']" />
    </div>
    <h3 class="mt-4 text-base font-semibold text-ink-900">{{ title }}</h3>
    <p class="mx-auto mt-2 max-w-md text-sm leading-6 text-ink-600">{{ description }}</p>
  </div>
</template>
