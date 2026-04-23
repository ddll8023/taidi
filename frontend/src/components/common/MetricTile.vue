<script setup>
/**
 * 指标卡片
 * 功能描述：统一展示概览数字、说明和状态色块
 * 依赖组件：无
 */
import { computed } from 'vue'

const props = defineProps({
  title: {
    type: String,
    required: true
  },
  value: {
    type: String,
    required: true
  },
  note: {
    type: String,
    default: ''
  },
  tone: {
    type: String,
    default: 'neutral'
  }
})

const toneClass = computed(() => {
  const classMap = {
    accent: 'border-accent-200 bg-accent-50/70',
    danger: 'border-danger/25 bg-danger/5',
    neutral: 'border-ink-200 bg-white',
    warning: 'border-warning/25 bg-warning/5'
  }

  return classMap[props.tone] || classMap.neutral
})
</script>

<template>
  <article class="rounded-3xl border p-5 shadow-soft" :class="toneClass">
    <p class="text-sm font-medium text-ink-600">{{ title }}</p>
    <div class="mt-3 flex items-end justify-between gap-3">
      <strong class="text-3xl font-semibold tracking-tight text-ink-900">{{ value }}</strong>
      <slot name="badge" />
    </div>
    <p v-if="note" class="mt-3 text-sm leading-6 text-ink-500">{{ note }}</p>
  </article>
</template>
