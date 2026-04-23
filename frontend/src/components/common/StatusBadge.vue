<script setup>
/**
 * 状态标签
 * 功能描述：统一展示流程状态与处理结果
 * 依赖组件：无
 */
import { computed } from 'vue'

const props = defineProps({
  label: {
    type: String,
    required: true
  },
  tone: {
    type: String,
    default: 'neutral'
  }
})

const badgeStyleMap = {
  accent: {
    badge: 'border border-accent-200 bg-accent-50 text-accent-700',
    dot: 'bg-accent-600'
  },
  danger: {
    badge: 'border border-transparent bg-[var(--danger-soft)] text-danger',
    dot: 'bg-danger'
  },
  neutral: {
    badge: 'border border-ink-200 bg-white text-ink-700',
    dot: 'bg-ink-400'
  },
  success: {
    badge: 'border border-transparent bg-[var(--success-soft)] text-success',
    dot: 'bg-success'
  },
  warning: {
    badge: 'border border-transparent bg-[var(--warning-soft)] text-warning',
    dot: 'bg-warning'
  }
}

const badgeClass = computed(() => (badgeStyleMap[props.tone] || badgeStyleMap.neutral).badge)
const dotClass = computed(() => (badgeStyleMap[props.tone] || badgeStyleMap.neutral).dot)
</script>

<template>
  <span
    class="inline-flex items-center gap-2 rounded-full px-3 py-1.5 text-xs font-medium tracking-[0.01em]"
    :class="badgeClass"
  >
    <span class="h-1.5 w-1.5 rounded-full" :class="dotClass" />
    {{ label }}
  </span>
</template>
