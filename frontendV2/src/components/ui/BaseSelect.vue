<script setup>
/**
 * BaseSelect
 * 原子级下拉选择器组件
 * 原生 select + appearance-none，用 FontAwesome 图标作下拉箭头
 * 支持 v-model 双向绑定
 *
 * 用法示例：
 *   <BaseSelect v-model="status" :options="statusOptions" />
 *   <BaseSelect v-model="year" :options="yearOptions" placeholder="全部年份" />
 */
import { computed } from 'vue'

const props = defineProps({
  modelValue: {
    type: [String, Number],
    default: ''
  },
  placeholder: {
    type: String,
    default: '请选择'
  },
  disabled: {
    type: Boolean,
    default: false
  },
  size: {
    type: String,
    default: 'md',
    validator: (v) => ['sm', 'md', 'lg'].includes(v)
  },
  options: {
    type: Array,
    default: () => []
  }
})

const emit = defineEmits(['update:modelValue'])

const hasValue = computed(() => props.modelValue !== '' && props.modelValue !== null)

// ── 尺寸映射 ──
const wrapClasses = computed(() => [
  'relative inline-flex',
  props.size === 'sm' ? 'text-xs' : '',
  props.size === 'md' ? 'text-sm' : '',
  props.size === 'lg' ? 'text-base' : ''
].filter(Boolean).join(' '))

const selectClasses = computed(() => [
  // 基础样式
  'w-full border bg-white text-ink-900 transition-all duration-150',
  'appearance-none cursor-pointer',
  // 交互态
  'hover:border-ink-300 hover:bg-ink-50/40',
  'focus:border-accent-400 focus:outline-none focus:ring-[3px] focus:ring-accent-400/20 focus:bg-white',
  // 禁用
  'disabled:cursor-not-allowed disabled:opacity-50 disabled:bg-ink-50 disabled:hover:border-black/10 disabled:hover:bg-ink-50',
  // 尺寸 + 形状
  props.size === 'sm' ? 'px-3 py-1.5 rounded-lg pr-8' : '',
  props.size === 'md' ? 'px-3 py-2 rounded-xl pr-9' : '',
  props.size === 'lg' ? 'px-4 py-2.5 rounded-xl pr-10' : '',
  // 边框颜色（跟随值状态）
  hasValue.value ? 'border-ink-300' : 'border-black/10'
].filter(Boolean).join(' '))

const chevronClasses = computed(() => [
  'absolute pointer-events-none text-ink-400 transition-transform duration-150',
  'group-focus-within:rotate-180',
  props.size === 'sm' ? 'right-2.5 top-1/2 -translate-y-1/2 text-[0.6rem]' : '',
  props.size === 'md' ? 'right-3 top-1/2 -translate-y-1/2 text-[0.65rem]' : '',
  props.size === 'lg' ? 'right-3.5 top-1/2 -translate-y-1/2 text-xs' : ''
].filter(Boolean).join(' '))

const handleChange = (event) => {
  emit('update:modelValue', event.target.value)
}
</script>

<template>
  <div :class="wrapClasses" class="group">
    <select
      :value="modelValue"
      :disabled="disabled"
      :class="selectClasses"
      @change="handleChange"
    >
      <option v-if="placeholder" value="" class="text-ink-400">{{ placeholder }}</option>
      <option
        v-for="opt in options"
        :key="opt.value"
        :value="opt.value"
        class="text-ink-900"
      >
        {{ opt.label }}
      </option>
    </select>
    <!-- 字体图标下拉箭头 -->
    <FontAwesomeIcon
      :icon="['fas', 'chevron-down']"
      :class="chevronClasses"
      aria-hidden="true"
    />
  </div>
</template>
