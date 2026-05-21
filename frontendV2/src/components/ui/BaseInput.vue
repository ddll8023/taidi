<script setup>
/**
 * BaseInput
 * 原子级输入框组件，封装 FontAwesome 图标 + Tailwind 样式
 * 支持前缀图标、清除按钮、v-model 双向绑定
 *
 * 用法示例：
 *   <BaseInput v-model="keyword" placeholder="搜索..." icon="search" />
 *   <BaseInput v-model="name" placeholder="请输入名称" />
 *   <BaseInput v-model="code" size="sm" placeholder="代码" />
 */
import { computed } from 'vue'

const props = defineProps({
  modelValue: {
    type: String,
    default: ''
  },
  placeholder: {
    type: String,
    default: ''
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
  // FontAwesome 图标名称（显示在输入框左侧）
  icon: {
    type: String,
    default: ''
  },
  // 图标前缀
  iconPrefix: {
    type: String,
    default: 'fas',
    validator: (v) => ['fas', 'far', 'fab'].includes(v)
  },
  // input type
  type: {
    type: String,
    default: 'text'
  },
  // 是否显示清除按钮
  clearable: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits(['update:modelValue', 'keydown', 'clear'])

// ── 尺寸映射 ──
const sizeClasses = {
  sm: 'py-1.5 text-xs rounded-lg',
  md: 'py-2 text-sm rounded-xl',
  lg: 'py-2.5 text-base rounded-xl'
}

const iconSizeClasses = {
  sm: 'left-3 text-[0.65em]',
  md: 'left-3 text-xs',
  lg: 'left-4 text-sm'
}

const iconTopClass = 'top-1/2 -translate-y-1/2'

const inputClasses = computed(() => [
  'w-full border border-black/10 bg-white text-ink-900 placeholder:text-ink-400',
  'focus:border-accent-400 focus:outline-none focus:ring-1 focus:ring-accent-400',
  'transition-colors disabled:cursor-not-allowed disabled:opacity-50 disabled:bg-ink-50',
  sizeClasses[props.size],
  props.icon
    ? (props.size === 'lg' ? 'pl-10 pr-4' : 'pl-8 pr-3')
    : 'px-3',
  props.clearable && props.modelValue ? 'pr-8' : ''
].filter(Boolean).join(' '))

const handleInput = (event) => {
  emit('update:modelValue', event.target.value)
}

const handleKeydown = (event) => {
  emit('keydown', event)
}

const handleClear = () => {
  emit('update:modelValue', '')
  emit('clear')
}
</script>

<template>
  <div class="relative inline-flex">
    <!-- 前缀图标 -->
    <FontAwesomeIcon
      v-if="icon"
      :icon="[iconPrefix, icon]"
      :class="[iconSizeClasses[size], iconTopClass, 'absolute pointer-events-none text-ink-400']"
      aria-hidden="true"
    />

    <input
      :type="type"
      :value="modelValue"
      :placeholder="placeholder"
      :disabled="disabled"
      :class="inputClasses"
      @input="handleInput"
      @keydown="handleKeydown"
    />

    <!-- 清除按钮 -->
    <button
      v-if="clearable && modelValue"
      type="button"
      :class="[iconSizeClasses[size], iconTopClass, 'absolute right-2 text-ink-400 hover:text-ink-600 transition-colors']"
      style="left: auto; right: 8px;"
      @click="handleClear"
    >
      <FontAwesomeIcon :icon="['fas', 'xmark']" class="text-[0.7em]" aria-hidden="true" />
    </button>
  </div>
</template>
