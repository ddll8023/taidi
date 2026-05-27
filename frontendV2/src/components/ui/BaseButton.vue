<script setup>
/**
 * BaseButton
 * 原子级按钮组件，封装 FontAwesome 图标 + Tailwind 样式
 * 支持多变体、多尺寸、加载态、图标前置/纯图标、禁用态
 *
 * 依赖：FontAwesomeIcon（注册在 main.js 中）
 *       使用前确认 icon 已在 library 中注册
 *
 * 用法示例：
 *   <BaseButton>提交</BaseButton>
 *   <BaseButton variant="secondary" icon="rotate-right" @click="refresh">刷新</BaseButton>
 *   <BaseButton variant="danger" icon="trash" :loading="deleting">删除</BaseButton>
 *   <BaseButton icon="plus" icon-only aria-label="添加" />
 */

import { computed } from 'vue'

const props = defineProps({
  /**
   * 按钮样式变体
   * primary  → shell-button（蓝底白字）
   * secondary → shell-button-secondary（白底灰边框）
   * danger    → 红底白字
   * success   → 绿底白字
   * warning   → 黄底深色字
   * ghost     → 透明底，hover 浅灰
   * amber     → 琥珀色底白字（系统初始化）
   * dark      → 深色底白字（上传研报PDF）
   * teal      → 青绿色底白字（切块操作）
   * violet    → 紫罗兰底白字（向量化操作）
   * info      → 蓝色底白字（信息/上传操作）
   */
  variant: {
    type: String,
    default: 'primary',
    validator: (v) => ['primary', 'secondary', 'danger', 'success', 'warning', 'ghost', 'amber', 'dark', 'teal', 'violet', 'info'].includes(v)
  },

  // 按钮尺寸
  size: {
    type: String,
    default: 'md',
    validator: (v) => ['xs', 'sm', 'md', 'lg', 'xl'].includes(v)
  },

  // 加载状态（显示 spinner 并禁用点击）
  loading: {
    type: Boolean,
    default: false
  },

  // FontAwesome 图标名称（不含前缀，如 "trash"、"play"）
  icon: {
    type: String,
    default: ''
  },

  // 图标前缀：fas（实心）/ far（线性）/ fab（品牌）
  iconPrefix: {
    type: String,
    default: 'fas',
    validator: (v) => ['fas', 'far', 'fab'].includes(v)
  },

  // 纯图标按钮模式（隐藏默认 slot，强制 aria-label 检查）
  iconOnly: {
    type: Boolean,
    default: false
  },

  // 禁用态
  disabled: {
    type: Boolean,
    default: false
  },

  // 原生 button type
  type: {
    type: String,
    default: 'button',
    validator: (v) => ['button', 'submit', 'reset'].includes(v)
  },

  // 撑满父容器宽度
  fullWidth: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits(['click'])

// ── 尺寸映射 ──
const sizeClasses = {
  xs: 'px-2.5 py-1 text-xs rounded-lg gap-1',
  sm: 'px-3 py-1.5 text-xs rounded-lg gap-1.5',
  md: 'px-4 py-2.5 text-sm rounded-xl gap-2',
  lg: 'px-5 py-3 text-base rounded-xl gap-2',
  xl: 'px-6 py-3.5 text-lg rounded-2xl gap-2.5'
}

// ── 图标尺寸映射 ──
const iconSizeClasses = {
  xs: 'text-[0.65em]',
  sm: 'text-[0.7em]',
  md: 'text-[0.8em]',
  lg: 'text-[0.85em]',
  xl: 'text-[0.9em]'
}

// ── 变体映射（优先使用 shell-* utility classes）──
const variantClasses = {
  primary:
    'shell-button',
  secondary:
    'shell-button-secondary',
  danger:
    'inline-flex items-center font-semibold transition-all duration-200 rounded-xl border border-transparent ' +
    'bg-red-600 text-white hover:bg-red-700 active:bg-red-800 ' +
    'shadow-sm hover:shadow ' +
    'disabled:cursor-not-allowed disabled:opacity-50',
  success:
    'inline-flex items-center font-semibold transition-all duration-200 rounded-xl border border-transparent ' +
    'bg-emerald-600 text-white hover:bg-emerald-700 active:bg-emerald-800 ' +
    'shadow-sm hover:shadow ' +
    'disabled:cursor-not-allowed disabled:opacity-50',
  warning:
    'inline-flex items-center font-semibold transition-all duration-200 rounded-xl border border-transparent ' +
    'bg-amber-400 text-amber-900 hover:bg-amber-500 active:bg-amber-600 ' +
    'shadow-sm hover:shadow ' +
    'disabled:cursor-not-allowed disabled:opacity-50',
  ghost:
    'inline-flex items-center font-medium transition-all duration-200 rounded-xl border border-transparent ' +
    'text-ink-600 hover:bg-ink-50/60 active:bg-ink-100/60 ' +
    'disabled:cursor-not-allowed disabled:opacity-40',
  amber:
    'inline-flex items-center font-semibold transition-all duration-200 rounded-xl border border-transparent ' +
    'bg-amber-600 text-white hover:bg-amber-700 active:bg-amber-800 ' +
    'shadow-sm hover:shadow ' +
    'disabled:cursor-not-allowed disabled:opacity-50',
  dark:
    'inline-flex items-center font-semibold transition-all duration-200 rounded-xl border border-transparent ' +
    'bg-ink-900 text-white hover:bg-ink-700 active:bg-ink-800 ' +
    'shadow-sm hover:shadow ' +
    'disabled:cursor-not-allowed disabled:opacity-40',
  teal:
    'inline-flex items-center font-semibold transition-all duration-200 rounded-xl border border-transparent ' +
    'bg-teal-600 text-white hover:bg-teal-700 active:bg-teal-800 ' +
    'shadow-sm hover:shadow ' +
    'disabled:cursor-not-allowed disabled:opacity-40',
  violet:
    'inline-flex items-center font-semibold transition-all duration-200 rounded-xl border border-transparent ' +
    'bg-violet-600 text-white hover:bg-violet-700 active:bg-violet-800 ' +
    'shadow-sm hover:shadow ' +
    'disabled:cursor-not-allowed disabled:opacity-40',
  info:
    'inline-flex items-center font-semibold transition-all duration-200 rounded-xl border border-transparent ' +
    'bg-blue-600 text-white hover:bg-blue-700 active:bg-blue-800 ' +
    'shadow-sm hover:shadow ' +
    'disabled:cursor-not-allowed disabled:opacity-40'
}

/**
 * 合成最终类名
 * 排序：布局 → 定位 → 尺寸 → 间距 → 颜色 → 字体 → 边框 → 阴影 → 动画
 */
const buttonClasses = computed(() => [
  // 基础布局
  'inline-flex items-center justify-center',
  // 撑满宽度
  props.fullWidth ? 'w-full' : '',
  // 尺寸变体
  sizeClasses[props.size],
  // 颜色变体
  variantClasses[props.variant],
  // 纯图标模式保持正方形
  props.iconOnly ? 'p-0 aspect-square' : '',
  // transform 过渡（shell-button 自带 translateY 效果）
  !['primary', 'secondary'].includes(props.variant) ? 'active:scale-[0.97]' : ''
].filter(Boolean).join(' '))

// ── 点击处理 ──
const handleClick = (event) => {
  if (props.loading || props.disabled) return
  emit('click', event)
}
</script>

<template>
  <button
    :type="type"
    :class="buttonClasses"
    :disabled="disabled || loading"
    :aria-label="iconOnly ? $attrs.ariaLabel || $attrs['aria-label'] || '按钮' : undefined"
    @click="handleClick"
  >
    <!-- 加载 spinner -->
    <FontAwesomeIcon
      v-if="loading"
      :icon="['fas', 'spinner']"
      spin
      :class="[iconSizeClasses[size], 'shrink-0']"
      aria-hidden="true"
    />

    <!-- 自定义图标（非加载状态时显示） -->
    <FontAwesomeIcon
      v-else-if="icon"
      :icon="[iconPrefix, icon]"
      :class="[iconSizeClasses[size], 'shrink-0']"
      aria-hidden="true"
    />

    <!-- 按钮文字（纯图标模式下隐藏） -->
    <span v-if="!iconOnly" class="truncate">
      <slot />
    </span>
  </button>
</template>
