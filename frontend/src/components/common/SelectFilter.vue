<script setup>
/**
 * 下拉选择器组件
 * 功能描述：胶囊式下拉筛选选择器，支持多种选项样式
 * 依赖组件：无
 */
import { ref, computed, onMounted, onBeforeUnmount, nextTick, watch } from 'vue'

const props = defineProps({
  modelValue: {
    type: [String, Number],
    default: ''
  },
  options: {
    type: Array,
    required: true
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
    validator: (value) => ['sm', 'md', 'lg'].includes(value)
  }
})

const emit = defineEmits(['update:modelValue', 'change'])

const isOpen = ref(false)
const selectRef = ref(null)
const dropdownRef = ref(null)
const dropdownStyle = ref({})

const handleScrollOrResize = () => {
  if (!isOpen.value || !selectRef.value) return
  const rect = selectRef.value.getBoundingClientRect()
  dropdownStyle.value = {
    position: 'fixed',
    top: `${rect.bottom + 8}px`,
    left: `${rect.left}px`,
    width: `${rect.width}px`
  }
}

const addScrollAndResizeListeners = () => {
  window.addEventListener('scroll', handleScrollOrResize, true)
  window.addEventListener('resize', handleScrollOrResize)
}

const removeScrollAndResizeListeners = () => {
  window.removeEventListener('scroll', handleScrollOrResize, true)
  window.removeEventListener('resize', handleScrollOrResize)
}

watch(isOpen, async (val) => {
  if (val) {
    await nextTick()
    handleScrollOrResize()
    addScrollAndResizeListeners()
  } else {
    removeScrollAndResizeListeners()
    dropdownStyle.value = {}
  }
})

const selectedLabel = computed(() => {
  const selected = props.options.find(opt => opt.value === props.modelValue)
  return selected?.label || props.placeholder
})

const selectedOption = computed(() => {
  return props.options.find(opt => opt.value === props.modelValue)
})

const hasSelection = computed(() => {
  return props.modelValue !== '' && props.modelValue !== null && props.modelValue !== undefined
})

const sizeClasses = computed(() => {
  const sizes = {
    sm: 'px-3 py-1.5 text-xs',
    md: 'px-4 py-2 text-sm',
    lg: 'px-5 py-2.5 text-base'
  }
  return sizes[props.size]
})

const toggleDropdown = () => {
  if (!props.disabled) {
    isOpen.value = !isOpen.value
  }
}

const selectOption = (option) => {
  emit('update:modelValue', option.value)
  emit('change', option.value)
  isOpen.value = false
}

const handleClickOutside = (event) => {
  if (selectRef.value && !selectRef.value.contains(event.target)) {
    isOpen.value = false
  }
}

onMounted(() => {
  document.addEventListener('click', handleClickOutside)
})

onBeforeUnmount(() => {
  document.removeEventListener('click', handleClickOutside)
  removeScrollAndResizeListeners()
})
</script>

<template>
  <div
    ref="selectRef"
    class="select-filter"
    :class="{ 'is-disabled': disabled, 'is-open': isOpen, 'has-selection': hasSelection }"
  >
    <button
      type="button"
      class="select-trigger"
      :class="[sizeClasses, { 'is-active': hasSelection }]"
      :disabled="disabled"
      @click="toggleDropdown"
    >
      <span class="select-label" :class="{ 'has-value': hasSelection }">
        {{ selectedLabel }}
      </span>

      <span class="select-indicator">
        <span class="indicator-dot" v-if="hasSelection" />
        <svg
          class="chevron-icon"
          :class="{ 'is-rotated': isOpen }"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          stroke-width="2.5"
          stroke-linecap="round"
          stroke-linejoin="round"
        >
          <polyline points="6 9 12 15 18 9" />
        </svg>
      </span>
    </button>

    <Teleport to="body">
      <Transition name="dropdown">
        <div v-if="isOpen" ref="dropdownRef" class="select-dropdown" :style="dropdownStyle">
          <div class="dropdown-inner">
            <button
              v-for="option in options"
              :key="option.value"
              type="button"
              class="dropdown-option"
              :class="{ 'is-selected': option.value === modelValue }"
              @click="selectOption(option)"
            >
              <span class="option-label">{{ option.label }}</span>
              <svg
                v-if="option.value === modelValue"
                class="check-icon"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                stroke-width="3"
                stroke-linecap="round"
                stroke-linejoin="round"
              >
                <polyline points="20 6 9 17 4 12" />
              </svg>
            </button>
          </div>

          <div class="dropdown-arrow" />
        </div>
      </Transition>
    </Teleport>
  </div>
</template>

<style scoped>
.select-filter {
  position: relative;
  display: inline-block;
}

.select-filter.is-disabled {
  opacity: 0.5;
  pointer-events: none;
}

.select-trigger {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  background: rgba(255, 255, 255, 0.9);
  backdrop-filter: blur(8px);
  border: 1.5px solid var(--border-soft, rgba(0, 0, 0, 0.06));
  border-radius: 12px;
  font-weight: 500;
  color: var(--text-muted, #64748b);
  cursor: pointer;
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.02);
}

.select-trigger:hover:not(:disabled) {
  border-color: rgba(59, 130, 246, 0.25);
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.04);
}

.select-trigger.is-active {
  border-color: var(--accent, #3b82f6);
  background: rgba(59, 130, 246, 0.04);
  color: var(--accent, #3b82f6);
}

.select-trigger.is-open {
  border-color: var(--accent, #3b82f6);
  box-shadow: 0 0 0 3px var(--filter-glow, rgba(59, 130, 246, 0.15));
}

.select-label {
  transition: color 0.15s ease;
}

.select-label.has-value {
  color: var(--text-default, #334155);
}

.select-indicator {
  display: flex;
  align-items: center;
  gap: 6px;
}

.indicator-dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--accent, #3b82f6);
  animation: dot-pulse 1.5s ease-in-out infinite;
}

@keyframes dot-pulse {
  0%, 100% {
    opacity: 1;
    transform: scale(1);
  }
  50% {
    opacity: 0.6;
    transform: scale(0.8);
  }
}

.chevron-icon {
  width: 14px;
  height: 14px;
  color: var(--text-muted, #64748b);
  transition: all 0.2s ease;
}

.chevron-icon.is-rotated {
  transform: rotate(180deg);
  color: var(--accent, #3b82f6);
}

/* 下拉菜单 */
.select-dropdown {
  z-index: 2147483647;
}

.dropdown-inner {
  background: rgba(255, 255, 255, 0.98);
  backdrop-filter: blur(16px) saturate(180%);
  border: 1px solid var(--border-soft, rgba(0, 0, 0, 0.06));
  border-radius: 14px;
  padding: 6px;
  box-shadow:
    0 4px 6px rgba(0, 0, 0, 0.03),
    0 10px 20px rgba(0, 0, 0, 0.06),
    0 0 0 1px rgba(0, 0, 0, 0.02);
}

.dropdown-option {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  padding: 10px 12px;
  border: none;
  border-radius: 10px;
  background: transparent;
  font-size: 14px;
  font-weight: 500;
  color: var(--text-default, #334155);
  text-align: left;
  cursor: pointer;
  transition: all 0.15s ease;
}

.dropdown-option:hover {
  background: rgba(59, 130, 246, 0.06);
  color: var(--text-strong, #0f172a);
}

.dropdown-option.is-selected {
  background: rgba(59, 130, 246, 0.1);
  color: var(--accent, #3b82f6);
}

.option-label {
  flex: 1;
}

.check-icon {
  width: 16px;
  height: 16px;
  color: var(--accent, #3b82f6);
  flex-shrink: 0;
}

.dropdown-arrow {
  position: absolute;
  top: -5px;
  left: 16px;
  width: 10px;
  height: 10px;
  background: rgba(255, 255, 255, 0.98);
  border-left: 1px solid var(--border-soft, rgba(0, 0, 0, 0.06));
  border-top: 1px solid var(--border-soft, rgba(0, 0, 0, 0.06));
  transform: rotate(45deg);
}

/* 下拉动画 */
.dropdown-enter-active,
.dropdown-leave-active {
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}

.dropdown-enter-from,
.dropdown-leave-to {
  opacity: 0;
  transform: translateY(-8px) scale(0.96);
}

.dropdown-enter-active .dropdown-inner,
.dropdown-leave-active .dropdown-inner {
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}

.dropdown-enter-from .dropdown-inner,
.dropdown-leave-to .dropdown-inner {
  transform: translateY(-4px);
}
</style>
