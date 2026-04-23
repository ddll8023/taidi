<script setup>
/**
 * 搜索输入框组件
 * 功能描述：带搜索图标的现代化搜索输入框，支持清除按钮
 * 依赖组件：无
 */
import { ref, computed, watch } from 'vue'

const props = defineProps({
  modelValue: {
    type: String,
    default: ''
  },
  placeholder: {
    type: String,
    default: '搜索...'
  },
  disabled: {
    type: Boolean,
    default: false
  },
  width: {
    type: String,
    default: '256px'
  }
})

const emit = defineEmits(['update:modelValue', 'search'])

const isFocused = ref(false)
const inputRef = ref(null)

const showClear = computed(() => props.modelValue && props.modelValue.length > 0)

const handleInput = (event) => {
  emit('update:modelValue', event.target.value)
}

const handleKeyup = (event) => {
  if (event.key === 'Enter') {
    emit('search', props.modelValue)
  }
}

const handleClear = () => {
  emit('update:modelValue', '')
  inputRef.value?.focus()
}

const handleFocus = () => {
  isFocused.value = true
}

const handleBlur = () => {
  isFocused.value = false
}

watch(() => props.modelValue, () => {
  if (props.modelValue) {
    inputRef.value?.focus()
  }
})
</script>

<template>
  <div
    class="search-input-wrapper"
    :class="{ 'is-focused': isFocused, 'is-disabled': disabled }"
    :style="{ '--search-width': width }"
  >
    <div class="search-input-inner">
      <div class="search-icon-wrapper">
        <svg
          class="search-icon"
          :class="{ 'is-active': isFocused }"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          stroke-width="2"
          stroke-linecap="round"
          stroke-linejoin="round"
        >
          <circle cx="11" cy="11" r="8" />
          <path d="m21 21-4.35-4.35" />
        </svg>
      </div>

      <input
        ref="inputRef"
        type="text"
        class="search-input"
        :value="modelValue"
        :placeholder="placeholder"
        :disabled="disabled"
        @input="handleInput"
        @keyup="handleKeyup"
        @focus="handleFocus"
        @blur="handleBlur"
      />

      <Transition name="clear-fade">
        <button
          v-if="showClear && !disabled"
          type="button"
          class="clear-button"
          tabindex="-1"
          aria-label="清除搜索"
          @click="handleClear"
        >
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            stroke-width="2.5"
            stroke-linecap="round"
          >
            <path d="M18 6 6 18M6 6l12 12" />
          </svg>
        </button>
      </Transition>
    </div>

    <div class="focus-glow" />
  </div>
</template>

<style scoped>
.search-input-wrapper {
  position: relative;
  width: var(--search-width, 256px);
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}

.search-input-wrapper.is-disabled {
  opacity: 0.5;
  pointer-events: none;
}

.search-input-inner {
  position: relative;
  display: flex;
  align-items: center;
  background: rgba(255, 255, 255, 0.85);
  backdrop-filter: blur(12px) saturate(160%);
  border: 1.5px solid transparent;
  border-radius: 14px;
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow:
    0 1px 2px rgba(0, 0, 0, 0.02),
    inset 0 1px 0 rgba(255, 255, 255, 0.8);
}

.search-input-wrapper:hover .search-input-inner {
  border-color: rgba(59, 130, 246, 0.2);
  box-shadow:
    0 2px 4px rgba(0, 0, 0, 0.03),
    0 4px 12px rgba(59, 130, 246, 0.06),
    inset 0 1px 0 rgba(255, 255, 255, 0.8);
}

.search-input-wrapper.is-focused .search-input-inner {
  border-color: var(--accent, #3b82f6);
  box-shadow:
    0 0 0 3px var(--filter-glow, rgba(59, 130, 246, 0.15)),
    0 2px 4px rgba(0, 0, 0, 0.02);
  transform: translateY(-1px);
}

.search-icon-wrapper {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 44px;
  flex-shrink: 0;
}

.search-icon {
  width: 18px;
  height: 18px;
  color: var(--text-muted, #64748b);
  transition: all 0.2s ease;
}

.search-icon.is-active {
  color: var(--accent, #3b82f6);
  animation: pulse-glow 1.5s ease-in-out infinite;
}

@keyframes pulse-glow {
  0%, 100% {
    opacity: 1;
  }
  50% {
    opacity: 0.6;
  }
}

.search-input {
  flex: 1;
  padding: 10px 0;
  border: none;
  background: transparent;
  font-size: 14px;
  font-weight: 500;
  color: var(--text-strong, #0f172a);
  outline: none;
}

.search-input::placeholder {
  color: var(--text-muted, #64748b);
  font-weight: 400;
}

.clear-button {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  margin-right: 6px;
  border: none;
  border-radius: 8px;
  background: rgba(0, 0, 0, 0.04);
  color: var(--text-muted, #64748b);
  cursor: pointer;
  transition: all 0.15s ease;
}

.clear-button:hover {
  background: rgba(239, 68, 68, 0.1);
  color: var(--danger, #ef4444);
}

.clear-button svg {
  width: 14px;
  height: 14px;
}

.focus-glow {
  position: absolute;
  bottom: -2px;
  left: 50%;
  transform: translateX(-50%);
  width: 0;
  height: 2px;
  background: linear-gradient(90deg, transparent, var(--accent, #3b82f6), transparent);
  border-radius: 1px;
  opacity: 0;
  transition: all 0.3s ease;
}

.search-input-wrapper.is-focused .focus-glow {
  width: 60%;
  opacity: 1;
}

/* 清除按钮动画 */
.clear-fade-enter-active,
.clear-fade-leave-active {
  transition: all 0.15s ease;
}

.clear-fade-enter-from,
.clear-fade-leave-to {
  opacity: 0;
  transform: scale(0.8);
}
</style>
