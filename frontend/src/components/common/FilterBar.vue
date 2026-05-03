<script setup>
/**
 * 筛选栏组件
 * 功能描述：组合搜索输入框和下拉筛选器的统一筛选栏
 * 依赖组件：SearchInput.vue, SelectFilter.vue
 */
import { computed } from 'vue'
import SearchInput from './SearchInput.vue'
import SelectFilter from './SelectFilter.vue'

const props = defineProps({
  searchValue: {
    type: String,
    default: ''
  },
  parseStatusOptions: {
    type: Array,
    required: true
  },
  parseStatusValue: {
    type: [String, Number],
    default: ''
  },
  isLoading: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits([
  'update:searchValue',
  'update:parseStatusValue',
  'search',
  'reset'
])

const activeFiltersCount = computed(() => {
  let count = 0
  if (props.searchValue && props.searchValue.trim()) count++
  if (props.parseStatusValue !== '') count++
  return count
})

const hasActiveFilters = computed(() => activeFiltersCount.value > 0)

const handleSearchUpdate = (value) => {
  emit('update:searchValue', value)
}

const handleSearch = (value) => {
  emit('search', value)
}

const handleParseStatusChange = (value) => {
  emit('update:parseStatusValue', value)
  emit('search')
}

const handleReset = () => {
  emit('update:searchValue', '')
  emit('update:parseStatusValue', '')
  emit('reset')
}
</script>

<template>
  <div class="filter-bar" :class="{ 'has-active-filters': hasActiveFilters }">
    <div class="filter-bar-inner">
      <!-- 搜索区域 -->
      <div class="search-section">
        <SearchInput
          :model-value="searchValue"
          placeholder="搜索文件名..."
          width="280px"
          :disabled="isLoading"
          @update:model-value="handleSearchUpdate"
          @search="handleSearch"
        />
      </div>

      <!-- 分隔线 -->
      <div class="filter-divider" />

      <!-- 筛选器区域 -->
      <div class="filters-section">
        <SelectFilter
          :model-value="parseStatusValue"
          :options="parseStatusOptions"
          placeholder="全部解析状态"
          :disabled="isLoading"
          @update:model-value="handleParseStatusChange"
        />
      </div>

      <!-- 重置按钮 -->
      <Transition name="reset-fade">
        <button
          v-if="hasActiveFilters"
          type="button"
          class="reset-button"
          :disabled="isLoading"
          @click="handleReset"
        >
          <svg
            class="reset-icon"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            stroke-width="2"
            stroke-linecap="round"
            stroke-linejoin="round"
          >
            <path d="M18 6 6 18M6 6l12 12" />
          </svg>
          <span>重置</span>
          <span class="filter-count">{{ activeFiltersCount }}</span>
        </button>
      </Transition>
    </div>

    <!-- 底部装饰线 -->
    <div class="filter-bar-decoration">
      <div class="decoration-line" />
    </div>
  </div>
</template>

<style scoped>
.filter-bar {
  position: relative;
  background: linear-gradient(
    180deg,
    rgba(255, 255, 255, 0.95) 0%,
    rgba(255, 255, 255, 0.85) 100%
  );
  backdrop-filter: blur(12px);
  border: 1px solid var(--border-soft, rgba(0, 0, 0, 0.06));
  border-radius: 16px;
  padding: 16px 20px;
  transition: all 0.25s ease;
  box-shadow:
    0 1px 3px rgba(0, 0, 0, 0.02),
    0 2px 8px rgba(0, 0, 0, 0.02);
}

.filter-bar:hover {
  box-shadow:
    0 2px 4px rgba(0, 0, 0, 0.02),
    0 4px 12px rgba(0, 0, 0, 0.03);
}

.filter-bar.has-active-filters {
  border-color: rgba(59, 130, 246, 0.15);
  box-shadow:
    0 1px 3px rgba(0, 0, 0, 0.02),
    0 4px 12px rgba(59, 130, 246, 0.08);
}

.filter-bar-inner {
  display: flex;
  align-items: center;
  gap: 16px;
}

.search-section {
  flex-shrink: 0;
}

.filter-divider {
  width: 1px;
  height: 32px;
  background: linear-gradient(
    180deg,
    transparent 0%,
    var(--border-strong, rgba(0, 0, 0, 0.1)) 50%,
    transparent 100%
  );
  flex-shrink: 0;
}

.filters-section {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}

.filter-separator {
  width: 4px;
  height: 4px;
  border-radius: 50%;
  background: var(--border-strong, rgba(0, 0, 0, 0.1));
  flex-shrink: 0;
}

.reset-button {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  margin-left: auto;
  padding: 8px 14px;
  border: 1.5px solid rgba(239, 68, 68, 0.2);
  border-radius: 10px;
  background: rgba(239, 68, 68, 0.04);
  font-size: 13px;
  font-weight: 600;
  color: var(--danger, #ef4444);
  cursor: pointer;
  transition: all 0.2s ease;
}

.reset-button:hover:not(:disabled) {
  background: rgba(239, 68, 68, 0.1);
  border-color: rgba(239, 68, 68, 0.3);
  transform: scale(1.02);
}

.reset-button:active:not(:disabled) {
  transform: scale(0.98);
}

.reset-button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.reset-icon {
  width: 12px;
  height: 12px;
}

.filter-count {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 18px;
  height: 18px;
  padding: 0 5px;
  border-radius: 9px;
  background: var(--danger, #ef4444);
  font-size: 11px;
  font-weight: 700;
  color: white;
  line-height: 1;
}

.filter-bar-decoration {
  position: absolute;
  bottom: 0;
  left: 20px;
  right: 20px;
  height: 1px;
  overflow: hidden;
}

.decoration-line {
  width: 100%;
  height: 100%;
  background: linear-gradient(
    90deg,
    transparent 0%,
    rgba(59, 130, 246, 0.1) 20%,
    rgba(59, 130, 246, 0.3) 50%,
    rgba(59, 130, 246, 0.1) 80%,
    transparent 100%
  );
  opacity: 0;
  transform: scaleX(0);
  transition: all 0.3s ease;
}

.filter-bar.has-active-filters .decoration-line {
  opacity: 1;
  transform: scaleX(1);
}

/* 重置按钮动画 */
.reset-fade-enter-active,
.reset-fade-leave-active {
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}

.reset-fade-enter-from,
.reset-fade-leave-to {
  opacity: 0;
  transform: translateX(8px) scale(0.9);
}

/* 响应式布局 */
@media (max-width: 768px) {
  .filter-bar-inner {
    flex-direction: column;
    align-items: stretch;
    gap: 12px;
  }

  .filter-divider {
    width: 100%;
    height: 1px;
    background: linear-gradient(
      90deg,
      transparent 0%,
      var(--border-strong, rgba(0, 0, 0, 0.1)) 50%,
      transparent 100%
    );
  }

  .search-section {
    width: 100%;
  }

  .search-section :deep(.search-input-wrapper) {
    width: 100% !important;
  }

  .filters-section {
    justify-content: flex-start;
  }

  .reset-button {
    margin-left: 0;
    align-self: flex-start;
  }
}
</style>
