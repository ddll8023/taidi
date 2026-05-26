<template>
  <div
    class="flex flex-col gap-3 border-t border-black/5 px-5 py-3 sm:flex-row sm:items-center sm:justify-between"
  >
    <!-- 左侧：页码信息 + 每页条数 -->
    <div class="flex items-center gap-3 text-sm text-ink-500">
      <span>第 {{ modelPage }} / {{ totalPages }} 页，共 {{ total }} 条</span>

      <div class="flex items-center gap-1.5">
        <span class="text-xs text-ink-400">每页</span>
        <select
          :value="modelPageSize"
          class="cursor-pointer appearance-none rounded-lg border border-black/10 bg-white px-2 py-1 pr-6 text-xs text-ink-700 transition-colors hover:border-ink-300 focus:border-accent-400 focus:outline-none focus:ring-[3px] focus:ring-accent-400/20"
          @change="handlePageSizeChange"
        >
          <option
            v-for="opt in pageSizeOptions"
            :key="opt.value"
            :value="opt.value"
          >
            {{ opt.label }}
          </option>
        </select>
        <FontAwesomeIcon
          :icon="['fas', 'chevron-down']"
          class="pointer-events-none -ml-5 text-[0.55rem] text-ink-400"
          aria-hidden="true"
        />
      </div>
    </div>

    <!-- 右侧：翻页按钮 -->
    <div class="flex items-center gap-2">
      <button
        class="inline-flex items-center gap-1.5 rounded-xl border border-black/10 px-3 py-1.5 text-xs font-medium text-ink-600 transition-all duration-150 hover:border-ink-300 hover:bg-ink-50/60 active:scale-[0.97] disabled:cursor-not-allowed disabled:opacity-40"
        :disabled="modelPage <= 1"
        @click="handlePrev"
      >
        <FontAwesomeIcon :icon="['fas', 'angle-left']" class="text-[0.65em]" aria-hidden="true" />
        上一页
      </button>
      <button
        class="inline-flex items-center gap-1.5 rounded-xl border border-black/10 px-3 py-1.5 text-xs font-medium text-ink-600 transition-all duration-150 hover:border-ink-300 hover:bg-ink-50/60 active:scale-[0.97] disabled:cursor-not-allowed disabled:opacity-40"
        :disabled="modelPage >= totalPages"
        @click="handleNext"
      >
        下一页
        <FontAwesomeIcon :icon="['fas', 'angle-right']" class="text-[0.65em]" aria-hidden="true" />
      </button>
    </div>
  </div>
</template>

<script setup>
/**
 * PaginationBar
 * 表格分页底栏组件，支持每页条数切换（10/20/50）和翻页
 *
 * 用法示例：
 *   <PaginationBar
 *     v-model:page="listState.page"
 *     v-model:pageSize="listState.pageSize"
 *     :total="listState.total"
 *     @change="fetchList"
 *   />
 */
import { computed } from 'vue'

const props = defineProps({
  page: {
    type: Number,
    required: true
  },
  pageSize: {
    type: Number,
    default: 10
  },
  total: {
    type: Number,
    default: 0
  },
  pageSizeOptions: {
    type: Array,
    default: () => [
      { value: 10, label: '10条/页' },
      { value: 20, label: '20条/页' },
      { value: 50, label: '50条/页' }
    ]
  }
})

const emit = defineEmits(['update:page', 'update:pageSize', 'change'])

const modelPage = computed(() => props.page)
const modelPageSize = computed(() => props.pageSize)

const totalPages = computed(() => {
  const size = Number(props.pageSize) || 10
  return Math.max(1, Math.ceil((Number(props.total) || 0) / size))
})

const handlePrev = () => {
  const prev = Math.max(1, props.page - 1)
  if (prev === props.page) return
  emit('update:page', prev)
  emit('change')
}

const handleNext = () => {
  const next = Math.min(totalPages.value, props.page + 1)
  if (next === props.page) return
  emit('update:page', next)
  emit('change')
}

const handlePageSizeChange = (event) => {
  const newSize = Number(event.target.value)
  emit('update:pageSize', newSize)
  emit('update:page', 1)
  emit('change')
}
</script>
