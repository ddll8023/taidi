<script setup>
/**
 * Task3ReferencesModal
 * 功能描述：整题统一参考文献弹窗，汇总展示所有轮次的 references
 */
defineProps({
  visible: {
    type: Boolean,
    default: false
  },
  references: {
    type: Array,
    default: () => []
  }
})

const emit = defineEmits(['close'])

function resolveImagePath(path) {
  if (!path) return ''
  if (path.startsWith('./result/')) {
    return path.replace('./result/', '/api/v1/chat/images/')
  }
  return path
}
</script>

<template>
  <div
    v-if="visible"
    class="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
    @click.self="emit('close')"
  >
    <div class="relative w-full max-w-2xl max-h-[80vh] rounded-2xl bg-white shadow-xl flex flex-col">
      <div class="flex shrink-0 items-center justify-between border-b border-black/5 px-6 py-4">
        <h3 class="text-base font-semibold text-ink-900">参考文献</h3>
        <button
          class="flex h-8 w-8 items-center justify-center rounded-lg text-ink-400 transition-colors hover:bg-ink-50 hover:text-ink-600"
          @click="emit('close')"
        >
          <FontAwesomeIcon :icon="['fas', 'times']" />
        </button>
      </div>

      <div class="flex-1 overflow-y-auto px-6 py-4">
        <div v-if="references.length === 0" class="py-12 text-center text-sm text-ink-400">
          当前题目暂无参考文献
        </div>

        <div v-else class="space-y-4">
          <div
            v-for="(ref, idx) in references"
            :key="idx"
            class="rounded-xl border border-blue-100 bg-blue-50/30 p-4"
          >
            <div class="mb-2 text-xs font-medium text-ink-500">来源 {{ idx + 1 }}</div>

            <div v-if="ref.paper_path" class="mb-2">
              <span class="text-xs text-ink-400">文献路径：</span>
              <span class="text-sm text-ink-700 break-all">{{ ref.paper_path }}</span>
            </div>

            <div v-if="ref.text" class="mb-2">
              <span class="text-xs text-ink-400">原文摘要：</span>
              <p class="mt-1 text-sm leading-6 text-ink-700">{{ ref.text }}</p>
            </div>

            <div v-if="ref.paper_image">
              <span class="text-xs text-ink-400">参考图表：</span>
              <img
                v-if="ref.paper_image.match(/\.(jpg|jpeg|png|gif|webp)$/i)"
                :src="resolveImagePath(ref.paper_image)"
                class="mt-1 max-w-sm rounded-lg border border-black/5"
              />
              <span v-else class="ml-2 text-sm text-ink-600">{{ ref.paper_image }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
