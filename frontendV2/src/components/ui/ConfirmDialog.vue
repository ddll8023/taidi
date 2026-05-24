<script setup>
/**
 * ConfirmDialog
 * 通用确认弹窗，替代浏览器原生 window.confirm
 * 支持自定义标题、内容、按钮文案、加载态
 *
 * 依赖组件：BaseButton
 *
 * 用法示例：
 *   <ConfirmDialog
 *     :visible="showDialog"
 *     title="删除会话"
 *     :message="`确定要删除「${name}」吗？`"
 *     @confirm="handleConfirm"
 *     @close="showDialog = false"
 *   />
 */

import { computed } from 'vue'
import BaseButton from '@/components/ui/BaseButton.vue'

const props = defineProps({
  visible: {
    type: Boolean,
    default: false
  },

  title: {
    type: String,
    default: '确认操作'
  },

  message: {
    type: String,
    default: '确定要执行此操作吗？'
  },

  confirmText: {
    type: String,
    default: '确认'
  },

  cancelText: {
    type: String,
    default: '取消'
  },

  loading: {
    type: Boolean,
    default: false
  },

  variant: {
    type: String,
    default: 'danger',
    validator: (v) => ['danger', 'primary'].includes(v)
  }
})

const emit = defineEmits(['confirm', 'close'])

const confirmVariant = computed(() => props.variant)

const handleClose = () => {
  if (props.loading) return
  emit('close')
}

const handleConfirm = () => {
  if (props.loading) return
  emit('confirm')
}
</script>

<template>
  <Teleport to="body">
    <Transition name="modal">
      <div
        v-if="visible"
        class="fixed inset-0 z-50 flex items-center justify-center p-4"
      >
        <div
          class="absolute inset-0 bg-black/40 backdrop-blur-xs"
          @click="handleClose"
        />

        <div
          class="relative w-full max-w-sm animate-scale-in rounded-[24px] bg-white shadow-2xl"
        >
          <div class="p-6 text-center">
            <div
              class="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-full"
              :class="confirmVariant === 'danger' ? 'bg-red-50 text-danger' : 'bg-accent-50 text-accent-500'"
            >
              <FontAwesomeIcon
                :icon="['fas', 'triangle-exclamation']"
                class="text-xl"
                aria-hidden="true"
              />
            </div>

            <h3 class="text-base font-semibold text-ink-900">{{ title }}</h3>
            <p class="mt-2 text-sm leading-5 text-ink-500">{{ message }}</p>
          </div>

          <div class="flex gap-3 border-t border-black/5 px-6 py-4">
            <BaseButton
              variant="secondary"
              :full-width="true"
              :disabled="loading"
              @click="handleClose"
            >
              {{ cancelText }}
            </BaseButton>
            <BaseButton
              :variant="confirmVariant"
              :full-width="true"
              :loading="loading"
              @click="handleConfirm"
            >
              {{ confirmText }}
            </BaseButton>
          </div>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>