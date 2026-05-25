<script setup>
import { ref, watch } from 'vue'

const props = defineProps({
  visible: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits(['close', 'success'])

const fileInput = ref(null)
const files = ref([])
const isUploading = ref(false)

watch(() => props.visible, (newVal) => {
  if (newVal) {
    files.value = []
    isUploading.value = false
  }
})

const triggerFileInput = () => {
  fileInput.value?.click()
}

const handleFileSelect = (event) => {
  const selectedFiles = Array.from(event.target.files || [])
  files.value = selectedFiles.filter(file => file.name.toLowerCase().endsWith('.pdf'))
  event.target.value = ''
}

const handleClose = () => {
  emit('close')
}

// TODO: 后续接入 uploadPdf 接口
</script>

<template>
  <Teleport to="body">
    <Transition name="modal">
      <div v-if="visible" class="fixed inset-0 z-50 flex items-center justify-center p-4">
        <div class="absolute inset-0 bg-black/50 backdrop-blur-sm" @click="handleClose"></div>

        <div class="relative w-full max-w-xl rounded-[32px] bg-white shadow-2xl">
          <div class="flex items-center justify-between border-b border-black/5 px-6 py-5">
            <div>
              <h3 class="text-lg font-semibold text-ink-900">上传研报PDF</h3>
              <p class="mt-1 text-sm text-ink-500">上传PDF文件，系统自动匹配元数据</p>
            </div>
            <button
              type="button"
              class="rounded-full p-2 text-ink-400 hover:bg-ink-100 hover:text-ink-600"
              @click="handleClose"
            >
              <FontAwesomeIcon :icon="['fas', 'xmark']" class="text-lg" aria-hidden="true" />
            </button>
          </div>

          <div class="p-6 space-y-4">
            <div
              v-if="!files.length"
              class="flex min-h-[180px] cursor-pointer flex-col items-center justify-center rounded-2xl border-2 border-dashed border-ink-200 bg-ink-50/50 hover:border-ink-300 hover:bg-ink-50"
              @click="triggerFileInput"
            >
              <div class="flex h-12 w-12 items-center justify-center rounded-xl bg-ink-900 text-white">
                <FontAwesomeIcon :icon="['fas', 'cloud-arrow-up']" class="text-xl" aria-hidden="true" />
              </div>
              <p class="mt-4 text-sm text-ink-600">点击选择PDF文件</p>
              <p class="mt-1 text-xs text-ink-400">仅支持 PDF 格式</p>
            </div>

            <div v-else class="space-y-4">
              <div class="flex items-center justify-between">
                <span class="text-sm text-ink-600">已选择 {{ files.length }} 个文件</span>
              </div>
              <div class="max-h-[200px] overflow-y-auto rounded-xl border border-black/5">
                <div
                  v-for="(file, index) in files"
                  :key="index"
                  class="flex items-center gap-3 border-b border-black/5 px-4 py-3 last:border-b-0"
                >
                  <div class="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-red-50 text-red-500">
                    <FontAwesomeIcon :icon="['fas', 'file-pdf']" aria-hidden="true" />
                  </div>
                  <div class="min-w-0 flex-1">
                    <p class="truncate text-sm font-medium text-ink-900">{{ file.name }}</p>
                  </div>
                </div>
              </div>
            </div>

            <input
              ref="fileInput"
              type="file"
              accept=".pdf"
              multiple
              class="hidden"
              @change="handleFileSelect"
            />
          </div>

          <div class="flex justify-end gap-3 border-t border-black/5 px-6 py-4">
            <button type="button" class="shell-button-secondary" @click="handleClose">关闭</button>
            <button
              type="button"
              class="shell-button"
              :disabled="isUploading"
            >
              <span>上传</span>
            </button>
          </div>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>

<style scoped>
.modal-enter-active,
.modal-leave-active {
  transition: opacity 0.2s ease;
}

.modal-enter-from,
.modal-leave-to {
  opacity: 0;
}

.modal-enter-active .relative,
.modal-leave-active .relative {
  transition: transform 0.2s ease;
}

.modal-enter-from .relative,
.modal-leave-to .relative {
  transform: scale(0.95);
}
</style>