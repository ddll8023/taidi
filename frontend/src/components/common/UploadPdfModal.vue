<script setup>
import { ref, computed, watch } from 'vue'
import { uploadPdfIncremental } from '@/api/knowledge_base'

const props = defineProps({
  visible: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits(['close', 'success'])

const fileInput = ref(null)
const files = ref([])
const docType = ref('RESEARCH_REPORT')
const isUploading = ref(false)
const uploadResult = ref(null)

const hasFiles = computed(() => files.value.length > 0)

watch(() => props.visible, (newVal) => {
  if (newVal) {
    resetState()
  }
})

const resetState = () => {
  files.value = []
  docType.value = 'RESEARCH_REPORT'
  isUploading.value = false
  uploadResult.value = null
}

const triggerFileInput = () => {
  fileInput.value?.click()
}

const handleFileSelect = (event) => {
  const selectedFiles = Array.from(event.target.files || [])
  const validFiles = selectedFiles.filter(file => file.name.toLowerCase().endsWith('.pdf'))

  if (validFiles.length < selectedFiles.length) {
    alert('部分文件已过滤：仅支持 PDF 格式')
  }

  files.value = validFiles
  event.target.value = ''
}

const removeFile = (index) => {
  files.value.splice(index, 1)
}

const canUpload = computed(() => {
  return hasFiles.value && !isUploading.value
})

const handleUpload = async () => {
  if (!canUpload.value) return

  isUploading.value = true
  uploadResult.value = null

  try {
    const formData = new FormData()
    files.value.forEach(file => {
      formData.append('pdfs', file)
    })

    const response = await uploadPdfIncremental(formData, docType.value)
    const result = response?.data || response

    uploadResult.value = result

    if (result.processed_count > 0) {
      emit('success')
    }
  } catch (error) {
    uploadResult.value = {
      success: false,
      message: error.message || '上传失败',
      processed_count: 0,
      failed_count: files.value.length,
      total_processed: 0,
      total_pending: 0,
      failed_documents: [],
      errors: [{ error: error.message || '未知错误' }]
    }
  } finally {
    isUploading.value = false
  }
}

const handleClose = () => {
  if (isUploading.value) {
    if (!confirm('上传进行中，确定要关闭吗？')) return
  }
  emit('close')
}

const formatFileSize = (bytes) => {
  if (bytes < 1024) return bytes + ' B'
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB'
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB'
}
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
              <p class="mt-1 text-sm text-ink-500">增量上传PDF文件，系统自动匹配元数据并切块</p>
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
            <div class="space-y-2">
              <label class="text-sm font-medium text-ink-700">文档类型</label>
              <div class="flex gap-4">
                <label class="flex items-center gap-2 cursor-pointer">
                  <input
                    type="radio"
                    v-model="docType"
                    value="RESEARCH_REPORT"
                    class="rounded border-ink-300 text-ink-900 focus:ring-ink-500"
                  />
                  <span class="text-sm text-ink-600">个股研报</span>
                </label>
                <label class="flex items-center gap-2 cursor-pointer">
                  <input
                    type="radio"
                    v-model="docType"
                    value="INDUSTRY_REPORT"
                    class="rounded border-ink-300 text-ink-900 focus:ring-ink-500"
                  />
                  <span class="text-sm text-ink-600">行业研报</span>
                </label>
              </div>
            </div>

            <div
              v-if="!hasFiles"
              class="flex min-h-[180px] cursor-pointer flex-col items-center justify-center rounded-2xl border-2 border-dashed border-ink-200 bg-ink-50/50 hover:border-ink-300 hover:bg-ink-50"
              @click="triggerFileInput"
            >
              <div class="flex h-12 w-12 items-center justify-center rounded-xl bg-ink-900 text-white">
                <FontAwesomeIcon :icon="['fas', 'cloud-arrow-up']" class="text-xl" aria-hidden="true" />
              </div>
              <p class="mt-4 text-sm text-ink-600">点击或拖拽文件到此处</p>
              <p class="mt-1 text-xs text-ink-400">仅支持 PDF 格式</p>
            </div>

            <div v-else class="space-y-4">
              <div class="flex items-center justify-between">
                <span class="text-sm text-ink-600">已选择 {{ files.length }} 个文件</span>
                <button
                  v-if="!isUploading"
                  type="button"
                  class="text-xs text-accent-600 hover:text-accent-700"
                  @click="triggerFileInput"
                >
                  继续添加
                </button>
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
                    <p class="text-xs text-ink-400">{{ formatFileSize(file.size) }}</p>
                  </div>
                  <button
                    v-if="!isUploading"
                    type="button"
                    class="rounded p-1 text-ink-400 hover:bg-ink-100 hover:text-ink-600"
                    @click="removeFile(index)"
                  >
                    <FontAwesomeIcon :icon="['fas', 'xmark']" aria-hidden="true" />
                  </button>
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

            <div
              v-if="uploadResult"
              class="rounded-xl border p-4"
              :class="uploadResult.failed_count > 0 ? 'border-yellow-200 bg-yellow-50' : 'border-green-200 bg-green-50'"
            >
              <p
                class="text-sm font-medium"
                :class="uploadResult.success === false && uploadResult.processed_count === 0 ? 'text-red-700' : (uploadResult.failed_count > 0 ? 'text-yellow-700' : 'text-green-700')"
              >
                {{ uploadResult.message }}
              </p>
              <div class="mt-2 space-y-1">
                <p class="text-xs text-ink-500">成功处理：{{ uploadResult.processed_count }} 个</p>
                <p v-if="uploadResult.failed_count > 0" class="text-xs text-ink-500">失败：{{ uploadResult.failed_count }} 个</p>
                <p class="text-xs text-ink-500">累计已处理：{{ uploadResult.total_processed }} 个，剩余待上传：{{ uploadResult.total_pending }} 个</p>
              </div>
              <div v-if="uploadResult.failed_documents?.length" class="mt-2">
                <p class="text-xs font-medium text-red-600">失败文档：</p>
                <div v-for="(doc, i) in uploadResult.failed_documents" :key="i" class="mt-1 rounded-lg bg-white/60 p-2">
                  <p class="text-xs text-ink-700">{{ doc.pdf_name }}</p>
                  <p class="text-xs text-red-500">{{ doc.reason }}</p>
                  <p v-if="doc.suggestion" class="text-xs text-ink-400">{{ doc.suggestion }}</p>
                </div>
              </div>
            </div>
          </div>

          <div class="flex justify-end gap-3 border-t border-black/5 px-6 py-4">
            <button
              v-if="!hasFiles || uploadResult"
              type="button"
              class="shell-button-secondary"
              @click="handleClose"
            >
              关闭
            </button>
            <button
              v-if="hasFiles && !uploadResult"
              type="button"
              class="shell-button-secondary"
              :disabled="isUploading"
              @click="resetState"
            >
              重新选择
            </button>
            <button
              v-if="hasFiles && !uploadResult"
              type="button"
              class="shell-button"
              :disabled="!canUpload"
              @click="handleUpload"
            >
              <FontAwesomeIcon v-if="isUploading" :icon="['fas', 'spinner']" spin aria-hidden="true" />
              <span>{{ isUploading ? '处理中...' : '开始上传' }}</span>
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
