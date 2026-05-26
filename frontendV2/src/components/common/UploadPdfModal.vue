<script setup>
/**
 * 上传研报PDF弹窗
 * 功能描述：批量选择 PDF 文件上传至知识库，按文件名自动匹配元数据
 * 依赖组件：无
 */
import { ref, watch } from 'vue'
import { uploadKnowledgeDocuments } from '@/api/knowledgeBase'

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
const uploadError = ref('')
const uploadResult = ref(null)

watch(() => props.visible, (newVal) => {
  if (newVal) {
    files.value = []
    isUploading.value = false
    uploadError.value = ''
    uploadResult.value = null
  }
})

const triggerFileInput = () => {
  fileInput.value?.click()
}

const handleFileSelect = (event) => {
  const selectedFiles = Array.from(event.target.files || [])
  const pdfFiles = selectedFiles.filter(file => file.name.toLowerCase().endsWith('.pdf'))
  files.value = [...files.value, ...pdfFiles]
  uploadError.value = ''
  uploadResult.value = null
  event.target.value = ''
}

const removeFile = (index) => {
  files.value.splice(index, 1)
}

const handleClose = () => {
  emit('close')
}

const handleUpload = async () => {
  if (!files.value.length) return

  isUploading.value = true
  uploadError.value = ''
  uploadResult.value = null

  try {
    const res = await uploadKnowledgeDocuments(files.value)
    const payload = res.data || res
    uploadResult.value = payload

    if (payload.success_count > 0) {
      emit('success')
    }
  } catch (e) {
    uploadError.value = e.message || '上传失败，请稍后重试'
  } finally {
    isUploading.value = false
  }
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
              <p class="mt-1 text-sm text-ink-500">上传PDF文件，系统自动按标题匹配元数据</p>
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
            <!-- 文件选择区 -->
            <div
              class="flex min-h-[120px] cursor-pointer flex-col items-center justify-center rounded-2xl border-2 border-dashed border-ink-200 bg-ink-50/50 hover:border-ink-300 hover:bg-ink-50"
              @click="triggerFileInput"
            >
              <div class="flex h-12 w-12 items-center justify-center rounded-xl bg-ink-900 text-white">
                <FontAwesomeIcon :icon="['fas', 'cloud-arrow-up']" class="text-xl" aria-hidden="true" />
              </div>
              <p class="mt-4 text-sm text-ink-600">点击选择PDF文件（可多选）</p>
              <p class="mt-1 text-xs text-ink-400">仅支持 PDF 格式，按文件名匹配元数据</p>
            </div>

            <!-- 已选文件列表 -->
            <div v-if="files.length" class="space-y-2">
              <div class="flex items-center justify-between">
                <span class="text-sm text-ink-600">已选择 {{ files.length }} 个文件</span>
                <button
                  type="button"
                  class="text-xs text-ink-400 hover:text-danger"
                  @click="files = []"
                >
                  清空
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
                  </div>
                  <button
                    type="button"
                    class="shrink-0 rounded-full p-1 text-ink-300 hover:text-danger"
                    :disabled="isUploading"
                    @click.stop="removeFile(index)"
                  >
                    <FontAwesomeIcon :icon="['fas', 'xmark']" class="text-xs" aria-hidden="true" />
                  </button>
                </div>
              </div>
            </div>

            <!-- 上传错误 -->
            <div
              v-if="uploadError"
              class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
            >
              <p class="flex items-center gap-2">
                <FontAwesomeIcon :icon="['fas', 'triangle-exclamation']" class="text-red-500" aria-hidden="true" />
                {{ uploadError }}
              </p>
            </div>

            <!-- 上传结果 -->
            <div
              v-if="uploadResult"
              class="rounded-xl border border-green-200 bg-green-50 px-4 py-3"
            >
              <p class="text-sm font-medium text-green-800">
                上传完成：成功 {{ uploadResult.success_count }} 个，失败 {{ uploadResult.failed_count }} 个
              </p>

              <div v-if="uploadResult.success_documents?.length" class="mt-2 space-y-1">
                <p class="text-xs font-medium text-green-700">成功文档：</p>
                <div
                  v-for="doc in uploadResult.success_documents"
                  :key="doc.document_id"
                  class="text-xs text-green-600"
                >
                  <FontAwesomeIcon :icon="['fas', 'check']" class="mr-1" aria-hidden="true" />
                  {{ doc.title }}（ID: {{ doc.document_id }}）
                </div>
              </div>

              <div v-if="uploadResult.failed_files?.length" class="mt-2 space-y-1">
                <p class="text-xs font-medium text-red-700">失败文件：</p>
                <div
                  v-for="item in uploadResult.failed_files"
                  :key="item.file_name"
                  class="text-xs text-red-600"
                >
                  <FontAwesomeIcon :icon="['fas', 'xmark']" class="mr-1" aria-hidden="true" />
                  {{ item.file_name }} — {{ item.error }}
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
            <button type="button" class="shell-button-secondary" :disabled="isUploading" @click="handleClose">
              {{ uploadResult ? '关闭' : '取消' }}
            </button>
            <button
              type="button"
              class="shell-button"
              :disabled="isUploading || !files.length"
              @click="handleUpload"
            >
              <FontAwesomeIcon
                v-if="isUploading"
                :icon="['fas', 'spinner']"
                spin
                class="mr-1.5"
                aria-hidden="true"
              />
              <span>{{ isUploading ? '上传中...' : `上传（${files.length}）` }}</span>
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