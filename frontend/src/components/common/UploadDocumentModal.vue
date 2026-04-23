<script setup>
/**
 * 上传文档弹窗组件
 * 功能描述：支持批量上传研报PDF文件，自动匹配Excel元数据
 * 依赖组件：无
 */
import { ref, computed, watch } from 'vue'
import { uploadDocumentsBatch } from '@/api/knowledge_base'

const props = defineProps({
  visible: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits(['close', 'success'])

const fileInput = ref(null)
const files = ref([])
const docType = ref('')
const isUploading = ref(false)
const uploadProgress = ref([])
const uploadResult = ref(null)

const hasFiles = computed(() => files.value.length > 0)
const totalFiles = computed(() => files.value.length)
const completedCount = computed(() => uploadProgress.value.filter(p => p.status === 'success' || p.status === 'error').length)
const progressPercent = computed(() => {
  if (totalFiles.value === 0) return 0
  return Math.round((completedCount.value / totalFiles.value) * 100)
})

watch(() => props.visible, (newVal) => {
  if (newVal) {
    resetState()
  }
})

const resetState = () => {
  files.value = []
  docType.value = ''
  uploadProgress.value = []
  uploadResult.value = null
  isUploading.value = false
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
  
  if (validFiles.length > 20) {
    alert('最多支持20个文件同时上传')
    files.value = validFiles.slice(0, 20)
  } else {
    files.value = validFiles
  }
  
  uploadProgress.value = files.value.map(file => ({
    name: file.name,
    status: 'pending',
    message: '等待上传'
  }))
  event.target.value = ''
}

const removeFile = (index) => {
  files.value.splice(index, 1)
  uploadProgress.value.splice(index, 1)
}

const handleUpload = async () => {
  if (files.value.length === 0 || isUploading.value) return
  
  isUploading.value = true
  uploadResult.value = null
  
  try {
    const formData = new FormData()
    files.value.forEach(file => {
      formData.append('files', file)
    })
    if (docType.value) {
      formData.append('doc_type', docType.value)
    }
    
    const response = await uploadDocumentsBatch(formData)
    const result = response?.data || response
    
    uploadProgress.value = files.value.map(file => {
      const docResult = result.documents?.find(d => d.title === file.name.replace('.pdf', ''))
      const errorResult = result.errors?.find(e => e.file === file.name)
      
      if (errorResult) {
        return { name: file.name, status: 'error', message: errorResult.error }
      } else if (docResult) {
        return { name: file.name, status: 'success', message: '上传成功' }
      }
      return { name: file.name, status: 'success', message: '上传成功' }
    })
    
    uploadResult.value = {
      total: result.total || files.value.length,
      success: result.success || 0,
      failed: result.failed || 0,
      documents: result.documents || [],
      errors: result.errors || []
    }
    
    if (result.success > 0) {
      emit('success')
    }
  } catch (error) {
    uploadProgress.value = files.value.map(file => ({
      name: file.name,
      status: 'error',
      message: error.message || '上传失败'
    }))
    uploadResult.value = {
      total: files.value.length,
      success: 0,
      failed: files.value.length,
      documents: [],
      errors: [{ error: error.message || '上传失败' }]
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
              <h3 class="text-lg font-semibold text-ink-900">上传研报文档</h3>
              <p class="mt-1 text-sm text-ink-500">支持批量上传，最多20个文件</p>
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
            <!-- 文档类型选择 -->
            <div class="space-y-2">
              <label class="text-sm font-medium text-ink-700">文档类型</label>
              <div class="flex gap-4">
                <label class="flex items-center gap-2 cursor-pointer">
                  <input
                    type="radio"
                    v-model="docType"
                    value=""
                    class="rounded border-ink-300 text-ink-900 focus:ring-ink-500"
                  />
                  <span class="text-sm text-ink-600">自动识别</span>
                </label>
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
              <p class="text-xs text-ink-400">选择"自动识别"时，系统将通过Excel元数据匹配确定文档类型</p>
            </div>

            <!-- 文件选择区域 -->
            <div
              v-if="!hasFiles"
              class="flex min-h-[180px] cursor-pointer flex-col items-center justify-center rounded-2xl border-2 border-dashed border-ink-200 bg-ink-50/50 hover:border-ink-300 hover:bg-ink-50"
              @click="triggerFileInput"
            >
              <div class="flex h-12 w-12 items-center justify-center rounded-xl bg-ink-900 text-white">
                <FontAwesomeIcon :icon="['fas', 'cloud-arrow-up']" class="text-xl" aria-hidden="true" />
              </div>
              <p class="mt-4 text-sm text-ink-600">点击或拖拽文件到此处</p>
              <p class="mt-1 text-xs text-ink-400">仅支持 PDF 格式，最多20个文件</p>
            </div>
            
            <!-- 文件列表 -->
            <div v-else class="space-y-4">
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
                  <div class="flex items-center gap-2">
                    <span
                      v-if="uploadProgress[index]?.status === 'success'"
                      class="text-xs text-green-600"
                    >
                      <FontAwesomeIcon :icon="['fas', 'check-circle']" class="mr-1" aria-hidden="true" />
                      成功
                    </span>
                    <span
                      v-else-if="uploadProgress[index]?.status === 'error'"
                      class="text-xs text-red-600"
                    >
                      <FontAwesomeIcon :icon="['fas', 'times-circle']" class="mr-1" aria-hidden="true" />
                      失败
                    </span>
                    <span
                      v-else-if="uploadProgress[index]?.status === 'uploading'"
                      class="text-xs text-accent-600"
                    >
                      <FontAwesomeIcon :icon="['fas', 'spinner']" spin class="mr-1" aria-hidden="true" />
                      上传中
                    </span>
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
              
              <!-- 上传进度 -->
              <div v-if="isUploading" class="space-y-2">
                <div class="flex items-center justify-between text-sm">
                  <span class="text-ink-600">上传进度</span>
                  <span class="font-medium text-ink-900">{{ completedCount }} / {{ totalFiles }}</span>
                </div>
                <div class="h-2 overflow-hidden rounded-full bg-ink-100">
                  <div
                    class="h-full rounded-full bg-accent-500 transition-all duration-300"
                    :style="{ width: `${progressPercent}%` }"
                  ></div>
                </div>
              </div>
              
              <!-- 上传结果 -->
              <div
                v-if="uploadResult"
                class="rounded-xl border p-4"
                :class="uploadResult.failed > 0 ? 'border-yellow-200 bg-yellow-50' : 'border-green-200 bg-green-50'"
              >
                <p
                  class="text-sm font-medium"
                  :class="uploadResult.failed > 0 ? 'text-yellow-700' : 'text-green-700'"
                >
                  上传完成：成功 {{ uploadResult.success }} 个，失败 {{ uploadResult.failed }} 个
                </p>
                <p class="mt-1 text-xs text-ink-500">
                  上传后需执行"切块"和"向量化"操作才能用于检索
                </p>
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
              :disabled="isUploading"
              @click="handleUpload"
            >
              <FontAwesomeIcon v-if="isUploading" :icon="['fas', 'spinner']" spin aria-hidden="true" />
              <span>{{ isUploading ? '上传中...' : '开始上传' }}</span>
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
