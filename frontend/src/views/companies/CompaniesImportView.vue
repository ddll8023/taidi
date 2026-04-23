<script setup>
import { ref } from 'vue'
import { importCompanies } from '@/api/analysis_data'
import AppEmptyState from '@/components/common/AppEmptyState.vue'
import SurfacePanel from '@/components/common/SurfacePanel.vue'

const fileInput = ref(null)
const isUploading = ref(false)
const uploadMessage = ref({ type: '', text: '' })
const importResult = ref(null)

const triggerFileInput = () => {
  fileInput.value?.click()
}

const handleFileChange = async (event) => {
  const file = event.target.files?.[0]
  if (!file) return

  if (!file.name.toLowerCase().endsWith('.xlsx') && !file.name.toLowerCase().endsWith('.xls')) {
    uploadMessage.value = { type: 'error', text: '仅支持 Excel 文件（.xlsx 或 .xls）' }
    return
  }

  isUploading.value = true
  uploadMessage.value = { type: '', text: '' }
  importResult.value = null

  try {
    const result = await importCompanies(file)
    importResult.value = result
    uploadMessage.value = { type: 'success', text: '导入成功' }
    event.target.value = ''
  } catch (error) {
    uploadMessage.value = { type: 'error', text: error.message || '导入失败' }
  } finally {
    isUploading.value = false
  }
}
</script>

<template>
  <div class="h-full space-y-6">
    <SurfacePanel
      title="导入公司基本信息"
      description="导入附件1的上市公司基本信息数据，作为财报数据的基础主数据。导入后才能上传财报PDF文件。"
      eyebrow="Import"
    >
      <div class="grid gap-6 xl:grid-cols-[minmax(0,1.3fr)_320px]">
        <div
          class="flex min-h-[360px] flex-col items-center justify-center rounded-[32px] border border-dashed border-ink-300 bg-white/80 px-6 py-10 text-center"
        >
          <div class="flex h-16 w-16 items-center justify-center rounded-[24px] bg-ink-900 text-white shadow-soft">
            <FontAwesomeIcon :icon="['fas', 'building']" class="text-xl" aria-hidden="true" />
          </div>
          <h3 class="mt-6 text-xl font-semibold text-ink-900">导入附件1公司信息</h3>
          <p class="mt-3 max-w-2xl text-sm leading-6 text-ink-600">
            上传附件1的Excel文件（中药上市公司基本信息），系统将自动解析并导入公司基本信息到数据库。导入成功后即可上传财报PDF文件。
          </p>
          <div class="mt-8 flex flex-wrap justify-center gap-3">
            <button type="button" class="shell-button" :disabled="isUploading" @click="triggerFileInput">
              <FontAwesomeIcon :icon="['fas', 'file-excel']" aria-hidden="true" />
              <span>{{ isUploading ? '导入中...' : '选择Excel文件' }}</span>
            </button>
          </div>
          <input type="file" ref="fileInput" accept=".xlsx,.xls" class="hidden" @change="handleFileChange" />
        </div>

        <div class="flex flex-col gap-4">
          <div class="rounded-[28px] border border-black/5 bg-ink-50/80 p-5">
            <p class="shell-kicker">导入说明</p>
            <ul class="mt-4 space-y-3 text-sm leading-6 text-ink-600">
              <li>仅支持 Excel 文件（.xlsx 或 .xls 格式）。</li>
              <li>文件需包含"基本信息表"工作表。</li>
              <li>支持增量导入，已存在的记录会被更新。</li>
              <li>导入成功后才能上传财报PDF文件。</li>
            </ul>
          </div>

          <div
            v-if="uploadMessage.text"
            class="rounded-[28px] border border-black/5 p-5"
            :class="{
              'bg-green-50/80 border-green-200': uploadMessage.type === 'success',
              'bg-red-50/80 border-red-200': uploadMessage.type === 'error',
            }"
          >
            <p
              class="text-sm leading-6"
              :class="{
                'text-green-700': uploadMessage.type === 'success',
                'text-red-700': uploadMessage.type === 'error',
              }"
            >
              {{ uploadMessage.text }}
            </p>
            <div v-if="importResult && uploadMessage.type === 'success'" class="mt-3 text-sm text-green-600">
              <p>总计：{{ importResult.data?.total || 0 }} 条</p>
              <p>新增：{{ importResult.data?.inserted || 0 }} 条</p>
              <p>更新：{{ importResult.data?.updated || 0 }} 条</p>
            </div>
          </div>
          <AppEmptyState
            v-else
            title="等待导入"
            description="请选择附件1的Excel文件进行导入。"
          />
        </div>
      </div>
    </SurfacePanel>
  </div>
</template>

<style scoped>
.hidden {
  display: none;
}
</style>
