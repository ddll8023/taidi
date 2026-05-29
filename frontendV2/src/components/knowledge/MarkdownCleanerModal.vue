<script setup>
/**
 * Markdown 清洗编辑器组件
 * 双栏布局：左侧编辑 Markdown 源码，右侧 marked 实时渲染预览
 * 功能：编辑 → 用户选中文本手动标记表格语义 → 保存（覆写至 MinerU 输出的 md 文件）
 * 表格标注：用户在源码区选中表格区域，点击「标记表格」按钮，行内输入说明后
 *           在选中区域前后插入 `<!-- table: xxx -->` 和 `<!-- endtable -->` 包裹标志
 */
import { ref, computed, watch } from 'vue'
import { getParseResult, saveParseResult, toggleCleanStatus } from '@/api/knowledgeBase'
import { renderMarkdown } from '@/utils/markdown'

const props = defineProps({
  visible: {
    type: Boolean,
    default: false
  },
  document: {
    type: Object,
    default: null
  }
})

const emit = defineEmits(['close', 'success'])

// ── 状态 ──

const isSaving = ref(false)
const isLoading = ref(false)
const errorMessage = ref('')
const markdownContent = ref('')
const documentTitle = ref('')
const documentId = ref(0)
const documentCleanStatus = ref(0)
const saveMessage = ref({ type: '', text: '' })
const textareaRef = ref(null)

// 表格标注状态 — 行内输入框
const annotateDescription = ref('')
const selectionRange = ref({ start: 0, end: 0 })
const hasSelection = ref(false)
const inputRef = ref(null)

// 同步滚动
const isSyncingScroll = ref(false)
const previewWrapRef = ref(null)
const editorInnerRef = ref(null)

function onEditorScroll() {
  if (isSyncingScroll.value) return
  const textarea = textareaRef.value
  const preview = previewWrapRef.value
  if (!textarea || !preview) return
  const sh = textarea.scrollHeight - textarea.clientHeight
  const th = preview.scrollHeight - preview.clientHeight
  if (sh <= 0 || th <= 0) return
  isSyncingScroll.value = true
  const ratio = textarea.scrollTop / sh
  preview.scrollTop = ratio * th
  requestAnimationFrame(() => { isSyncingScroll.value = false })
}

function onPreviewScroll() {
  if (isSyncingScroll.value) return
  const preview = previewWrapRef.value
  const textarea = textareaRef.value
  if (!textarea || !preview) return
  const th = preview.scrollHeight - preview.clientHeight
  const sh = textarea.scrollHeight - textarea.clientHeight
  if (th <= 0 || sh <= 0) return
  isSyncingScroll.value = true
  const ratio = preview.scrollTop / th
  textarea.scrollTop = ratio * sh
  requestAnimationFrame(() => { isSyncingScroll.value = false })
}

// 行号计数
const lineCount = computed(() => markdownContent.value.split('\n').length)

// 渲染后的 HTML
const renderedHtml = computed(() => renderMarkdown(markdownContent.value))

// ── 弹窗生命周期 ──

watch(() => props.visible, async (newVal) => {
  if (newVal && props.document) {
    await loadParseResult()
  }
})

// ── 数据加载 ──

async function loadParseResult() {
  isLoading.value = true
  errorMessage.value = ''
  saveMessage.value = { type: '', text: '' }

  try {
    const res = await getParseResult(props.document.id)
    const payload = res.data || res
    markdownContent.value = payload.markdown_content || ''
    documentTitle.value = payload.title || ''
    documentId.value = payload.document_id || props.document.id
    documentCleanStatus.value = props.document.clean_status ?? 0
  } catch (e) {
    errorMessage.value = e.message || '加载解析结果失败'
  } finally {
    isLoading.value = false
  }
}

// ── 选中文本检测 ──

function onTextareaSelect() {
  const textarea = textareaRef.value
  if (!textarea) return
  hasSelection.value = textarea.selectionStart !== textarea.selectionEnd
}

// ── 表格标注 ──

function confirmAnnotate() {
  if (!annotateDescription.value.trim()) return
  if (!hasSelection.value) return

  const textarea = textareaRef.value
  if (!textarea) return
  const start = textarea.selectionStart
  const end = textarea.selectionEnd
  if (start === end) return

  const content = markdownContent.value
  const annotationStart = `<!-- table: ${annotateDescription.value.trim()} -->`
  const annotationEnd = `<!-- endtable -->`

  markdownContent.value = `${content.substring(0, start)}\n${annotationStart}\n${content.substring(start, end)}\n${annotationEnd}\n${content.substring(end)}`

  annotateDescription.value = ''
  hasSelection.value = false
  saveMessage.value = { type: 'success', text: `已标注：${annotateDescription.value.trim()}` }
  setTimeout(() => {
    if (saveMessage.value.type === 'success') saveMessage.value = { type: '', text: '' }
  }, 2000)
}

function onInputKeydown(event) {
  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault()
    confirmAnnotate()
  }
}

// ── 编辑区快捷键 ──

function handleEditorKeydown(event) {
  if ((event.ctrlKey || event.metaKey) && event.key === 's') {
    event.preventDefault()
    handleSave()
  }
  if (event.key === 'Tab') {
    event.preventDefault()
    const textarea = event.target
    const start = textarea.selectionStart
    const end = textarea.selectionEnd
    markdownContent.value = markdownContent.value.substring(0, start) + '  ' + markdownContent.value.substring(end)
    requestAnimationFrame(() => {
      textarea.selectionStart = textarea.selectionEnd = start + 2
    })
  }
}

// ── 保存 ──

async function handleSave() {
  if (!markdownContent.value.trim()) {
    saveMessage.value = { type: 'error', text: 'Markdown 内容不能为空' }
    return
  }

  isSaving.value = true
  saveMessage.value = { type: '', text: '' }

  try {
    await saveParseResult(documentId.value, markdownContent.value)
    documentCleanStatus.value = 1
    saveMessage.value = { type: 'success', text: '清洗结果已保存' }
  } catch (e) {
    saveMessage.value = { type: 'error', text: e.message || '保存失败' }
  } finally {
    isSaving.value = false
  }
}

function handleClose() {
  annotateDescription.value = ''
  emit('close')
}

async function handleToggleClean() {
  try {
    await toggleCleanStatus(documentId.value)
    documentCleanStatus.value = documentCleanStatus.value === 1 ? 0 : 1
    const label = documentCleanStatus.value === 1 ? '已标记为已清洗' : '已解除清洗标记'
    saveMessage.value = { type: 'success', text: label }
    setTimeout(() => { saveMessage.value = { type: '', text: '' } }, 2000)
  } catch (e) {
    saveMessage.value = { type: 'error', text: e.message || '操作失败' }
  }
}
</script>

<template>
  <Teleport to="body">
    <div
      v-if="visible"
      class="fixed inset-0 z-50 flex items-center justify-center"
    >
      <!-- 遮罩 -->
      <div
        class="absolute inset-0 bg-black/40 backdrop-blur-sm"
        @click="handleClose"
      />

      <!-- 面板 -->
      <div
        class="relative flex h-[85vh] w-[90vw] max-w-1440 flex-col rounded-2xl bg-white shadow-2xl"
      >
        <!-- 标题栏 -->
        <div class="flex shrink-0 items-center justify-between rounded-t-2xl border-b border-black/5 px-5 py-3">
          <div class="flex items-center gap-3">
            <FontAwesomeIcon :icon="['fas', 'broom']" class="text-sm text-accent-500" aria-hidden="true" />
            <h2 class="text-sm font-semibold text-ink-900">Markdown 清洗</h2>
            <span class="max-w-sm truncate text-sm text-ink-400">{{ documentTitle }}</span>
          </div>
          <div class="flex items-center gap-2">
            <span class="text-xs text-ink-400">ID: {{ documentId }}</span>
            <span
              v-if="documentCleanStatus === 1"
              class="inline-flex items-center rounded-full bg-green-50 px-2 py-0.5 text-xs font-medium text-green-700 border border-green-200"
            >已清洗</span>
            <button
              class="flex h-7 w-7 items-center justify-center rounded-lg text-ink-400 hover:bg-black/5 hover:text-ink-600"
              @click="handleClose"
              aria-label="关闭"
            >
              <FontAwesomeIcon :icon="['fas', 'xmark']" aria-hidden="true" />
            </button>
          </div>
        </div>

        <!-- 内容区：双栏布局 -->
        <div class="flex min-h-0 flex-1 gap-0">
          <!-- 左侧：编辑区 -->
          <div class="flex w-1/2 flex-col border-r border-black/5">
            <div class="flex items-center justify-between border-b border-black/5 px-4 py-1.5">
              <span class="text-xs font-medium text-ink-500">Markdown 源码</span>
              <span class="text-xs text-ink-400 tabular-nums">{{ lineCount }} 行</span>
            </div>
            <div class="relative flex-1" style="display: flex; flex-direction: column;">
              <textarea
                ref="textareaRef"
                v-model="markdownContent"
                class="block flex-1 w-full resize-none bg-white p-4 font-mono text-sm leading-relaxed text-ink-800 outline-none"
                placeholder="加载解析结果后在此编辑 Markdown 内容..."
                spellcheck="false"
                @keydown="handleEditorKeydown"
                @mouseup="onTextareaSelect"
                @keyup="onTextareaSelect"
                @scroll="onEditorScroll"
              />

              <!-- 表格标记操作条 -->
              <div class="flex items-center border-t border-black/5 bg-gray-50/90 px-3 py-2 backdrop-blur">
                <FontAwesomeIcon :icon="['fas', 'table']" class="text-xs text-accent-400" aria-hidden="true" />
                <span class="ml-1 mr-2 text-xs text-ink-500 whitespace-nowrap">标记表格：</span>
                <input
                  ref="inputRef"
                  v-model="annotateDescription"
                  type="text"
                  class="flex-1 rounded-md border border-black/10 bg-white px-3 py-1.5 text-sm outline-none placeholder:text-ink-300 focus:border-accent-500 disabled:cursor-not-allowed disabled:opacity-40"
                  placeholder="选中表格区域后输入语义说明，例如：营收预测数据..."
                  :disabled="!hasSelection"
                  @keydown="onInputKeydown"
                />
                <button
                  class="ml-2 inline-flex items-center gap-1 rounded-md px-3 py-1.5 text-xs font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-40"
                  :class="hasSelection && annotateDescription.trim()
                    ? 'bg-accent-500 text-white hover:bg-accent-600'
                    : 'bg-gray-200 text-ink-400'"
                  :disabled="!hasSelection || !annotateDescription.trim()"
                  @click="confirmAnnotate"
                >
                  <FontAwesomeIcon :icon="['fas', 'check']" class="text-[10px]" aria-hidden="true" />
                  确定
                </button>
              </div>
            </div>
          </div>

          <!-- 右侧：预览区 -->
          <div class="flex w-1/2 flex-col">
            <div class="flex items-center border-b border-black/5 px-4 py-1.5">
              <span class="text-xs font-medium text-ink-500">渲染预览</span>
            </div>
            <div
              ref="previewWrapRef"
              class="h-full overflow-y-auto p-6 prose prose-sm max-w-none"
              @scroll="onPreviewScroll"
              v-html="renderedHtml"
            />
          </div>
        </div>

        <!-- 底部操作栏 -->
        <div class="flex shrink-0 items-center justify-between rounded-b-2xl border-t border-black/5 bg-gray-50 px-5 py-2.5">
          <div class="flex items-center gap-3">
            <div
              v-if="isLoading"
              class="flex items-center gap-2 text-sm text-ink-400"
            >
              <FontAwesomeIcon :icon="['fas', 'spinner']" spin aria-hidden="true" />
              <span>加载解析结果...</span>
            </div>

            <p
              v-else-if="errorMessage"
              class="text-sm text-danger"
            >
              <FontAwesomeIcon :icon="['fas', 'circle-exclamation']" class="mr-1" aria-hidden="true" />
              {{ errorMessage }}
            </p>

            <p
              v-else-if="saveMessage.text"
              class="text-sm"
              :class="saveMessage.type === 'success' ? 'text-success' : 'text-danger'"
            >
              <FontAwesomeIcon
                :icon="saveMessage.type === 'success' ? ['fas', 'check-circle'] : ['fas', 'circle-exclamation']"
                class="mr-1"
                aria-hidden="true"
              />
              {{ saveMessage.text }}
            </p>

            <span v-else class="text-xs text-ink-400">
              <kbd class="rounded border border-black/10 bg-white px-1.5 py-0.5 text-xs">Ctrl+S</kbd> 保存
            </span>
          </div>

          <div class="flex items-center gap-2">
            <button
              class="rounded-xl border border-black/10 bg-white px-4 py-1.5 text-sm font-medium text-ink-600 hover:bg-black/5"
              @click="handleClose"
            >
              取消
            </button>
            <button
              class="rounded-xl border bg-white px-4 py-1.5 text-sm font-medium hover:bg-black/5"
              :class="documentCleanStatus === 1
                ? 'border-red-200 text-red-600 hover:bg-red-50'
                : 'border-emerald-200 text-emerald-600 hover:bg-emerald-50'"
              @click="handleToggleClean"
            >
              {{ documentCleanStatus === 1 ? '解除清洗标记' : '标记为已清洗' }}
            </button>
            <button
              class="rounded-xl bg-accent-500 px-4 py-1.5 text-sm font-medium text-white hover:bg-accent-600 disabled:cursor-not-allowed disabled:opacity-50"
              :disabled="isSaving || isLoading || !markdownContent"
              @click="handleSave"
            >
              <FontAwesomeIcon
                v-if="isSaving"
                :icon="['fas', 'spinner']"
                spin
                class="mr-1"
                aria-hidden="true"
              />
              <FontAwesomeIcon
                v-else
                :icon="['fas', 'floppy-disk']"
                class="mr-1"
                aria-hidden="true"
              />
              {{ isSaving ? '保存中...' : '保存清洗结果' }}
            </button>
          </div>
        </div>
      </div>
    </div>
  </Teleport>
</template>

<style scoped>
textarea {
  scrollbar-width: thin;
}
</style>
