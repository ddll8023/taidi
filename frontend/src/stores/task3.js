import { ref, computed } from 'vue'
import { defineStore } from 'pinia'
import {
  getWorkspace as apiGetWorkspace,
  importFujian6 as apiImportFujian6,
  getQuestions as apiGetQuestions,
  getQuestionDetail as apiGetQuestionDetail,
  answerQuestion as apiAnswerQuestion,
  deleteAnswer as apiDeleteAnswer,
  rerunQuestion as apiRerunQuestion,
  batchAnswer as apiBatchAnswer,
  exportResult as apiExportResult,
  getLatestExport as apiGetLatestExport
} from '@/api/task3'

export const useTask3Store = defineStore('task3', () => {
  const workspace = ref(null)
  const questions = ref([])
  const currentQuestion = ref(null)
  const isLoading = ref(false)
  const isUploading = ref(false)
  const isProcessing = ref(false)
  const isExporting = ref(false)
  const error = ref(null)

  const showReferencesModal = ref(false)
  const aggregatedReferences = computed(() => {
    if (!currentQuestion.value?.answer_json) return []
    const seen = new Set()
    const result = []
    for (const qa of currentQuestion.value.answer_json) {
      for (const ref of (qa.A?.references || [])) {
        const key = `${ref.paper_path || ''}|${ref.text || ''}|${ref.paper_image || ''}`
        if (!seen.has(key)) {
          seen.add(key)
          result.push(ref)
        }
      }
    }
    return result
  })

  const currentPlanSummary = computed(() => currentQuestion.value?.execution_plan || null)
  const currentVerificationSummary = computed(() => currentQuestion.value?.verification || null)
  const currentRetrievalSummary = computed(() => currentQuestion.value?.retrieval_summary || null)

  async function loadWorkspace() {
    isLoading.value = true
    error.value = null
    try {
      const result = await apiGetWorkspace()
      workspace.value = result.data
      return result
    } catch (err) {
      error.value = err.message || '加载工作台失败'
    } finally {
      isLoading.value = false
    }
  }

  async function importFujian6(file) {
    isUploading.value = true
    error.value = null
    try {
      const result = await apiImportFujian6(file)
      workspace.value = {
        ...workspace.value,
        import_status: 2,
        source_file_name: result.data?.source_file_name,
        total_questions: result.data?.total_questions
      }
      await loadQuestions()
      return result
    } catch (err) {
      error.value = err.message || '导入失败'
      throw err
    } finally {
      isUploading.value = false
    }
  }

  async function loadQuestions(status = null) {
    isLoading.value = true
    error.value = null
    try {
      const params = status !== null ? { status } : {}
      const result = await apiGetQuestions(params)
      questions.value = result.data?.items || []
      return result
    } catch (err) {
      error.value = err.message || '加载题目列表失败'
    } finally {
      isLoading.value = false
    }
  }

  async function loadQuestionDetail(questionId) {
    isLoading.value = true
    error.value = null
    try {
      const result = await apiGetQuestionDetail(questionId)
      currentQuestion.value = result.data
      return result
    } catch (err) {
      error.value = err.message || '加载题目详情失败'
    } finally {
      isLoading.value = false
    }
  }

  async function answerQuestion(questionId) {
    isProcessing.value = true
    error.value = null
    try {
      const result = await apiAnswerQuestion(questionId)
      await loadQuestions()
      return result
    } catch (err) {
      error.value = err.message || '回答失败'
      throw err
    } finally {
      isProcessing.value = false
    }
  }

  async function deleteAnswer(questionId) {
    isProcessing.value = true
    error.value = null
    try {
      const result = await apiDeleteAnswer(questionId)
      await loadQuestions()
      return result
    } catch (err) {
      error.value = err.message || '删除失败'
      throw err
    } finally {
      isProcessing.value = false
    }
  }

  async function rerunQuestion(questionId) {
    isProcessing.value = true
    error.value = null
    try {
      const result = await apiRerunQuestion(questionId)
      await loadQuestions()
      return result
    } catch (err) {
      error.value = err.message || '重新回答失败'
      throw err
    } finally {
      isProcessing.value = false
    }
  }

  async function batchAnswer(scope = 'unfinished') {
    isProcessing.value = true
    error.value = null
    try {
      const result = await apiBatchAnswer(scope)
      await loadQuestions()
      return result
    } catch (err) {
      error.value = err.message || '批量回答失败'
      throw err
    } finally {
      isProcessing.value = false
    }
  }

  async function exportResult() {
    isExporting.value = true
    error.value = null
    try {
      const result = await apiExportResult()
      if (workspace.value) {
        workspace.value.last_export_path = result.data?.xlsx_path
      }
      return result
    } catch (err) {
      error.value = err.message || '导出失败'
      throw err
    } finally {
      isExporting.value = false
    }
  }

  async function getLatestExport() {
    try {
      const result = await apiGetLatestExport()
      return result
    } catch (err) {
      error.value = err.message || '获取导出信息失败'
    }
  }

  function openReferencesModal() {
    showReferencesModal.value = true
  }

  function closeReferencesModal() {
    showReferencesModal.value = false
  }

  function clearCurrentQuestion() {
    currentQuestion.value = null
  }

  return {
    workspace,
    questions,
    currentQuestion,
    isLoading,
    isUploading,
    isProcessing,
    isExporting,
    error,
    showReferencesModal,
    aggregatedReferences,
    currentPlanSummary,
    currentVerificationSummary,
    currentRetrievalSummary,
    loadWorkspace,
    importFujian6,
    loadQuestions,
    loadQuestionDetail,
    answerQuestion,
    deleteAnswer,
    rerunQuestion,
    batchAnswer,
    exportResult,
    getLatestExport,
    openReferencesModal,
    closeReferencesModal,
    clearCurrentQuestion
  }
})
