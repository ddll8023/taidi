/**
 * Markdown 渲染工具
 * 使用 marked 解析 Markdown，DOMPurify 防止 XSS
 */
import { marked } from 'marked'
import DOMPurify from 'dompurify'

// 配置 marked
marked.setOptions({
  breaks: true,
  gfm: true
})

/**
 * 将 Markdown 文本渲染为安全的 HTML
 * @param {string} text - Markdown 原始文本
 * @returns {string} 安全的 HTML 字符串
 */
export function renderMarkdown(text) {
  if (!text) return ''
  const rawHtml = marked.parse(text)
  return DOMPurify.sanitize(rawHtml)
}
