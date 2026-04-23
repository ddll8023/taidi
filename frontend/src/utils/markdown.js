import { marked } from 'marked'
import DOMPurify from 'dompurify'

// Configure marked
marked.setOptions({
  breaks: true,
  gfm: true
})

/**
 * Render markdown content to sanitized HTML
 * @param {string} content - Raw markdown content
 * @returns {string} Sanitized HTML string
 */
export function renderMarkdown(content) {
  if (!content) return ''
  const rawHtml = marked.parse(content)
  return DOMPurify.sanitize(rawHtml)
}