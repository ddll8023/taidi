/**
 * 智能问数 API
 * 功能描述：对话消息接口
 */
import request from './request'

/**
 * 发送对话消息
 * @param {Object} params - 请求参数
 * @param {string} [params.session_id] - 会话ID（新对话不传）
 * @param {string} params.question - 用户问题（1~500字符）
 * @returns {Promise} { session_id, answer: { content, image }, sql, chart_type }
 */
export function sendChatMessage(params) {
  return request.post('/chat', params)
}
