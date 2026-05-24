/**
 * 智能问数 API
 * 功能描述：对话消息接口、会话列表查询
 */
import request from "./request";

const BASE_URL = import.meta.env.VITE_API_BASE_URL || "/api/v1";

/**
 * 发送对话消息（SSE 流式）
 * 通过回调逐步推送进度事件，最终返回完整结果
 * @param {Object} params - 请求参数
 * @param {string} [params.session_id] - 会话ID（新对话不传）
 * @param {string} params.question - 用户问题
 * @param {Function} onStep - 步骤回调(data: {step, message})
 * @param {Function} onToken - 流式 token 回调(data: {content})
 * @param {Function} onResult - 结果回调(data: {session_id, answer, sql})
 * @param {Function} onError - 错误回调(data: {code?, message})
 * @returns {Promise<void>}
 */
export function sendChatMessageStream(
  params,
  onStep,
  onToken,
  onResult,
  onError,
) {
  const token = window.localStorage.getItem("financial_reports_token");

  return fetch(`${BASE_URL}/chat`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(params),
  })
    .then(async (response) => {
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const parts = buffer.split("\n\n");
        buffer = parts.pop() || "";

        for (const part of parts) {
          const lines = part.split("\n");
          let eventType = "message";
          let dataStr = "";

          for (const line of lines) {
            if (line.startsWith("event: ")) {
              eventType = line.slice(7).trim();
            } else if (line.startsWith("data: ")) {
              dataStr = line.slice(6);
            }
          }

          if (!dataStr) continue;

          try {
            const data = JSON.parse(dataStr);

            if (eventType === "step" && onStep) {
              onStep(data);
            } else if (eventType === "token" && onToken) {
              onToken(data);
            } else if (eventType === "result" && onResult) {
              onResult(data);
            } else if (eventType === "error" && onError) {
              onError(data);
            }
          } catch (e) {
            console.error("SSE parse error:", e);
          }
        }
      }
    })
    .catch((error) => {
      if (onError) {
        onError({ code: -1, message: error.message || "请求失败，请稍后重试" });
      }
    });
}

/**
 * 获取聊天会话列表
 * @param {Object} params - 查询参数
 * @param {number} params.page - 页码
 * @param {number} params.page_size - 每页数量
 * @returns {Promise} { lists: [...], pagination: {...} }
 */
export function getChatList(params) {
  return request.post("/chat/list", params);
}

/**
 * 获取聊天会话详情（含历史消息）
 * @param {string} sessionId - 会话ID
 * @returns {Promise} { id, session_name, status, messages: [...], created_at, updated_at }
 */
export function getChatDetail(sessionId) {
  return request.post("/chat/detail", { session_id: sessionId });
}
