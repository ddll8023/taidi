"""聊天 Agent Graph 构建与编译"""

import json
import logging

from langchain_core.messages import AIMessage, SystemMessage, ToolMessage
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import ToolNode
from sqlalchemy.orm import Session

from app.agent.chat_prompts import REACT_SYSTEM_PROMPT_TEMPLATE
from app.agent.chat_tools import make_chat_tools
from app.models import company_basic_info as models_company_basic_info
from app.states.chat_state import ChatAgentState
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model
from sqlalchemy import select

logger = setup_logger(__name__)

# 最大 ReAct 循环次数
MAX_ITERATIONS = 30


def format_system_prompt(db: Session) -> str:
    """从数据库获取动态数据，格式化 System Prompt"""
    industry_values = db.scalars(
        select(models_company_basic_info.CompanyBasicInfo.csrc_industry).distinct()
    ).all()

    company_rows = db.execute(
        select(
            models_company_basic_info.CompanyBasicInfo.company_name,
            models_company_basic_info.CompanyBasicInfo.stock_code,
            models_company_basic_info.CompanyBasicInfo.stock_abbr,
        )
    ).all()
    company_list = "\n".join(
        [
            f"{item.stock_code} {item.stock_abbr} {item.company_name}"
            for item in company_rows
        ]
    )

    return REACT_SYSTEM_PROMPT_TEMPLATE.format(
        industry_values="、".join(industry_values) if industry_values else "（无数据）",
        company_list=company_list or "（无数据）",
    )


def extract_thinking_rounds_from_messages(state) -> list:
    """从 Agent state 的 messages 中提取推理轮次（含推理文本和工具名）

    规则：
    - AIMessage 有 content + tool_calls → 创建新轮次（含推理文本 + 工具）
    - AIMessage 仅有 tool_calls（无 content）→ 合并到上一个轮次的工具列表
    - AIMessage 仅有 content（无 tool_calls）→ 最终回答，跳过
    """
    if not state:
        return []
    messages = state.get("messages") or []
    rounds = []
    for msg in messages:
        if not isinstance(msg, AIMessage):
            continue
        has_tool_calls = bool(getattr(msg, "tool_calls", None))
        content = msg.content or ""

        # 优先使用 reasoning_content（ChatDeepSeek thinking 模式）
        reasoning = msg.additional_kwargs.get("reasoning_content", "") or ""
        thinking_text = reasoning or content

        if has_tool_calls:
            tools = []
            for tc in msg.tool_calls:
                name = tc.get("name", "") if isinstance(tc, dict) else tc.name
                if name and name not in tools:
                    tools.append(name)

            if thinking_text:
                rounds.append({"thinking": thinking_text, "tools": tools})
            else:
                if rounds:
                    for t in tools:
                        if t not in rounds[-1]["tools"]:
                            rounds[-1]["tools"].append(t)
                else:
                    rounds.append({"thinking": "", "tools": tools})

    return rounds


def build_chat_graph():
    """构建聊天 Agent 的 LangGraph StateGraph

    节点说明：
        call_reasoner:   LLM 推理节点，根据当前消息上下文决定是调用工具还是生成答案
        execute_tools:   执行 LLM 选择的工具（通过 prebuilt ToolNode）
        finalize_answer: 最终答案节点，将 call_reasoner 产生的答案标记为完成

    条件路由（should_continue）：
        continue  → 继续调用工具（execute_tools）
        finalize  → 生成最终答案（finalize_answer）
        end       → 结束（达到迭代上限或发生错误）
    """
    # 创建工具集（工具内部使用独立数据库会话）
    tools = make_chat_tools()
    tool_node = ToolNode(tools)

    # 绑定工具的 LLM
    model = get_model.chat_model.bind_tools(tools)

    # ──── 节点函数定义 ────

    def call_reasoner(state: ChatAgentState) -> dict:
        """LLM 推理节点：思考并决定下一步行动"""
        messages = state.get("messages", [])

        try:
            # 使用流式调用，逐 token 推入缓冲区
            content_chunks = []
            full_msg = None
            for chunk in model.stream(messages):
                if full_msg is None:
                    full_msg = chunk
                else:
                    full_msg += chunk
                if chunk.content:
                    content_chunks.append(chunk.content)

            full_content = "".join(content_chunks)
            has_tool_calls = bool(getattr(full_msg, "tool_calls", None))

            if has_tool_calls:
                response = AIMessage(
                    content=full_content, tool_calls=list(full_msg.tool_calls)
                )
            else:
                response = AIMessage(content=full_content)
        except Exception as e:
            logger.error(f"LLM 推理调用失败: {e}", exc_info=True)
            return {
                "messages": [AIMessage(content=f"LLM 调用失败: {e}")],
                "reasoning_text": "",
                "tool_name": "",
                "tool_args": "",
                "tool_result": "",
                "answer_text": "服务调用失败，请稍后重试",
                "tool_names": [],
                "error": str(e),
                "iteration": state.get("iteration", 0) + 1,
            }

        # 判断是否有工具调用
        has_tool_calls = bool(getattr(response, "tool_calls", None))

        result = {
            "messages": [response],
            "reasoning_text": response.content or "",
            "iteration": state.get("iteration", 0) + 1,
        }

        if has_tool_calls:
            tc = response.tool_calls[0]
            result["tool_name"] = tc["name"]
            result["tool_args"] = json.dumps(tc.get("args", {}), ensure_ascii=False)
            result["tool_result"] = ""
            result["answer_text"] = ""
            result["tool_names"] = list(dict.fromkeys(
                t["name"] if isinstance(t, dict) else t.name
                for t in response.tool_calls
            ))
        else:
            result["reasoning_text"] = ""
            result["tool_name"] = ""
            result["tool_args"] = ""
            result["tool_result"] = ""
            result["answer_text"] = response.content or ""
            result["tool_names"] = []

        return result

    def finalize_answer(state: ChatAgentState) -> dict:
        """最终回答节点：整理答案元数据"""
        sql_query = ""
        # 从工具调用结果中提取 SQL（如果有）
        messages = state.get("messages", [])
        for msg in reversed(messages):
            if isinstance(msg, ToolMessage) and msg.content:
                try:
                    parsed = json.loads(msg.content)
                    if isinstance(parsed, dict) and parsed.get("sql"):
                        sql_query = parsed["sql"]
                        break
                except (json.JSONDecodeError, TypeError):
                    continue
            if hasattr(msg, "tool_call_id"):
                continue

        return {"sql_query": sql_query}

    def should_continue(state: ChatAgentState) -> str:
        """条件路由：判断下一步"""
        # 检查错误
        if state.get("error"):
            return "end"

        # 检查迭代上限
        if state.get("iteration", 0) >= MAX_ITERATIONS:
            logger.warning(f"ReAct 循环达到上限 {MAX_ITERATIONS}")
            return "end"

        # 检查最后一条消息是否有工具调用
        messages = state.get("messages", [])
        if messages:
            last_msg = messages[-1]
            if getattr(last_msg, "tool_calls", None):
                return "continue"

        # 无工具调用，生成最终答案
        return "finalize"

    # ──── 构建图 ────

    graph = StateGraph(ChatAgentState)

    graph.add_node("call_reasoner", call_reasoner)
    graph.add_node("execute_tools", tool_node)
    graph.add_node("finalize_answer", finalize_answer)

    graph.set_entry_point("call_reasoner")

    graph.add_conditional_edges(
        "call_reasoner",
        should_continue,
        {
            "continue": "execute_tools",
            "finalize": "finalize_answer",
            "end": END,
        },
    )
    graph.add_edge("execute_tools", "call_reasoner")
    graph.add_edge("finalize_answer", END)

    return graph.compile()
