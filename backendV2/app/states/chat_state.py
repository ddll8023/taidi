"""聊天 Agent 状态定义"""

from typing import Annotated, Sequence, TypedDict

from langchain_core.messages import BaseMessage
import operator


class ChatAgentState(TypedDict):
    """聊天 Agent State，用于 LangGraph 的 ReAct 循环

    字段说明：
        session_id: 会话ID
        question: 用户原始问题
        history_context: 历史对话上下文文本

        messages: LangChain 消息队列，按 ReAct 循环自动累积
        reasoning_text: 本轮 call_reasoner 输出的推理思考过程
        tool_name: 本轮调用的工具名（空字符串表示无工具调用）
        tool_args: 工具参数字符串
        tool_result: 工具执行结果

        answer_text: finalize_answer 输出的最终回答
        sql_query: 查询涉及的 SQL（供前端展示）

        iteration: 当前 ReAct 循环次数
        error: 错误信息
    """

    # 输入
    session_id: str
    question: str
    history_context: str

    # LangChain 消息队列（自动累积）
    messages: Annotated[Sequence[BaseMessage], operator.add]

    # 本轮推理与工具调用
    reasoning_text: str
    tool_name: str
    tool_args: str
    tool_result: str
    tool_names: list[str]

    # 最终答案
    answer_text: str
    sql_query: str

    # 控制
    iteration: int
    error: str
