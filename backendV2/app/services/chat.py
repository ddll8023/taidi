"""聊天对话服务（ReAct Agent 驱动）"""

import json
import math
import uuid
from datetime import datetime

from fastapi import BackgroundTasks
from langchain_core.messages import AIMessageChunk, HumanMessage, SystemMessage, ToolMessage
from langgraph.errors import GraphRecursionError
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.agent.chat_agent import build_chat_graph, format_system_prompt, extract_thinking_rounds_from_messages
from app.constants import chat as constants_chat
from app.db.database import get_background_db_session, commit_or_rollback
from app.models import chat_message as models_chat_message
from app.models import chat_session as models_chat_session
from app.schemas import chat as schemas_chat
from app.schemas.common import ErrorCode, PaginatedResponse, PaginationInfo
from app.states.chat_state import ChatAgentState
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model

logger = setup_logger(__name__)


def start_chat(
    db: Session,
    background_tasks: BackgroundTasks,
    start_chat_request: schemas_chat.StartChatRequest,
):
    """流式聊天（ReAct Agent 驱动），逐步推送 SSE 进度事件"""
    logger.info(
        f"开始聊天请求: session_id={start_chat_request.session_id} "
        f"question={start_chat_request.question}"
    )

    # ──── 会话加载/创建 ────

    if start_chat_request.session_id:
        chat_session = db.execute(
            select(models_chat_session.ChatSession).where(
                models_chat_session.ChatSession.id == start_chat_request.session_id
            )
        ).scalar_one_or_none()
        if not chat_session:
            logger.error(f"会话不存在: session_id={start_chat_request.session_id}")
            yield f"event: error\ndata: {json.dumps({'code': ErrorCode.DATA_NOT_FOUND, 'message': '会话不存在'}, ensure_ascii=False)}\n\n"
            return
        logger.info(f"加载已有会话: session_id={start_chat_request.session_id}")
    else:
        start_chat_request.session_id = str(uuid.uuid4())
        chat_session = models_chat_session.ChatSession(
            id=start_chat_request.session_id,
            session_name=start_chat_request.question[:100],
            status=0,
            messages=[],
        )
        db.add(chat_session)
        db.flush()
        logger.info(f"创建新会话: session_id={start_chat_request.session_id}")

    # ──── 构建初始状态 ────

    history_context_full, _, _ = _build_history_context(db, chat_session)
    system_content = format_system_prompt(db)

    initial_messages = [SystemMessage(content=system_content)]

    if history_context_full:
        initial_messages.append(
            SystemMessage(content=f"[历史对话上下文]\n{history_context_full}\n[/历史对话上下文]"
        ))

    initial_messages.append(HumanMessage(content=start_chat_request.question))

    initial_state: ChatAgentState = {
        "session_id": start_chat_request.session_id,
        "question": start_chat_request.question,
        "history_context": history_context_full,
        "messages": initial_messages,
        "reasoning_text": "",
        "tool_name": "",
        "tool_args": "",
        "tool_result": "",
        "answer_text": "",
        "sql_query": "",
        "iteration": 0,
        "error": "",
    }

    # ──── 执行 ReAct Agent ────

    graph = build_chat_graph()

    try:
        last_tool_iter = -1
        final_state = None
        content_buffer = []
        config = {"recursion_limit": 120}

        for event in graph.stream(initial_state, config=config, stream_mode=["messages", "values"]):
            # 多 mode 模式下，事件格式为 (mode_name, data)
            if not isinstance(event, tuple) or len(event) != 2:
                continue
            mode_name, data = event

            if mode_name == "messages":
                # 消息 chunk：(chunk, metadata) from LLM 流式输出
                if not isinstance(data, tuple) or len(data) != 2:
                    continue
                chunk, metadata = data
                node = metadata.get("langgraph_node", "")

                if node == "call_reasoner" and isinstance(chunk, AIMessageChunk):
                    # reasoning_content = 思考过程 → 前端卡片
                    reasoning = chunk.additional_kwargs.get("reasoning_content")
                    if reasoning:
                        yield f"event: reasoning_token\ndata: {json.dumps({'content': reasoning}, ensure_ascii=False)}\n\n"
                    # content 不在此处发射（无法提前区分是推理文本还是最终回答）

            elif mode_name == "values":
                # 状态快照
                state_snapshot = data
                iteration = state_snapshot.get("iteration", 0)

                # 跳过初始状态（iteration 尚未递增）
                if not iteration and not state_snapshot.get("tool_names"):
                    continue

                # 工具调用事件
                tool_names = state_snapshot.get("tool_names", [])
                if tool_names and iteration != last_tool_iter:
                    logger.info(
                        f"工具调用: session_id={start_chat_request.session_id} "
                        f"tools={tool_names}"
                    )
                    for name in tool_names:
                        yield f"event: tool_call\ndata: {json.dumps({'tool': name}, ensure_ascii=False)}\n\n"
                    last_tool_iter = iteration

                # 记录最终状态
                final_state = state_snapshot

        # ──── Agent 执行完毕，提取轮次和结果 ────

        if final_state:
            final_answer = final_state.get("answer_text", "") or ""
            final_sql = final_state.get("sql_query", "") or ""
            final_error = final_state.get("error", "") or ""
        else:
            final_answer = final_sql = final_error = ""

        answer = final_answer or "暂无回答"
        thinking_rounds = extract_thinking_rounds_from_messages(final_state) if final_state else []

        if final_error:
            logger.error(f"Agent 执行错误: session_id={start_chat_request.session_id} error={final_error}")

        logger.info(
            f"Agent 执行完成: session_id={start_chat_request.session_id} "
            f"has_answer={bool(final_answer)} has_sql={bool(final_sql)} "
            f"rounds={len(thinking_rounds)}"
        )

    except GraphRecursionError:
        logger.error(f"Agent 执行超限: session_id={start_chat_request.session_id}")
        _write_failure_message(start_chat_request.session_id, "[分析超限] Agent 循环次数达到上限")
        yield f"event: error\ndata: {json.dumps({'code': ErrorCode.AI_SERVICE_ERROR, 'message': '分析超限，请简化问题后重试'}, ensure_ascii=False)}\n\n"
        return

    except TimeoutError:
        logger.error(f"Agent 执行超时: session_id={start_chat_request.session_id}")
        _write_failure_message(start_chat_request.session_id, "[执行超时] Agent 执行超时")
        yield f"event: error\ndata: {json.dumps({'code': ErrorCode.AI_SERVICE_ERROR, 'message': '请求超时，请稍后重试'}, ensure_ascii=False)}\n\n"
        return

    except Exception as e:
        logger.error(f"Agent 执行异常: session_id={start_chat_request.session_id} error={e}", exc_info=True)
        _write_failure_message(start_chat_request.session_id, f"[执行异常] {str(e)[:200]}")
        yield f"event: error\ndata: {json.dumps({'code': ErrorCode.AI_SERVICE_ERROR, 'message': '服务调用失败，请稍后重试'}, ensure_ascii=False)}\n\n"
        return

    # ──── 持久化本轮消息 ────

    chat_message = models_chat_message.ChatMessage(
        session_id=start_chat_request.session_id,
        message_type="conversation",
        query=start_chat_request.question,
        intent_result={},
        sql_query=final_sql or None,
        sql_result=None,
        rag_result=None,
        answer=answer,
        created_at=datetime.now(),
        answer_at=datetime.now(),
    )
    db.add(chat_message)
    db.flush()

    # 加锁重新加载会话，获取最新的 messages 列表
    chat_session = db.execute(
        select(models_chat_session.ChatSession)
        .where(models_chat_session.ChatSession.id == start_chat_request.session_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    ).scalar_one()
    current_messages = chat_session.messages or []
    chat_session.messages = current_messages + [chat_message.id]
    commit_or_rollback(db)
    logger.info(
        f"消息已持久化: session_id={start_chat_request.session_id} "
        f"message_id={chat_message.id} window_size={len(chat_session.messages)}"
    )

    # ──── 触发后台摘要 ────

    background_tasks.add_task(_summarize_overflow, start_chat_request.session_id)

    # ──── 推送最终结果 ────

    result_data = {
        "session_id": start_chat_request.session_id,
        "answer": {"content": answer},
        "thinkingRounds": thinking_rounds,
    }
    if final_sql:
        result_data["sql"] = final_sql
    yield f"event: result\ndata: {json.dumps(result_data, ensure_ascii=False)}\n\n"


def get_chat_list(
    db: Session,
    get_chat_list_request: schemas_chat.GetChatListRequest,
):
    """获取聊天列表"""
    logger.info(
        f"查询聊天列表: page={get_chat_list_request.page} "
        f"page_size={get_chat_list_request.page_size}"
    )

    base_stmt = select(models_chat_session.ChatSession)

    total = db.scalar(select(func.count()).select_from(base_stmt.subquery()))

    session_entity_list = db.scalars(
        base_stmt.order_by(models_chat_session.ChatSession.updated_at.desc())
        .offset((get_chat_list_request.page - 1) * get_chat_list_request.page_size)
        .limit(get_chat_list_request.page_size)
    ).all()

    return PaginatedResponse[schemas_chat.GetChatListResponse](
        lists=[
            schemas_chat.GetChatListResponse.model_validate(item)
            for item in session_entity_list
        ],
        pagination=PaginationInfo(
            page=get_chat_list_request.page,
            page_size=get_chat_list_request.page_size,
            total=total,
            total_pages=(
                math.ceil(total / get_chat_list_request.page_size) if total else 0
            ),
        ),
    )


def get_chat_detail(
    db: Session,
    get_chat_detail_request: schemas_chat.GetChatDetailRequest,
):
    """获取聊天详情"""
    logger.info(f"获取聊天详情: session_id={get_chat_detail_request.session_id}")

    chat_session = db.execute(
        select(models_chat_session.ChatSession).where(
            models_chat_session.ChatSession.id == get_chat_detail_request.session_id
        )
    ).scalar_one_or_none()

    if not chat_session:
        logger.error(f"会话不存在: session_id={get_chat_detail_request.session_id}")
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "会话不存在")

    message_entities = (
        db.execute(
            select(models_chat_message.ChatMessage)
            .where(
                models_chat_message.ChatMessage.session_id
                == get_chat_detail_request.session_id,
                models_chat_message.ChatMessage.message_type == "conversation",
            )
            .order_by(models_chat_message.ChatMessage.id)
        )
        .scalars()
        .all()
    )
    message_items = [
        schemas_chat.ChatMessageItem.model_validate(m) for m in message_entities
    ]

    return schemas_chat.GetChatDetailResponse(
        id=chat_session.id,
        session_name=chat_session.session_name,
        status=chat_session.status,
        messages=message_items,
        created_at=chat_session.created_at,
        updated_at=chat_session.updated_at,
    )


def delete_chat_session(
    db: Session,
    delete_chat_session_request: schemas_chat.DeleteChatSessionRequest,
):
    """删除聊天会话"""
    logger.info(f"删除聊天会话: session_id={delete_chat_session_request.session_id}")

    chat_session_entity = db.get(
        models_chat_session.ChatSession, delete_chat_session_request.session_id
    )

    if not chat_session_entity:
        logger.error(f"会话不存在: session_id={delete_chat_session_request.session_id}")
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "会话不存在")

    db.delete(chat_session_entity)
    commit_or_rollback(db)

    logger.info(f"聊天会话已删除: session_id={delete_chat_session_request.session_id}")
    return schemas_chat.DeleteChatSessionResponse(
        session_id=delete_chat_session_request.session_id,
        deleted=True,
    )


"""辅助函数"""


def _write_failure_message(session_id: str, error_info: str):
    """写入失败记录，使用独立数据库会话（有痕失败）"""
    failure_db = get_background_db_session()
    try:
        failure_msg = models_chat_message.ChatMessage(
            session_id=session_id,
            message_type="conversation",
            query="",
            intent_result={},
            sql_query=None,
            sql_result=None,
            rag_result=None,
            answer=f"❌ {error_info}",
            created_at=datetime.now(),
            answer_at=datetime.now(),
        )
        failure_db.add(failure_msg)
        failure_db.flush()

        chat_session = failure_db.execute(
            select(models_chat_session.ChatSession)
            .where(models_chat_session.ChatSession.id == session_id)
            .with_for_update()
        ).scalar_one()
        current_messages = chat_session.messages or []
        chat_session.messages = current_messages + [failure_msg.id]
        commit_or_rollback(failure_db)
        logger.info(f"失败记录已写入: session_id={session_id} error={error_info}")
    except Exception as e:
        logger.error(f"写入失败记录异常: session_id={session_id} error={e}")
        failure_db.rollback()
    finally:
        failure_db.close()


def _build_history_context(db, chat_session):
    """从会话消息窗口构建历史上下文字符串，返回 (完整上下文, SQL专用上下文, 意图识别上下文)"""

    if not chat_session.messages:
        return "", "", ""

    msg_entities = (
        db.execute(
            select(models_chat_message.ChatMessage)
            .where(models_chat_message.ChatMessage.id.in_(chat_session.messages))
        )
        .scalars()
        .all()
    )
    msg_map = {m.id: m for m in msg_entities}

    messages = [msg_map[mid] for mid in chat_session.messages if mid in msg_map]

    full_parts = []
    for msg in messages:
        if msg.message_type == "summary":
            full_parts.append(f"[历史摘要] {msg.summary_content}")
        elif msg.message_type == "conversation":
            conv_text = (
                f"用户: {msg.query}\n"
                f"回答: {msg.answer}"
            )
            full_parts.append(conv_text)

    context = "\n\n".join(full_parts)
    return context, context, context


def _summarize_overflow(session_id: str):
    """后台任务：检查滑动窗口是否溢出，触发摘要压缩"""
    db = get_background_db_session()
    try:
        chat_session = db.scalar(
            select(models_chat_session.ChatSession)
            .where(models_chat_session.ChatSession.id == session_id)
            .with_for_update()
        )
        if not chat_session:
            return

        messages = chat_session.messages or []
        if len(messages) <= constants_chat.MAX_HISTORY_MESSAGES:
            return

        # 取列表最前 2 条
        head_ids = messages[:2]
        conv_msgs = db.scalars(
            select(models_chat_message.ChatMessage)
            .where(models_chat_message.ChatMessage.id.in_(head_ids))
        ).all()
        if len(conv_msgs) < 2:
            logger.info(f"列表头部不足 2 条有效消息，跳过压缩: session_id={session_id}")
            return

        logger.info(f"开始生成摘要: session_id={session_id}")
        summary_text = _generate_summary(conv_msgs)
        logger.info(f"摘要生成完成: session_id={session_id}")

        summary_msg = models_chat_message.ChatMessage(
            session_id=session_id,
            message_type="summary",
            summary_content=summary_text,
        )
        db.add(summary_msg)
        db.flush()

        chat_session.messages = [summary_msg.id] + messages[2:]
        commit_or_rollback(db)
        logger.info(
            f"滑动窗口已压缩: session_id={session_id} "
            f"old_size={len(messages)} new_size={len(chat_session.messages)}"
        )
    except Exception as e:
        logger.error(f"摘要生成失败: session_id={session_id} error={e}")
    finally:
        db.close()


def _generate_summary(conv_msgs):
    """调用 LLM 将对话历史压缩为摘要"""
    parts = []
    for m in conv_msgs:
        if m.message_type == "conversation":
            parts.append(f"问题: {m.query}\n回答: {m.answer}")
        elif m.message_type == "summary":
            parts.append(f"[历史摘要] {m.summary_content}")
    conversation_text = "\n\n".join(parts)

    from app.core.config import settings
    prompt_cfg = settings.PROMPT_CONFIG.get_chat_config["summarize"]
    prompt = [
        SystemMessage(content=prompt_cfg["system_prompt"]),
        HumanMessage(
            content=prompt_cfg["user_prompt_template"].format(
                conversation_text=conversation_text
            )
        ),
    ]
    model = get_model.chat_model
    result = model.invoke(prompt)

    return result.content
