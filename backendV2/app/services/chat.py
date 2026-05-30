"""聊天对话服务"""

import json
import math
import uuid
from datetime import datetime
from decimal import Decimal

from fastapi import BackgroundTasks
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from sqlalchemy import select, func, text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.database import get_background_db_session, commit_or_rollback
from app.constants import chat as constants_chat
from app.constants import knowledge_base as constants_knowledge_base
from app.models import company_basic_info as models_company_basic_info
from app.models import chat_message as models_chat_message
from app.models import chat_session as models_chat_session
from app.schemas import chat as schemas_chat
from app.schemas import knowledge_base as schemas_knowledge_base
from app.schemas.common import ErrorCode, PaginatedResponse, PaginationInfo
from app.services import knowledge_base as services_knowledge_base
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model

# 配置日志记录器
logger = setup_logger(__name__)


def start_chat(
    db: Session,
    background_tasks: BackgroundTasks,
    start_chat_request: schemas_chat.StartChatRequest,
):
    """流式聊天，逐步推送 SSE 进度事件（支持多轮对话上下文）"""
    logger.info(
        f"开始聊天请求: session_id={start_chat_request.session_id} "
        f"question={start_chat_request.question}"
    )

    # === 会话加载/创建 ===
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

    # === 构建历史上下文 ===
    history_context_full, history_context_sql, history_context_intent = _build_history_context(db, chat_session)

    # === 步骤1: 意图识别 ===
    yield f"event: step\ndata: {json.dumps({'step': 'intent', 'message': '正在识别意图...'}, ensure_ascii=False)}\n\n"
    logger.info(f"意图识别开始: session_id={start_chat_request.session_id}")
    try:
        intent_result: dict = _identify_intent(
            db, start_chat_request, history_context_intent
        )
        intent_result_item = schemas_chat.IdentifyIntentResultItem.model_validate(
            intent_result
        )
        logger.info(
            f"意图识别完成: session_id={start_chat_request.session_id} "
            f"intent={intent_result_item.model_dump_json()}"
        )
        yield f"event: step\ndata: {json.dumps({'step': 'intent_done', 'message': '意图识别完成'}, ensure_ascii=False)}\n\n"
    except ServiceException as e:
        logger.error(
            f"意图识别失败: session_id={start_chat_request.session_id} error={e}"
        )
        yield f"event: error\ndata: {json.dumps({'code': e.code, 'message': e.message}, ensure_ascii=False)}\n\n"
        return

    # 根据意图决定是否执行SQL查询和RAG检索
    db_result = []
    rag_results = []

    need_sql = intent_result_item.need_sql_query
    need_rag = intent_result_item.need_rag_search
    logger.info(
        f"意图决策结果: session_id={start_chat_request.session_id} "
        f"need_sql={need_sql} need_rag={need_rag} "
        f"rag_companys={intent_result_item.rag_companys} "
        f"rag_industrys={intent_result_item.rag_industrys}"
    )

    if not need_sql and not need_rag:
        logger.info(
            f"纯文本对话（无需SQL和RAG）: session_id={start_chat_request.session_id}"
        )

    # === 步骤2: SQL查询（如需）===
    if intent_result_item.need_sql_query:
        yield f"event: step\ndata: {json.dumps({'step': 'sql', 'message': '正在生成查询语句...'}, ensure_ascii=False)}\n\n"
        logger.info(f"生成SQL语句开始: session_id={start_chat_request.session_id}")
        try:
            sql_statement: str = _generate_sql_statement(
                db, intent_result_item, history_context_sql
            )
            logger.info(
                f"生成SQL语句完成: session_id={start_chat_request.session_id} "
                f"sql={sql_statement}"
            )
            yield f"event: step\ndata: {json.dumps({'step': 'sql_done', 'message': '查询语句生成完成'}, ensure_ascii=False)}\n\n"
        except ServiceException as e:
            logger.error(
                f"生成SQL语句失败: session_id={start_chat_request.session_id} error={e}"
            )
            yield f"event: error\ndata: {json.dumps({'code': e.code, 'message': e.message}, ensure_ascii=False)}\n\n"
            return

        yield f"event: step\ndata: {json.dumps({'step': 'query', 'message': '正在查询数据...'}, ensure_ascii=False)}\n\n"
        logger.info(f"执行SQL语句开始: session_id={start_chat_request.session_id}")
        try:
            db_result = db.execute(text(sql_statement)).all()
            logger.info(
                f"执行SQL语句完成: session_id={start_chat_request.session_id} "
                f"result_count={len(db_result)}"
            )
            yield f"event: step\ndata: {json.dumps({'step': 'query_done', 'message': '数据查询完成'}, ensure_ascii=False)}\n\n"
        except Exception as e:
            logger.error(
                f"执行SQL语句失败: session_id={start_chat_request.session_id} error={e}"
            )
            yield f"event: error\ndata: {json.dumps({'code': ErrorCode.AI_SERVICE_ERROR, 'message': '执行SQL语句失败'}, ensure_ascii=False)}\n\n"
            return
    else:
        sql_statement = ""
        logger.info(
            f"跳过SQL查询（意图判定无需SQL）: session_id={start_chat_request.session_id}"
        )

    # === 步骤3: RAG检索（如需）===
    if need_rag:
        yield f"event: step\ndata: {json.dumps({'step': 'rag', 'message': '正在检索知识库...'}, ensure_ascii=False)}\n\n"
        logger.info(f"RAG检索开始: session_id={start_chat_request.session_id}")
        try:
            stock_codes = None
            if intent_result_item.rag_companys:
                stock_codes = [
                    c.get("stock_code")
                    for c in intent_result_item.rag_companys
                    if c.get("stock_code")
                ]
                logger.info(
                    f"RAG个股过滤: session_id={start_chat_request.session_id} stock_codes={stock_codes}"
                )
            if intent_result_item.rag_industrys:
                logger.info(
                    f"RAG行业过滤: session_id={start_chat_request.session_id} industry_names={intent_result_item.rag_industrys}"
                )
            search_request = schemas_knowledge_base.SearchKnowledgeRequest(
                query=start_chat_request.question,
                stock_codes=stock_codes or None,
                industry_names=intent_result_item.rag_industrys or None,
                top_k=10,
            )
            rag_response = services_knowledge_base.search_knowledge(db, search_request)
            rag_results = rag_response.results
            logger.info(
                f"RAG检索完成: session_id={start_chat_request.session_id} "
                f"results={len(rag_results)}"
            )
            if rag_results:
                logger.info(
                    f"RAG检索结果预览: session_id={start_chat_request.session_id} "
                    f"top_scores={[round(r.score, 3) for r in rag_results[:3]]} "
                    f"top_docs={[r.document_id for r in rag_results[:3]]}"
                )
            yield f"event: step\ndata: {json.dumps({'step': 'rag_done', 'message': f'知识库检索完成，找到{len(rag_results)}条相关内容'}, ensure_ascii=False)}\n\n"
        except Exception as e:
            logger.error(
                f"RAG检索失败: session_id={start_chat_request.session_id} error={e}",
                exc_info=True,
            )
            yield f"event: step\ndata: {json.dumps({'step': 'rag_done', 'message': '知识库检索失败，跳过'}, ensure_ascii=False)}\n\n"
    else:
        logger.info(
            f"跳过RAG检索（意图判定无需RAG）: session_id={start_chat_request.session_id}"
        )

    # === 步骤4: 生成回答 ===
    yield f"event: step\ndata: {json.dumps({'step': 'answer', 'message': '正在综合分析生成回答...'}, ensure_ascii=False)}\n\n"
    logger.info(
        f"生成回答开始: session_id={start_chat_request.session_id} "
        f"has_sql_result={bool(db_result)} sql_rows={len(db_result) if db_result else 0} "
        f"has_rag_result={bool(rag_results)} rag_count={len(rag_results) if rag_results else 0} "
        f"has_history={bool(history_context_full)}"
    )
    try:
        answer_parts = []
        for token in _generate_answer_stream(
            start_chat_request,
            intent_result_item,
            list(db_result) if db_result else [],
            history_context_full,
            rag_results=rag_results if rag_results else None,
        ):
            answer_parts.append(token)
            yield f"event: token\ndata: {json.dumps({'content': token}, ensure_ascii=False)}\n\n"
        answer = "".join(answer_parts)
        logger.info(f"聊天完成: session_id={start_chat_request.session_id}")
        yield f"event: step\ndata: {json.dumps({'step': 'answer_done', 'message': '回答生成完成'}, ensure_ascii=False)}\n\n"
    except ServiceException as e:
        logger.error(
            f"生成回答失败: session_id={start_chat_request.session_id} error={e}"
        )
        yield f"event: error\ndata: {json.dumps({'code': e.code, 'message': e.message}, ensure_ascii=False)}\n\n"
        return

    # === 持久化本轮消息 ===
    sql_result_for_storage = _truncate_sql_result(db_result) if db_result else None
    chat_message = models_chat_message.ChatMessage(
        session_id=start_chat_request.session_id,
        message_type="conversation",
        query=start_chat_request.question,
        intent_result=intent_result_item.model_dump(),
        sql_query=sql_statement or None,
        sql_result=sql_result_for_storage,
        rag_result=[r.model_dump() for r in rag_results] if rag_results else None,
        answer=answer,
        created_at=datetime.now(),
        answer_at=datetime.now(),
    )
    db.add(chat_message)
    db.flush()

    # 加锁重新加载会话，获取最新的 messages 列表（防止并发覆盖丢失更新）
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

    # === 触发后台摘要 ===
    background_tasks.add_task(_summarize_overflow, start_chat_request.session_id)

    # === 推送最终结果 ===
    result_data = {
        "session_id": start_chat_request.session_id,
        "answer": {"content": answer},
    }
    if sql_statement:
        result_data["sql"] = sql_statement
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

    message_items = []
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


def _identify_intent(
    db: Session,
    start_chat_request: schemas_chat.StartChatRequest,
    history_context: str = "",
):
    """意图识别"""
    logger.info(
        f"意图识别入口: session_id={start_chat_request.session_id} "
        f"question={start_chat_request.question}"
    )

    # 从数据库中获取所需数据
    try:
        industry_values = db.scalars(
            select(models_company_basic_info.CompanyBasicInfo.csrc_industry).distinct()
        ).all()
        company_list = db.execute(
            select(
                models_company_basic_info.CompanyBasicInfo.company_name,
                models_company_basic_info.CompanyBasicInfo.stock_code,
                models_company_basic_info.CompanyBasicInfo.stock_abbr,
            )
        ).all()
        company_list: str = "\n".join(
            [f"{item[1]} {item[2]} {item[0]}" for item in company_list]
        )
        system_prompt = PromptTemplate.from_template(
            settings.PROMPT_CONFIG.get_chat_config["intent_parse"]["system_prompt"]
        ).format(
            industry_values=industry_values,
            company_list=company_list,
        )
        if history_context:
            system_prompt += (
                f"\n\n=== 历史对话上下文（仅用于理解当前问题的指代关系）===\n"
                f"{history_context}\n"
                f"=== 历史对话上下文结束 ===\n"
                f"现在请根据当前用户问题输出JSON，只输出JSON，不要输出任何其他内容。"
            )
        user_prompt = PromptTemplate.from_template(
            settings.PROMPT_CONFIG.get_chat_config["intent_parse"][
                "user_prompt_template"
            ]
        ).format(
            question=start_chat_request.question,
        )
        prompt = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
        model = get_model.chat_model
        chain = model | JsonOutputParser()
        result: dict = chain.invoke(prompt)
        return result
    except Exception as e:
        logger.error(f"意图识别失败: {e}")
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, str(e)) from e


def _generate_sql_statement(
    db: Session,
    intent_result_item: schemas_chat.IdentifyIntentResultItem,
    history_context: str = "",
):
    """生成sql语句"""
    logger.info(f"生成SQL语句入口: intent={intent_result_item.model_dump_json()}")

    try:
        industry_values = db.scalars(
            select(models_company_basic_info.CompanyBasicInfo.csrc_industry).distinct()
        ).all()

        system_prompt = PromptTemplate.from_template(
            settings.PROMPT_CONFIG.get_chat_config["sql_generate"]["system_prompt"]
        ).format(
            industry_values=industry_values,
        )
        if history_context:
            system_prompt += f"\n\n历史对话的意图信息：\n{history_context}"

        user_prompt = PromptTemplate.from_template(
            settings.PROMPT_CONFIG.get_chat_config["sql_generate"][
                "user_prompt_template"
            ]
        ).format(
            intent_json=intent_result_item.model_dump_json(),
        )

        prompt = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
        model = get_model.chat_model
        chain = model | JsonOutputParser()
        result: dict = chain.invoke(prompt)
        return result.get("sql", "")
    except Exception as e:
        logger.error(f"生成sql语句失败: {e}")
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, str(e)) from e


def _generate_answer_stream(
    start_chat_request: schemas_chat.StartChatRequest,
    intent_result_item: schemas_chat.IdentifyIntentResultItem,
    query_result: list,
    history_context: str = "",
    rag_results: list | None = None,
):
    """流式生成回答，逐 token 返回"""
    logger.info(
        f"生成回答入口: intent={intent_result_item.model_dump_json()} "
        f"query_result={query_result} rag_results_count={len(rag_results) if rag_results else 0}"
    )
    try:
        system_prompt = settings.PROMPT_CONFIG.get_chat_config["answer_build"][
            "system_prompt"
        ]
        user_prompt = PromptTemplate.from_template(
            settings.PROMPT_CONFIG.get_chat_config["answer_build"][
                "user_prompt_template"
            ]
        ).format(
            question=start_chat_request.question,
            query_result=str(query_result),
            intent_json=intent_result_item.model_dump_json(),
        )

        # 拼接RAG检索结果
        if rag_results:
            rag_context = "\n\n=== 知识库检索结果 ===\n"
            for i, r in enumerate(rag_results, 1):
                rag_context += f"\n[{i}] (相关度:{r.score:.2f}) {r.chunk_text[:500]}"
            rag_context += "\n\n=== 知识库检索结果结束 ==="
            user_prompt += rag_context

        if history_context:
            user_prompt += f"\n\n历史对话上下文：\n{history_context}"
        prompt = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
        model = get_model.chat_model
        logger.info(f"开始流式生成回答")
        for chunk in model.stream(prompt):
            if chunk.content:
                yield chunk.content
    except Exception as e:
        logger.error(f"生成回答失败: {e}")
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, str(e)) from e


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

    # 按 chat_session.messages 列表顺序排列（保持语义时间线正确）
    messages = [msg_map[mid] for mid in chat_session.messages if mid in msg_map]

    full_parts = []
    sql_parts = []
    intent_parts = []
    for msg in messages:
        if msg.message_type == "summary":
            full_parts.append(f"[历史摘要] {msg.summary_content}")
            intent_parts.append(f"[历史摘要] {msg.summary_content}")
        elif msg.message_type == "conversation":
            conv_text = (
                f"用户: {msg.query}\n"
                f"意图: {msg.intent_result}\n"
                f"SQL: {msg.sql_query}\n"
                f"查询结果: {msg.sql_result}\n"
                f"回答: {msg.answer}"
            )
            full_parts.append(conv_text)
            sql_parts.append(conv_text)
            intent_parts.append(f"用户: {msg.query}\n回答: {msg.answer}")

    return "\n\n".join(full_parts), "\n\n".join(sql_parts), "\n\n".join(intent_parts)


def _truncate_sql_result(db_result):
    """截断SQL结果，最多保留100行"""
    MAX_ROWS = 100
    rows = list(db_result)
    rows = rows[:MAX_ROWS] if len(rows) > MAX_ROWS else rows
    result = []
    for row in rows:
        item = {}
        for key, value in dict(row._mapping).items():
            item[key] = float(value) if isinstance(value, Decimal) else value
        result.append(item)
    return result


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

        # 取列表最前 2 条（不限类型，对话和摘要都算最旧信息）
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

        # 用新摘要替换列表最前 2 条（构建新列表确保 SQLAlchemy 检测到变更）
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
    """调用 LLM 将对话历史压缩为摘要（支持 conversation 和 summary 两种输入类型）"""

    parts = []
    for m in conv_msgs:
        if m.message_type == "conversation":
            parts.append(f"问题: {m.query}\n回答: {m.answer}")
        elif m.message_type == "summary":
            parts.append(f"[历史摘要] {m.summary_content}")
    conversation_text = "\n\n".join(parts)

    prompt_config = settings.PROMPT_CONFIG.get_chat_config["summarize"]
    prompt = [
        SystemMessage(content=prompt_config["system_prompt"]),
        HumanMessage(
            content=prompt_config["user_prompt_template"].format(
                conversation_text=conversation_text
            )
        ),
    ]
    model = get_model.chat_model
    result = model.invoke(prompt)

    return result.content
