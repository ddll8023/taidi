"""聊天对话服务"""

import json
import uuid

from fastapi import BackgroundTasks
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models import company_basic_info as models_company_basic_info
from app.schemas import chat as schemas_chat
from app.schemas.common import ErrorCode
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
    """流式聊天，逐步推送 SSE 进度事件"""
    logger.info(
        f"开始聊天请求: session_id={start_chat_request.session_id} "
        f"question={start_chat_request.question}"
    )

    if start_chat_request.session_id is None:
        start_chat_request.session_id = str(uuid.uuid4())
        logger.info(f"创建新会话: session_id={start_chat_request.session_id}")

        # 步骤1: 意图识别
        yield f"event: step\ndata: {json.dumps({'step': 'intent', 'message': '正在识别意图...'}, ensure_ascii=False)}\n\n"
        logger.info(f"意图识别开始: session_id={start_chat_request.session_id}")
        try:
            intent_result: dict = _identify_intent(db, start_chat_request)
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

        # 步骤2: 生成SQL
        yield f"event: step\ndata: {json.dumps({'step': 'sql', 'message': '正在生成查询语句...'}, ensure_ascii=False)}\n\n"
        logger.info(f"生成SQL语句开始: session_id={start_chat_request.session_id}")
        try:
            sql_statement: str = _generate_sql_statement(db, intent_result_item)
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

        # 步骤3: 执行SQL
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

        # 步骤4: 生成回答
        yield f"event: step\ndata: {json.dumps({'step': 'answer', 'message': '正在综合分析生成回答...'}, ensure_ascii=False)}\n\n"
        logger.info(f"生成回答开始: session_id={start_chat_request.session_id}")
        try:
            answer: str = _generate_answer(
                start_chat_request, intent_result_item, list(db_result)
            )
            logger.info(f"聊天完成: session_id={start_chat_request.session_id}")
            yield f"event: step\ndata: {json.dumps({'step': 'answer_done', 'message': '回答生成完成'}, ensure_ascii=False)}\n\n"
        except ServiceException as e:
            logger.error(
                f"生成回答失败: session_id={start_chat_request.session_id} error={e}"
            )
            yield f"event: error\ndata: {json.dumps({'code': e.code, 'message': e.message}, ensure_ascii=False)}\n\n"
            return

        # TODO 生成图表

        yield f"event: result\ndata: {json.dumps({'session_id': start_chat_request.session_id, 'answer': {'content': answer, 'image': None}, 'sql': sql_statement}, ensure_ascii=False)}\n\n"


"""辅助函数"""


def _identify_intent(
    db: Session,
    start_chat_request: schemas_chat.StartChatRequest,
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
        raise ServiceException(ErrorCode.DB_ERROR, str(e)) from e


def _generate_answer(
    start_chat_request: schemas_chat.StartChatRequest,
    intent_result_item: schemas_chat.IdentifyIntentResultItem,
    query_result: list,
):
    """生成回答"""
    logger.info(
        f"生成回答入口: intent={intent_result_item.model_dump_json()} "
        f"query_result={query_result}"
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
        prompt = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
        model = get_model.chat_model
        chain = model | JsonOutputParser()
        result: dict = chain.invoke(prompt)

        return result.get("answer", "")
    except Exception as e:
        logger.error(f"生成回答失败: {e}")
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, str(e)) from e
