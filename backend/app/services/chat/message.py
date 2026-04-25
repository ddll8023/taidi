"""对话主流程：消息处理、意图解析、插槽检查、澄清、回答构建"""
import json
import uuid

from sqlalchemy.orm import Session

from app.constants import chat as constants_chat
from app.models import chat_message as models_chat_message
from app.models import chat_session as models_chat_session
from app.schemas import chat as schemas_chat
from app.schemas.common import ErrorCode
from app.db.database import commit_or_rollback
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.services.chat.context import (
    _classify_query_capability,
    _contains_continuity_keyword,
    _convert_path_to_url,
    _detect_all_unsupported_keywords,
    _detect_unsupported,
    _extract_json_from_response,
    _get_chat_config,
    _get_company_list,
    _handle_business_definition_clarification,
    _invoke_llm,
    _is_aggregation_collection_question,
    _merge_context,
    _references_collection_result,
    _repair_intent_from_question,
    _resolve_coreference,
    _build_schema_info_text,
    _build_schema_ddl_text,
)
from app.services.chat.metric import (
    _detect_derived_metric,
    _generate_derived_metric_sql,
    _has_non_null_measure_values,
    _normalize_metric_payload,
    _normalize_time_range,
)
from app.services.chat.sql_builder import (
    _extract_sql_from_response,
    _generate_sql,
    _normalize_sql_for_question,
)
from app.services.chat.executor import (
    _execute_query,
    _validate_sql,
)
from app.services.chat.query_handler import (
    _build_cross_table_topn_ratio_answer,
    _build_multi_metric_topn_intersection_answer,
)

logger = setup_logger(__name__)


"""辅助函数"""


def process_chat_message(
    session_id: str | None,
    question: str,
    db: Session,
    question_id: str | None = None,
    chart_sequence: int = 1,
) -> schemas_chat.ChatResponse:
    if session_id is None:
        session_id = str(uuid.uuid4())
        chat_session = models_chat_session.ChatSession(
            id=session_id, status=0, context_slots={}
        )
        db.add(chat_session)
        db.flush()
        logger.info("创建新会话: session_id=%s", session_id)
    else:
        chat_session = db.get(models_chat_session.ChatSession, session_id)
        if chat_session is None:
            raise ServiceException(
                ErrorCode.DATA_NOT_FOUND, f"会话不存在: {session_id}"
            )

    user_message = models_chat_message.ChatMessage(
        session_id=session_id,
        role="user",
        content=question,
    )
    db.add(user_message)
    db.flush()

    context_slots = chat_session.context_slots or {}

    resolved_question = _resolve_coreference(question, context_slots)
    if resolved_question != question:
        logger.info("指代消解: '%s' -> '%s'", question, resolved_question)

    intent = _parse_intent(resolved_question, context_slots, db)
    intent = _repair_intent_from_question(intent)

    intent = _merge_context(session_id, intent, context_slots)

    if intent.is_unsupported():
        unsupported_keyword = _detect_unsupported(resolved_question)
        hint = constants_chat.UNSUPPORTED_METRIC_HINTS.get(
            unsupported_keyword, "当前数据源不支持该查询"
        )
        unsupported_msg = f"抱歉，{hint}。请尝试其他问题或换一种表述方式。"
        assistant_message = models_chat_message.ChatMessage(
            session_id=session_id,
            role="assistant",
            content=unsupported_msg,
            intent_result=intent.model_dump(),
        )
        db.add(assistant_message)
        db.flush()

        chat_session.context_slots = intent.model_dump()
        commit_or_rollback(db)

        return schemas_chat.ChatResponse(
            session_id=session_id,
            answer=schemas_chat.AnswerContent(content=unsupported_msg),
            need_clarification=False,
            sql=None,
        )

    if intent.is_partial_support():
        all_unsupported = _detect_all_unsupported_keywords(resolved_question)
        intent.unsupported_keywords = all_unsupported
        logger.info("部分支持场景，检测到不支持关键词: %s", all_unsupported)

    need_business_clarification, business_clarification_msg = (
        _handle_business_definition_clarification(resolved_question, intent)
    )
    # 部分支持场景下，即使检测到unsupported关键词，如果有可执行的metric则跳过澄清
    if need_business_clarification and intent.is_partial_support() and intent.metric:
        logger.info("部分支持场景，跳过unsupported澄清，继续执行可支持的查询")
        need_business_clarification = False
        business_clarification_msg = ""

    if need_business_clarification:
        assistant_message = models_chat_message.ChatMessage(
            session_id=session_id,
            role="assistant",
            content=business_clarification_msg,
            intent_result=intent.model_dump(),
        )
        db.add(assistant_message)
        db.flush()

        chat_session.context_slots = intent.model_dump()
        commit_or_rollback(db)

        return schemas_chat.ChatResponse(
            session_id=session_id,
            answer=schemas_chat.AnswerContent(content=business_clarification_msg),
            need_clarification=True,
            sql=None,
        )

    missing_slots = _check_missing_slots(intent)
    if missing_slots:
        clarification = _generate_clarification(missing_slots, intent)
        assistant_message = models_chat_message.ChatMessage(
            session_id=session_id,
            role="assistant",
            content=clarification,
            intent_result=intent.model_dump(),
        )
        db.add(assistant_message)
        db.flush()

        chat_session.context_slots = intent.model_dump()
        commit_or_rollback(db)

        return schemas_chat.ChatResponse(
            session_id=session_id,
            answer=schemas_chat.AnswerContent(content=clarification),
            need_clarification=True,
            sql=None,
        )

    sql = _normalize_sql_for_question(_generate_sql(intent, db), intent)
    is_valid, validate_msg = _validate_sql(sql)
    if not is_valid:
        logger.warning("SQL校验失败: %s, sql=%s", validate_msg, sql)
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR, f"生成的SQL不安全: {validate_msg}"
        )

    query_result, result_companies = _execute_query(sql, db)

    first_metric = intent.get_first_metric()
    metric_field = first_metric.get("field", "") if first_metric else ""
    is_prestored_derived = metric_field in constants_chat.PRESTORED_DERIVED_FIELDS
    if is_prestored_derived and intent.derived_metric_type:
        has_metric_values = _has_non_null_measure_values(query_result, metric_field)

        if not has_metric_values:
            logger.info(
                "预存派生字段 %s 查询结果缺少有效指标值，fallback到动态计算",
                metric_field,
            )
            template_sql = _generate_derived_metric_sql(
                intent, intent.derived_metric_type
            )
            if template_sql:
                normalized_template_sql = _normalize_sql_for_question(
                    template_sql, intent
                )
                is_valid_fallback, validate_msg_fallback = _validate_sql(
                    normalized_template_sql
                )
                if is_valid_fallback:
                    logger.info(
                        "使用派生指标模板fallback SQL: %s",
                        intent.derived_metric_type.value,
                    )
                    sql = normalized_template_sql
                    query_result, result_companies = _execute_query(sql, db)
            else:
                logger.info("派生指标模板返回None，让LLM重新生成SQL")
                fallback_intent = intent.model_dump()
                fallback_intent["derived_metric_type"] = (
                    intent.derived_metric_type.value
                    if intent.derived_metric_type
                    else "无"
                )
                config = _get_chat_config()
                sql_config = config.get("sql_generate", {})
                schema_ddl = _build_schema_ddl_text()
                system_prompt = sql_config.get("system_prompt", "").replace(
                    "{schema_ddl}", schema_ddl
                )
                user_prompt = (
                    sql_config.get("user_prompt_template", "")
                    .replace(
                        "{intent_json}", json.dumps(fallback_intent, ensure_ascii=False)
                    )
                    .replace(
                        "{derived_metric_type}",
                        (
                            intent.derived_metric_type.value
                            if intent.derived_metric_type
                            else "无"
                        ),
                    )
                )
                response_text = _invoke_llm(
                    system_prompt, user_prompt, max_tokens=2048, temperature=0.0
                )
                fallback_sql = _extract_sql_from_response(response_text)
                if fallback_sql:
                    normalized_fallback_sql = _normalize_sql_for_question(
                        fallback_sql, intent
                    )
                    is_valid_fallback, validate_msg_fallback = _validate_sql(
                        normalized_fallback_sql
                    )
                    if is_valid_fallback:
                        logger.info("LLM重新生成fallback SQL成功")
                        sql = normalized_fallback_sql
                        query_result, result_companies = _execute_query(sql, db)

    from app.services import visualization as services_visualization

    chart_question_id = question_id or session_id[:8]
    chart_path, chart_type = services_visualization.generate_chart(
        data=query_result,
        intent=intent,
        question_id=chart_question_id,
        sequence=chart_sequence,
    )

    answer_text = _build_answer(question, query_result, intent)

    image_list = [_convert_path_to_url(chart_path)] if chart_path else []

    assistant_message = models_chat_message.ChatMessage(
        session_id=session_id,
        role="assistant",
        content=answer_text,
        intent_result=intent.model_dump(),
        sql_query=sql,
        chart_paths=image_list,
    )
    db.add(assistant_message)
    db.flush()

    context_slots_to_save = intent.model_dump()
    if result_companies and len(result_companies) > 1:
        context_slots_to_save["last_result_companies"] = result_companies
        logger.info("保存上一轮筛选结果公司集合: %d家公司", len(result_companies))

    chat_session.context_slots = context_slots_to_save
    commit_or_rollback(db)

    return schemas_chat.ChatResponse(
        session_id=session_id,
        answer=schemas_chat.AnswerContent(content=answer_text, image=image_list),
        need_clarification=False,
        sql=sql,
        chart_type=chart_type,
    )


def _parse_intent(
    question: str, context_slots: dict, db: Session
) -> schemas_chat.IntentResult:
    config = _get_chat_config()
    intent_config = config.get("intent_parse", {})

    schema_info = _build_schema_info_text()
    company_list = _get_company_list(db)

    current_company = context_slots.get("company", "无")
    if isinstance(current_company, list):
        current_company = "、".join(
            [c.get("value", "") for c in current_company if isinstance(c, dict)]
        )
    elif isinstance(current_company, dict):
        current_company = current_company.get("value", "无")
    current_metric = context_slots.get("metric", "无")
    current_time = context_slots.get("time_range", "无")

    system_prompt = (
        intent_config.get("system_prompt", "")
        .replace("{schema_info}", schema_info)
        .replace("{company_list}", company_list)
    )
    user_prompt = (
        intent_config.get("user_prompt_template", "")
        .replace("{question}", question)
        .replace("{current_company}", str(current_company))
        .replace("{current_metric}", str(current_metric))
        .replace("{current_time}", str(current_time))
    )

    response_text = _invoke_llm(
        system_prompt, user_prompt, max_tokens=32768, temperature=0.0
    )
    logger.info("意图解析结果: %s", response_text[:500])

    parsed = _extract_json_from_response(response_text)
    if parsed is None:
        logger.warning("意图解析返回非JSON，使用默认值")
        return schemas_chat.IntentResult(
            confidence=0.0, missing_slots=["company", "metric", "time_range"]
        )

    try:
        query_type = None
        if parsed.get("query_type"):
            try:
                query_type = schemas_chat.QueryType(parsed["query_type"])
            except ValueError:
                query_type = schemas_chat.QueryType.SINGLE_VALUE

        company_data = parsed.get("company")
        if company_data is not None:
            if isinstance(company_data, list):
                valid_companies = [
                    c for c in company_data if isinstance(c, dict) and c.get("value")
                ]
                if not valid_companies:
                    company_data = None
                elif len(valid_companies) == 1:
                    company_data = valid_companies[0]
                else:
                    company_data = valid_companies
            elif not isinstance(company_data, dict):
                company_data = None

        metric_data = _normalize_metric_payload(parsed.get("metric"))
        if isinstance(metric_data, list) and metric_data:
            first_metric_item = metric_data[0]
            metric_field = (
                first_metric_item.get("field", "")
                if isinstance(first_metric_item, dict)
                else ""
            )
            metric_table = (
                first_metric_item.get("table", "")
                if isinstance(first_metric_item, dict)
                else ""
            )
            has_component_fields = (
                first_metric_item.get("component_fields") is not None
                if isinstance(first_metric_item, dict)
                else False
            )
        elif isinstance(metric_data, dict):
            metric_field = (
                metric_data.get("field", "")
                if isinstance(metric_data.get("field"), str)
                else ""
            )
            metric_table = (
                metric_data.get("table", "")
                if isinstance(metric_data.get("table"), str)
                else ""
            )
            has_component_fields = metric_data.get("component_fields") is not None
        else:
            metric_field = ""
            metric_table = ""
            has_component_fields = False
        is_cross_table_query = "+" in metric_table or has_component_fields

        detected_derived_type = _detect_derived_metric(question)
        is_prestored_derived = (
            metric_field and metric_field in constants_chat.PRESTORED_DERIVED_FIELDS
        )

        if is_cross_table_query:
            derived_metric_type = None
            logger.info("检测到跨表查询需求，不使用派生指标模板，让LLM生成SQL")
        elif is_prestored_derived:
            derived_metric_type = detected_derived_type
            logger.info(
                "指标字段 %s 为数据库预存派生字段，将先查表再动态计算", metric_field
            )
        else:
            derived_metric_type = detected_derived_type
        capability = _classify_query_capability(
            question, metric_data, derived_metric_type
        )

        return schemas_chat.IntentResult(
            company=company_data,
            metric=metric_data,
            time_range=_normalize_time_range(parsed.get("time_range")),
            ranking_time_range=_normalize_time_range(parsed.get("ranking_time_range")),
            calculation_time_range=_normalize_time_range(
                parsed.get("calculation_time_range")
            ),
            query_type=query_type,
            capability=capability,
            derived_metric_type=derived_metric_type,
            confidence=float(parsed.get("confidence", 0.0)),
            missing_slots=[],
            question=question,
        )
    except Exception as exc:
        logger.warning("意图解析结果构造失败: %s", exc)
        derived_metric_type = _detect_derived_metric(question)
        capability = _classify_query_capability(question, None, derived_metric_type)
        return schemas_chat.IntentResult(
            capability=capability,
            derived_metric_type=derived_metric_type,
            confidence=0.0,
            missing_slots=["company", "metric", "time_range"],
            question=question,
        )


def _check_missing_slots(intent: schemas_chat.IntentResult) -> list[str]:
    """检查槽位缺失情况，根据查询类型动态判断必需槽位"""
    missing = []
    question = intent.question or ""
    has_company = intent.company is not None and (
        (isinstance(intent.company, dict) and intent.company.get("value"))
        or (isinstance(intent.company, list) and len(intent.company) > 0)
    )
    has_result_companies = intent.has_last_result_companies()
    is_collection_reference = _references_collection_result(question)
    is_aggregation_question = _is_aggregation_collection_question(question)

    if not has_company and has_result_companies and is_collection_reference:
        has_company = True

    if is_collection_reference and not has_result_companies and not has_company:
        return ["last_result_companies"]

    # 派生指标场景：不强制要求company，优先检查metric和time_range
    if intent.derived_metric_type:
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range:
            missing.append("time_range")
        return missing

    # 集合查询场景：ranking、comparison、聚合问题不要求company槽位
    is_collection_query = intent.query_type in [
        schemas_chat.QueryType.RANKING,
        schemas_chat.QueryType.COMPARISON,
    ]
    if is_aggregation_question:
        is_collection_query = True

    if intent.query_type == schemas_chat.QueryType.SINGLE_VALUE:
        if not is_collection_query and not has_company:
            missing.append("company")
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range:
            missing.append("time_range")
    elif intent.query_type == schemas_chat.QueryType.TREND:
        if not is_collection_query and not has_company:
            missing.append("company")
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range:
            missing.append("time_range_for_trend")
    elif intent.query_type == schemas_chat.QueryType.COMPARISON:
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range:
            missing.append("time_range")
    elif intent.query_type == schemas_chat.QueryType.RANKING:
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range:
            missing.append("time_range")
    elif intent.query_type == schemas_chat.QueryType.CONTINUITY:
        # 连续性查询需要：metric、time_range（或continuity_config中的时间范围）
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range and not intent.continuity_config:
            missing.append("time_range")
        continuity_cfg = intent.continuity_config or {}
        if not continuity_cfg.get("period_count"):
            # 如果LLM没有解析出连续期数，尝试从问题中提取
            if not _contains_continuity_keyword(intent.question or ""):
                missing.append("period_count")
    else:
        if not is_collection_query and not has_company:
            missing.append("company")
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range:
            missing.append("time_range")
    return missing


def _enrich_clarification(template: str, intent: schemas_chat.IntentResult) -> str:
    context_parts = []

    if intent.company is not None:
        if isinstance(intent.company, list):
            values = [
                c.get("value", "")
                for c in intent.company
                if isinstance(c, dict) and c.get("value")
            ]
            if values:
                context_parts.append(f"关于{'、'.join(values)}")
        elif isinstance(intent.company, dict) and intent.company.get("value"):
            context_parts.append(f"关于{intent.company['value']}")

    first_metric = intent.get_first_metric()
    if first_metric and first_metric.get("display_name"):
        context_parts.append(f"的{first_metric['display_name']}")
    if (
        intent.time_range
        and isinstance(intent.time_range, dict)
        and intent.time_range.get("report_year")
    ):
        context_parts.append(f"（{intent.time_range['report_year']}年）")

    if context_parts:
        return "".join(context_parts) + "，" + template

    return template


def _generate_clarification(
    missing_slots: list[str], intent: schemas_chat.IntentResult
) -> str:
    config = _get_chat_config()
    templates = config.get("clarification", {}).get("templates", {})

    if len(missing_slots) == 1:
        slot = missing_slots[0]
        template = templates.get(slot, "请提供更多信息。")
        if slot == "last_result_companies":
            return template
        return _enrich_clarification(template, intent)

    parts = []
    for slot in missing_slots:
        template = templates.get(slot, "请提供{slot}信息。")
        if slot == "last_result_companies":
            parts.append(template)
            continue
        parts.append(_enrich_clarification(template, intent))
    return " ".join(parts)


def _build_answer(
    question: str, query_result: list[dict], intent: schemas_chat.IntentResult
) -> str:
    structured_answer = _build_multi_metric_topn_intersection_answer(
        question, query_result, intent
    )
    if structured_answer:
        logger.info("使用多指标TopN交集模板构建回答")
        return structured_answer

    structured_answer = _build_cross_table_topn_ratio_answer(
        question, query_result, intent
    )
    if structured_answer:
        logger.info("使用跨表TopN占比模板构建回答")
        return structured_answer

    config = _get_chat_config()
    answer_config = config.get("answer_build", {})

    intent_json = json.dumps(intent.model_dump(), ensure_ascii=False)
    query_result_str = json.dumps(query_result, ensure_ascii=False, default=str)
    derived_metric_type_str = (
        intent.derived_metric_type.value if intent.derived_metric_type else "无"
    )

    # 处理部分支持场景：在user_prompt中添加说明
    partial_support_note = ""
    if intent.is_partial_support() and intent.unsupported_keywords:
        unsupported_list = "、".join(intent.unsupported_keywords)
        partial_support_note = (
            f"\n\n【重要提示】用户问题中包含数据库不支持的指标/内容：{unsupported_list}。"
            f"请在回答中明确说明这些指标无法查询，并返回其他可支持的指标查询结果。"
        )

    system_prompt = answer_config.get("system_prompt", "")
    user_prompt = (
        answer_config.get("user_prompt_template", "")
        .replace("{question}", question)
        .replace("{query_result}", query_result_str)
        .replace("{intent_json}", intent_json)
        .replace("{derived_metric_type}", derived_metric_type_str)
        + partial_support_note
    )

    response_text = _invoke_llm(
        system_prompt, user_prompt, max_tokens=32768, temperature=0.3
    )
    logger.info("回答构建完成: length=%d", len(response_text))
    return response_text


def _ensure_non_empty_qa_pairs(
    question_json_str: str,
    qa_pairs: list[dict],
) -> list[dict]:
    if qa_pairs:
        return qa_pairs

    try:
        rounds = json.loads(question_json_str)
    except (json.JSONDecodeError, TypeError):
        rounds = [{"Q": question_json_str}]

    first_question = ""
    if isinstance(rounds, list) and rounds:
        first_round = rounds[0]
        first_question = (
            first_round.get("Q", "")
            if isinstance(first_round, dict)
            else str(first_round)
        )

    fallback_question = first_question.strip() or str(question_json_str)
    return [
        {
            "Q": fallback_question,
            "A": {"content": "回答生成失败：未生成任何有效轮次结果，请重新执行该题。"},
        }
    ]
