"""对话主流程：消息处理、意图解析、插槽检查、澄清、回答构建"""

import json
import os
import re
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session
from app.services import visualization as services_visualization

from app.constants import chat as constants_chat
from app.models import chat_message as models_chat_message
from app.models import chat_session as models_chat_session
from app.models.company_basic_info import CompanyBasicInfo
from app.schemas import chat as schemas_chat
from app.schemas.common import ErrorCode
from app.db.database import commit_or_rollback
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.core.config import settings
from app.services.chat.helpers import (
    build_schema_ddl_text,
    extract_sql_from_response,
    extract_topn_limit,
    generate_derived_metric_sql,
    invoke_llm,
    is_cross_table_topn_ratio_question,
    load_derived_metrics_config,
)
from app.services.chat.sql_builder import (
    _is_multi_metric_topn_intersection_question,
    generate_sql,
)
from app.services.chat.executor import (
    execute_query,
    validate_sql,
)

logger = setup_logger(__name__)


# ========== 公共入口函数 ==========


def process_chat_message(
    session_id: str | None,
    question: str,
    db: Session,
    question_id: str | None = None,
    chart_sequence: int = 1,
):
    """处理对话消息主流程"""
    if session_id is None:
        session_id = str(uuid.uuid4())
        chat_session = models_chat_session.ChatSession(
            id=session_id, status=0, context_slots={}
        )
        db.add(chat_session)
        db.flush()
        logger.info(f"创建新会话: session_id={session_id}")
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
        logger.info(f"指代消解: '{question}' -> '{resolved_question}'")

    intent = _parse_intent(resolved_question, context_slots, db)
    intent = _repair_intent_from_question(intent)

    intent = _merge_context(session_id, intent, context_slots)

    if intent.is_unsupported():
        unsupported_keyword = next(
            (
                kw
                for kw in constants_chat.UNSUPPORTED_KEYWORDS
                if kw in resolved_question
            ),
            None,
        )
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
        all_unsupported = [
            kw for kw in constants_chat.UNSUPPORTED_KEYWORDS if kw in resolved_question
        ]
        intent.unsupported_keywords = all_unsupported
        logger.info(f"部分支持场景，检测到不支持关键词: {all_unsupported}")

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

    sql = _normalize_sql_for_question(generate_sql(intent, db), intent)
    is_valid, validate_msg = validate_sql(sql)
    if not is_valid:
        logger.warning(f"SQL校验失败: {validate_msg}, sql={sql}")
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR, f"生成的SQL不安全: {validate_msg}"
        )

    query_result, result_companies = execute_query(sql, db)

    first_metric = intent.get_first_metric()
    metric_field = first_metric.get("field", "") if first_metric else ""
    is_prestored_derived = metric_field in constants_chat.PRESTORED_DERIVED_FIELDS
    if is_prestored_derived and intent.derived_metric_type:
        has_metric_values = _has_non_null_measure_values(query_result, metric_field)

        if not has_metric_values:
            logger.info(
                f"预存派生字段 {metric_field} 查询结果缺少有效指标值，fallback到动态计算"
            )
            template_sql = generate_derived_metric_sql(
                intent, intent.derived_metric_type
            )
            if template_sql:
                normalized_template_sql = _normalize_sql_for_question(
                    template_sql, intent
                )
                is_valid_fallback, validate_msg_fallback = validate_sql(
                    normalized_template_sql
                )
                if is_valid_fallback:
                    logger.info(
                        f"使用派生指标模板fallback SQL: {intent.derived_metric_type.value}"
                    )
                    sql = normalized_template_sql
                    query_result, result_companies = execute_query(sql, db)
            else:
                logger.info("派生指标模板返回None，让LLM重新生成SQL")
                fallback_intent = intent.model_dump()
                fallback_intent["derived_metric_type"] = (
                    intent.derived_metric_type.value
                    if intent.derived_metric_type
                    else "无"
                )
                config = settings.PROMPT_CONFIG.get_chat_config
                sql_config = config.get("sql_generate", {})
                schema_ddl = build_schema_ddl_text()
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
                response_text = invoke_llm(
                    system_prompt, user_prompt, max_tokens=2048, temperature=0.0
                )
                fallback_sql = extract_sql_from_response(response_text)
                if fallback_sql:
                    normalized_fallback_sql = _normalize_sql_for_question(
                        fallback_sql, intent
                    )
                    is_valid_fallback, validate_msg_fallback = validate_sql(
                        normalized_fallback_sql
                    )
                    if is_valid_fallback:
                        logger.info("LLM重新生成fallback SQL成功")
                        sql = normalized_fallback_sql
                        query_result, result_companies = execute_query(sql, db)

    chart_question_id = question_id or session_id[:8]
    chart_path, chart_type = services_visualization.generate_chart(
        data=query_result,
        intent=intent,
        question_id=chart_question_id,
        sequence=chart_sequence,
    )

    answer_text = _build_answer(question, query_result, intent)

    image_list = (
        [f"/api/v1/chat/images/{os.path.basename(chart_path)}"] if chart_path else []
    )

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
        logger.info(f"保存上一轮筛选结果公司集合: {len(result_companies)}家公司")

    chat_session.context_slots = context_slots_to_save
    commit_or_rollback(db)

    return schemas_chat.ChatResponse(
        session_id=session_id,
        answer=schemas_chat.AnswerContent(content=answer_text, image=image_list),
        need_clarification=False,
        sql=sql,
        chart_type=chart_type,
    )


"""辅助函数"""


def _detect_unsupported(question: str):
    """检测问题中是否包含不支持的关键词"""
    for keyword in constants_chat.UNSUPPORTED_METRIC_HINTS:
        if keyword in question:
            return keyword
    return None


def _is_aggregation_collection_question(question: str):
    """检测问题是否为聚合/集合类查询"""
    return any(kw in question for kw in constants_chat.AGGREGATION_RESULT_KEYWORDS)


def _classify_query_capability(
    question: str,
    metric: dict | None,
    derived_metric_type: schemas_chat.DerivedMetricType | None,
):
    """分类查询能力"""
    unsupported_keyword = _detect_unsupported(question)
    if unsupported_keyword:
        if metric is not None:
            return schemas_chat.QueryCapability.PARTIAL_SUPPORT
        return schemas_chat.QueryCapability.UNSUPPORTED

    if _is_aggregation_collection_question(question):
        return schemas_chat.QueryCapability.AGGREGATION

    if derived_metric_type:
        return schemas_chat.QueryCapability.DERIVED_METRIC

    if metric is None:
        return schemas_chat.QueryCapability.DIRECT_FIELD

    return schemas_chat.QueryCapability.DIRECT_FIELD


def _detect_derived_metric(question: str):
    """检测问题中是否包含派生指标关键词"""
    for metric_type, keywords in constants_chat.DERIVED_METRIC_KEYWORDS.items():
        for keyword in keywords:
            if keyword in question:
                return metric_type
    return None


def _build_schema_info_text():
    """构建表结构信息文本"""
    lines = []
    for table_name, table_info in constants_chat.SCHEMA_INFO.items():
        lines.append(f"\n表名: {table_name}")
        if "identity_fields" in table_info:
            lines.append(f"  身份字段: {', '.join(table_info['identity_fields'])}")
        if "metric_fields" in table_info:
            lines.append("  指标字段:")
            for field_name, field_desc in table_info["metric_fields"].items():
                lines.append(f"    {field_name}: {field_desc}")
        if "fields" in table_info:
            lines.append("  字段:")
            for field_name, field_desc in table_info["fields"].items():
                lines.append(f"    {field_name}: {field_desc}")
    return "\n".join(lines)


def _get_company_list(db: Session):
    """查询所有公司列表并格式化为文本"""
    stmt = select(
        CompanyBasicInfo.stock_code,
        CompanyBasicInfo.stock_abbr,
        CompanyBasicInfo.company_name,
    )
    results = db.execute(stmt).all()
    lines = []
    for row in results:
        lines.append(f"{row.stock_code} {row.stock_abbr} ({row.company_name})")
    return "\n".join(lines)


def _extract_json_from_response(response_text: str):
    """从 LLM 响应文本中提取 JSON 对象"""
    json_match = re.search(r"\{[\s\S]*\}", response_text)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        return None


def _references_collection_result(question: str):
    """检测问题是否引用了上一轮查询结果集"""
    self_contained_filter_keywords = [
        "为负数",
        "为负",
        "大于",
        "小于",
        "等于",
        "不低于",
        "不超过",
        "高于",
        "低于",
        "满足",
        "符合",
        "筛选",
        "条件",
    ]
    has_self_contained_filter = any(
        kw in question for kw in self_contained_filter_keywords
    )

    for pattern, _ in constants_chat.COLLECTION_COREFERENCE_PATTERNS:
        if re.search(pattern, question):
            if has_self_contained_filter and pattern in (
                r"这些公司",
                r"那些公司",
                r"这几家公司",
                r"那几家公司",
            ):
                return False
            return True

    return bool(
        re.search(r"这[0-9一二三四五六七八九十两几]+家(?:公司|企业|上市公司)", question)
        or re.search(
            r"前[0-9一二三四五六七八九十两]+家(?:公司|企业|上市公司)", question
        )
        or re.search(
            r"上述[0-9一二三四五六七八九十两几]*家(?:公司|企业|上市公司)", question
        )
    )


def _normalize_sql_for_question(sql: str, intent: schemas_chat.IntentResult):
    """根据问题场景对 SQL 做兼容性和业务层面的标准化"""
    question = intent.question or ""
    normalized_sql = _normalize_sql_for_mysql_compatibility(sql.strip())

    if not question or not any(
        keyword in question for keyword in constants_chat.TCM_CONTEST_UNIVERSE_KEYWORDS
    ):
        return normalized_sql

    original_sql = normalized_sql
    for pattern in constants_chat.TCM_CONTEST_INDUSTRY_SQL_PATTERNS:
        normalized_sql = re.sub(
            rf"(?i)\b(WHERE|AND)\s+{pattern}",
            lambda match: f"{match.group(1)} 1=1",
            normalized_sql,
        )

    if normalized_sql != original_sql:
        logger.info("已中和中药样本问题中的行业过滤条件")

    return normalized_sql


def _has_non_null_measure_values(
    rows: list[dict],
    metric_field: str = "",
):
    """检查查询结果行中是否存在非空度量值"""
    if not rows:
        return False

    for row in rows:
        if not isinstance(row, dict):
            continue

        if metric_field and row.get(metric_field) is not None:
            return True

        for column, value in row.items():
            column_name = str(column)
            if value is None:
                continue
            if column_name in constants_chat.RESULT_IDENTITY_COLUMNS:
                continue
            if any(column_name.startswith(prefix) for prefix in ("year_", "period_")):
                continue
            return True

    return False


def _detect_business_definition_needed(question: str):
    """检测问题中是否包含需要业务定义澄清的关键词"""
    config = load_derived_metrics_config()
    if not config:
        return None

    business_definitions = config.get("business_definitions", {})
    for def_key, def_info in business_definitions.items():
        keyword = def_info.get("keyword", "")
        aliases = def_info.get("keyword_aliases", [])
        all_keywords = [keyword] + aliases

        for kw in all_keywords:
            if kw in question:
                return {
                    "keyword": keyword,
                    "definition": def_info.get("definition", ""),
                    "clarification": def_info.get("clarification", ""),
                    "fallback_metric": def_info.get("fallback_metric"),
                    "unsupported": def_info.get("unsupported", False),
                }
    return None


def _handle_business_definition_clarification(
    question: str,
    intent: schemas_chat.IntentResult,
):
    """处理业务定义澄清，返回(是否需要澄清, 澄清问题或空字符串)"""
    business_def = _detect_business_definition_needed(question)
    if not business_def:
        return False, ""

    if business_def.get("unsupported"):
        return (
            True,
            f"抱歉，{business_def.get('clarification', '当前数据源不支持该查询')}",
        )

    if any(
        pattern in question
        for pattern in constants_chat.BUSINESS_DEFINITION_RESPONSE_PATTERNS
    ):
        fallback_metric = business_def.get("fallback_metric")
        current_metric = intent.get_first_metric()
        current_field = current_metric.get("field") if current_metric else None
        if fallback_metric and current_field == fallback_metric:
            return False, ""

    clarification = business_def.get("clarification", "")
    if clarification:
        return True, clarification

    return False, ""


def _normalize_time_range(time_range: dict | list | None):
    """将 LLM 返回的 time_range 统一为字典格式"""
    if time_range is None:
        return None

    if isinstance(time_range, dict):
        return time_range

    if isinstance(time_range, list) and len(time_range) > 0:
        years = []
        periods = []
        for item in time_range:
            if isinstance(item, dict):
                if item.get("report_year"):
                    years.append(item["report_year"])
                if item.get("report_period"):
                    periods.append(item["report_period"])

        if not years:
            return None

        result = {
            "report_year": years[0] if len(years) == 1 else years,
            "report_period": periods[0] if periods else "FY",
            "is_range": False,
        }
        logger.info(f"time_range数组格式已转换为字典: {result}")
        return result

    return None


def _get_metric_by_field(field_name: str | None):
    """根据字段名在别名映射表中查找指标定义"""
    if not field_name:
        return None

    for alias, metric in constants_chat.METRIC_ALIAS_MAP.items():
        if metric.get("field") == field_name:
            resolved_metric = dict(metric)
            resolved_metric["display_name"] = alias
            return resolved_metric
    return None


def _normalize_metric_payload(metric_data: Any):
    """将 LLM 返回的 metric 统一规范成 dict 或 list[dict]"""
    if metric_data is None:
        return None

    def _coerce_metric_item(item: Any) -> list[dict]:
        if not isinstance(item, dict):
            return []

        fields = item.get("field")
        if isinstance(fields, list):
            normalized_items: list[dict] = []
            raw_tables = item.get("table")
            raw_display_names = item.get("display_name")

            for idx, raw_field in enumerate(fields):
                if not isinstance(raw_field, str) or not raw_field:
                    continue

                resolved_metric = _get_metric_by_field(raw_field) or {"field": raw_field}

                if isinstance(raw_tables, list):
                    table_value = raw_tables[idx] if idx < len(raw_tables) else None
                    if isinstance(table_value, str) and table_value:
                        resolved_metric["table"] = table_value
                elif isinstance(raw_tables, str) and raw_tables:
                    resolved_metric.setdefault("table", raw_tables)

                if isinstance(raw_display_names, list):
                    display_name = (
                        raw_display_names[idx] if idx < len(raw_display_names) else None
                    )
                    if isinstance(display_name, str) and display_name:
                        resolved_metric["display_name"] = display_name

                normalized_items.append(resolved_metric)

            return normalized_items

        if isinstance(fields, str) and fields:
            return [item]

        return []

    if isinstance(metric_data, list):
        normalized_metrics: list[dict] = []
        seen_metric_keys: set[tuple[str, str]] = set()

        for item in metric_data:
            for normalized_item in _coerce_metric_item(item):
                metric_key = (
                    str(normalized_item.get("table", "")),
                    str(normalized_item.get("field", "")),
                )
                if metric_key in seen_metric_keys:
                    continue
                normalized_metrics.append(normalized_item)
                seen_metric_keys.add(metric_key)

        if not normalized_metrics:
            return None
        if len(normalized_metrics) == 1:
            return normalized_metrics[0]
        return normalized_metrics

    if isinstance(metric_data, dict):
        normalized_items = _coerce_metric_item(metric_data)
        if not normalized_items:
            return None
        if len(normalized_items) == 1:
            return normalized_items[0]
        return normalized_items

    return None


def _merge_metric_payload(
    current_metric: dict | list[dict] | None,
    inferred_metrics: list[dict],
):
    """合并当前指标与推断指标，去重后返回"""
    merged_metrics: list[dict] = []
    seen_metric_keys: set[tuple[str, str]] = set()

    def _append_metric(metric: dict) -> None:
        field = str(metric.get("field", ""))
        table = str(metric.get("table", ""))
        if not field or not table:
            return
        metric_key = (table, field)
        if metric_key in seen_metric_keys:
            return
        merged_metrics.append(metric)
        seen_metric_keys.add(metric_key)

    current_metric_list = []
    normalized_current = _normalize_metric_payload(current_metric)
    if isinstance(normalized_current, dict):
        current_metric_list = [normalized_current]
    elif isinstance(normalized_current, list):
        current_metric_list = normalized_current

    for metric in current_metric_list:
        if isinstance(metric, dict):
            _append_metric(metric)
    for metric in inferred_metrics:
        if isinstance(metric, dict):
            _append_metric(metric)

    if not merged_metrics:
        return None
    if len(merged_metrics) == 1:
        return merged_metrics[0]
    return merged_metrics


def _extract_metrics_from_question(question: str):
    """从问题文本中匹配所有已知指标别名"""
    matched_metrics: list[dict] = []
    seen_metric_keys: set[tuple[str, str]] = set()

    for alias, metric in sorted(
        constants_chat.METRIC_ALIAS_MAP.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        if alias not in question:
            continue

        metric_key = (metric.get("table", ""), metric.get("field", ""))
        if metric_key in seen_metric_keys:
            continue

        resolved_metric = dict(metric)
        resolved_metric["display_name"] = alias
        matched_metrics.append(resolved_metric)
        seen_metric_keys.add(metric_key)

    return matched_metrics


def _resolve_time_expression(time_text: str):
    """从时间文本中解析年份和报告期"""
    year_match = re.search(r"(\d{4})", time_text)
    year = int(year_match.group(1)) if year_match else None

    period = None
    for alias, code in constants_chat.PERIOD_ALIAS_MAP.items():
        if alias in time_text:
            period = code
            break

    is_range = any(
        kw in time_text for kw in ["近几年", "近三年", "近五年", "变化趋势", "趋势"]
    )

    if year is None and period is None:
        return None

    result = {"is_range": is_range}
    if year is not None:
        result["report_year"] = year
    if period is not None:
        result["report_period"] = period
    return result


def _infer_query_type_from_question(question: str):
    """从问题文本推断查询类型"""
    if any(
        keyword in question
        for keyword in ["对比", "比较", "相比", "差异", "变化", "增长率如何"]
    ):
        return schemas_chat.QueryType.COMPARISON
    if any(
        keyword in question for keyword in constants_chat.AGGREGATION_RESULT_KEYWORDS
    ):
        return schemas_chat.QueryType.RANKING
    if any(keyword in question for keyword in ["趋势", "近几年", "近三年", "近五年"]):
        return schemas_chat.QueryType.TREND
    return None


def _is_business_definition_followup(intent: schemas_chat.IntentResult):
    """检测是否为业务定义追问场景"""
    question = intent.question or ""
    return bool(
        _detect_business_definition_needed(question)
        and any(
            pattern in question
            for pattern in constants_chat.BUSINESS_DEFINITION_RESPONSE_PATTERNS
        )
        and intent.query_type == schemas_chat.QueryType.RANKING
        and intent.company is None
        and intent.metric is not None
    )


def _is_business_definition_response(question: str):
    """检测问题文本是否包含业务定义回答模式"""
    return any(
        pattern in question
        for pattern in constants_chat.BUSINESS_DEFINITION_RESPONSE_PATTERNS
    )


def _resolve_coreference(question: str, context_slots: dict):
    """消解指代语，将代词替换为上下文中的公司名"""
    if not context_slots:
        return question

    company_info = context_slots.get("company")
    last_result_companies = context_slots.get("last_result_companies")

    resolved = question

    if (
        last_result_companies
        and isinstance(last_result_companies, list)
        and len(last_result_companies) > 0
    ):
        values = [
            c.get("value", "")
            for c in last_result_companies
            if isinstance(c, dict) and c.get("value")
        ]
        if values:
            company_value = "、".join(values)
            for pattern, _ in constants_chat.COLLECTION_COREFERENCE_PATTERNS:
                if re.search(pattern, question):
                    resolved = re.sub(pattern, f"{company_value}中", resolved)
                    return resolved

    if company_info:
        company_value = None
        if isinstance(company_info, list):
            values = [
                c.get("value", "")
                for c in company_info
                if isinstance(c, dict) and c.get("value")
            ]
            company_value = "、".join(values) if values else None
        elif isinstance(company_info, dict):
            company_value = company_info.get("value", "")

        if company_value:
            for pattern in constants_chat.COREFERENCE_PATTERNS:
                resolved = re.sub(pattern, company_value, resolved)

    return resolved


def _merge_context(
    session_id: str, new_intent: schemas_chat.IntentResult, context_slots: dict
):
    """合并新一轮意图与上下文插槽，补全缺失字段"""
    if not context_slots:
        return new_intent

    def _is_valid_company(company):
        if company is None:
            return False
        if isinstance(company, list):
            return any(isinstance(c, dict) and c.get("value") for c in company)
        if isinstance(company, dict):
            return bool(company.get("value"))
        return False

    def _get_company_value(company):
        if company is None:
            return None
        if isinstance(company, list):
            values = [
                c.get("value")
                for c in company
                if isinstance(c, dict) and c.get("value")
            ]
            return "、".join(values) if values else None
        if isinstance(company, dict):
            return company.get("value")
        return None

    merged_company = new_intent.company
    if not _is_valid_company(merged_company):
        context_company = context_slots.get("company")
        if _is_valid_company(context_company):
            merged_company = context_company

    merged_metric = _normalize_metric_payload(
        new_intent.metric
    ) or _normalize_metric_payload(context_slots.get("metric"))
    merged_time_range = new_intent.time_range or context_slots.get("time_range")
    merged_ranking_time_range = new_intent.ranking_time_range or context_slots.get(
        "ranking_time_range"
    )
    merged_calculation_time_range = (
        new_intent.calculation_time_range or context_slots.get("calculation_time_range")
    )
    merged_query_type = new_intent.query_type

    if not merged_query_type and context_slots.get("query_type"):
        try:
            merged_query_type = schemas_chat.QueryType(context_slots["query_type"])
        except (ValueError, KeyError):
            pass

    def _metric_has_field(m):
        if isinstance(m, dict):
            return bool(m.get("field"))
        if isinstance(m, list) and m:
            first = m[0]
            return isinstance(first, dict) and bool(first.get("field"))
        return False

    if (
        merged_metric
        and not _metric_has_field(merged_metric)
        and context_slots.get("metric")
    ):
        merged_metric = context_slots["metric"]

    if merged_time_range and isinstance(merged_time_range, dict):
        if not merged_time_range.get("report_year") and context_slots.get("time_range"):
            merged_time_range = context_slots["time_range"]

    merged_capability = new_intent.capability
    if not merged_capability and context_slots.get("capability"):
        try:
            merged_capability = schemas_chat.QueryCapability(
                context_slots["capability"]
            )
        except (ValueError, KeyError):
            pass

    merged_derived_metric_type = new_intent.derived_metric_type
    if not merged_derived_metric_type and context_slots.get("derived_metric_type"):
        try:
            merged_derived_metric_type = schemas_chat.DerivedMetricType(
                context_slots["derived_metric_type"]
            )
        except (ValueError, KeyError):
            pass

    merged_last_result_companies = new_intent.last_result_companies
    if not merged_last_result_companies and context_slots.get("last_result_companies"):
        merged_last_result_companies = context_slots.get("last_result_companies")

    return schemas_chat.IntentResult(
        company=merged_company,
        metric=merged_metric,
        time_range=merged_time_range,
        ranking_time_range=merged_ranking_time_range,
        calculation_time_range=merged_calculation_time_range,
        query_type=merged_query_type,
        capability=merged_capability,
        derived_metric_type=merged_derived_metric_type,
        last_result_companies=merged_last_result_companies,
        confidence=new_intent.confidence,
        missing_slots=new_intent.missing_slots,
        question=new_intent.question,
    )


def _repair_intent_from_question(
    intent: schemas_chat.IntentResult,
):
    """根据问题文本修复意图中缺失的字段"""
    question = intent.question or ""
    if not question:
        return intent

    patched_intent = intent.model_copy(deep=True)

    if not patched_intent.time_range:
        inferred_time_range = _resolve_time_expression(question)
        if inferred_time_range:
            patched_intent.time_range = inferred_time_range

    if not patched_intent.metric:
        inferred_metrics = _extract_metrics_from_question(question)
        if len(inferred_metrics) == 1:
            patched_intent.metric = inferred_metrics[0]
        elif len(inferred_metrics) > 1:
            patched_intent.metric = inferred_metrics

    business_def = _detect_business_definition_needed(question)
    if (
        business_def
        and _is_business_definition_response(question)
        and not patched_intent.metric
    ):
        fallback_metric = _get_metric_by_field(business_def.get("fallback_metric"))
        if fallback_metric:
            patched_intent.metric = fallback_metric

    if patched_intent.query_type is None or (
        patched_intent.query_type == schemas_chat.QueryType.SINGLE_VALUE
        and any(kw in question for kw in constants_chat.AGGREGATION_RESULT_KEYWORDS)
    ):
        inferred_query_type = _infer_query_type_from_question(question)
        if inferred_query_type:
            patched_intent.query_type = inferred_query_type

    if any(kw in question for kw in constants_chat.AGGREGATION_RESULT_KEYWORDS):
        patched_intent.capability = schemas_chat.QueryCapability.AGGREGATION

    if not patched_intent.time_range and _is_business_definition_followup(
        patched_intent
    ):
        patched_intent.time_range = dict(constants_chat.DEFAULT_LATEST_TIME_RANGE)

    if (
        not patched_intent.time_range
        and any(
            keyword in question
            for keyword in constants_chat.TCM_CONTEST_UNIVERSE_KEYWORDS
        )
        and patched_intent.query_type != schemas_chat.QueryType.TREND
    ):
        patched_intent.time_range = dict(constants_chat.DEFAULT_LATEST_TIME_RANGE)

    if is_cross_table_topn_ratio_question(question):
        inferred_metrics = _extract_metrics_from_question(question)
        patched_intent.metric = _merge_metric_payload(
            patched_intent.metric,
            inferred_metrics,
        )
        patched_intent.query_type = schemas_chat.QueryType.RANKING
        patched_intent.capability = schemas_chat.QueryCapability.CROSS_TABLE
        patched_intent.derived_metric_type = schemas_chat.DerivedMetricType.RATIO

        ranking_time_range, calculation_time_range = (
            _infer_cross_table_topn_ratio_time_ranges(
                question,
                patched_intent.time_range,
            )
        )
        if ranking_time_range:
            patched_intent.ranking_time_range = ranking_time_range
            patched_intent.time_range = ranking_time_range
        if calculation_time_range:
            patched_intent.calculation_time_range = calculation_time_range

    return patched_intent


def _build_time_range(
    report_year: int | None = None,
    report_period: str | None = None,
    is_range: bool = False,
):
    """构造 time_range 字典"""
    if report_year is None and not report_period:
        return None

    result = {"is_range": is_range}
    if report_year is not None:
        result["report_year"] = report_year
    if report_period:
        result["report_period"] = report_period
    return result


def _extract_ordered_time_mentions(question: str):
    """按出现顺序提取问题中的年份-周期时间提及"""
    if not question:
        return []

    aliases = sorted(set(constants_chat.PERIOD_ALIAS_MAP.keys()), key=len, reverse=True)
    period_pattern = "|".join(re.escape(alias) for alias in aliases)
    time_pattern = re.compile(rf"(\d{{4}})\s*年\s*({period_pattern})")

    mentions: list[dict[str, Any]] = []
    for match in time_pattern.finditer(question):
        year = int(match.group(1))
        period_alias = match.group(2)
        period_code = constants_chat.PERIOD_ALIAS_MAP.get(period_alias)
        if not period_code:
            continue
        mentions.append(
            {
                "report_year": year,
                "report_period": period_code,
                "is_range": False,
            }
        )
    return mentions


def _format_time_range_label(time_range: dict | None):
    """将 time_range 字典格式化为可读的中文时间标签"""
    if not isinstance(time_range, dict):
        return "目标报告期"

    report_year = time_range.get("report_year")
    report_period = time_range.get("report_period")
    period_display_map = {
        "Q1": "第一季度",
        "HY": "半年度",
        "Q3": "第三季度",
        "FY": "年度",
    }

    if isinstance(report_year, int) and isinstance(report_period, str):
        return f"{report_year}年{period_display_map.get(report_period, report_period)}"
    if isinstance(report_year, int):
        return f"{report_year}年"
    return "目标报告期"


def _infer_cross_table_topn_ratio_time_ranges(
    question: str,
    fallback_time_range: dict | None,
):
    """推断跨表 TopN 占比查询的排名时间范围和计算时间范围"""
    ordered_mentions = _extract_ordered_time_mentions(question)
    ranking_time_range = (
        ordered_mentions[0] if ordered_mentions else fallback_time_range
    )

    aliases = sorted(set(constants_chat.PERIOD_ALIAS_MAP.keys()), key=len, reverse=True)
    period_pattern = "|".join(re.escape(alias) for alias in aliases)
    calculation_match = re.search(
        rf"(\d{{4}})年(?!\s*(?:{period_pattern}))(?:的)?[^，。；]*净利润",
        question,
    )
    calculation_time_range = None
    if calculation_match:
        calculation_time_range = _build_time_range(
            report_year=int(calculation_match.group(1)),
            report_period="FY",
            is_range=False,
        )

    if calculation_time_range is None:
        calculation_time_range = fallback_time_range

    return ranking_time_range, calculation_time_range


def _normalize_sql_for_mysql_compatibility(sql: str):
    """重写 SQL 中的 DISTINCT + ORDER BY TopN 子查询以兼容 MySQL"""
    normalized_sql = sql.strip()

    distinct_topn_pattern = re.compile(
        r"""
        SELECT\s+DISTINCT\s+(?P<selected_col>\w+)\s+
        FROM\s+(?P<table_name>\w+)\s*
        (?P<where_clause>[\s\S]*?)
        ORDER\s+BY\s+(?P<order_expr>[\w\.]+)\s+(?P<direction>ASC|DESC)\s+
        LIMIT\s+(?P<limit>\d+)
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    def _rewrite_distinct_topn(match: re.Match[str]) -> str:
        selected_col = match.group("selected_col")
        table_name = match.group("table_name")
        where_clause = match.group("where_clause").rstrip()
        order_expr = match.group("order_expr")
        direction = match.group("direction")
        limit = match.group("limit")

        if re.search(r"\bGROUP\s+BY\b", where_clause, re.IGNORECASE):
            return match.group(0)

        if order_expr.split(".")[-1].lower() == selected_col.lower():
            return match.group(0)

        rewritten_where_clause = f"{where_clause}\n        GROUP BY {selected_col}"
        return (
            f"SELECT {selected_col}\n"
            f"            FROM {table_name}{rewritten_where_clause}\n"
            f"            ORDER BY MAX({order_expr}) {direction}\n"
            f"            LIMIT {limit}"
        )

    rewritten_sql = distinct_topn_pattern.sub(_rewrite_distinct_topn, normalized_sql)
    if rewritten_sql != normalized_sql:
        logger.info("已重写 DISTINCT + ORDER BY TopN 子查询，兼容 MySQL 执行规则")

    return rewritten_sql


def _build_multi_metric_topn_intersection_answer(
    question: str,
    query_result: list[dict],
    intent: schemas_chat.IntentResult,
):
    """构建多指标 TopN 交集查询的 Markdown 表格回答"""
    if not _is_multi_metric_topn_intersection_question(intent) or not query_result:
        return None

    metrics = intent.get_metric_list()
    topn_limit = extract_topn_limit(question)
    if topn_limit is None:
        return None
    time_range = intent.time_range or {}
    report_year = time_range.get("report_year")
    report_period = time_range.get("report_period")
    period_display_map = {
        "Q1": "第一季度",
        "HY": "半年度",
        "Q3": "第三季度",
        "FY": "年度",
    }
    period_display = (
        f"{report_year}年{period_display_map.get(report_period, report_period)}"
        if report_year and report_period
        else "目标报告期"
    )

    headers = ["股票代码", "股票简称"] + [
        str(metric.get("display_name") or metric.get("field") or f"指标{index + 1}")
        for index, metric in enumerate(metrics)
    ]
    metric_fields = [str(metric.get("field") or "") for metric in metrics]

    def _format_cell(value: Any) -> str:
        if isinstance(value, float):
            return f"{value:,.2f}"
        if isinstance(value, int):
            return str(value)
        return str(value) if value is not None else "-"

    lines = [
        "## 分析结果",
        "",
        (
            f"根据查询结果，{period_display}同时满足所给指标均排名前{topn_limit}的公司"
            f"共有 **{len(query_result)} 家**。"
        ),
        "",
        "|" + "|".join(headers) + "|",
        "|" + "|".join(["---"] * len(headers)) + "|",
    ]

    for row in query_result:
        cells = [
            _format_cell(row.get("stock_code")),
            _format_cell(row.get("stock_abbr")),
        ] + [_format_cell(row.get(field)) for field in metric_fields]
        lines.append("| " + " | ".join(cells) + " |")

    lines.extend(
        [
            "",
            "### 说明",
            "",
            (
                f"- **均排名前{topn_limit}**表示先分别对各指标做排名，再取交集；"
                f"因此最终结果少于 {topn_limit} 家是正常情况，不代表查询不完整。"
            ),
            "- 表中金额单位按题目要求统一为万元。",
        ]
    )

    return "\n".join(lines)


def _build_cross_table_topn_ratio_answer(
    question: str,
    query_result: list[dict],
    intent: schemas_chat.IntentResult,
):
    """构建跨表 TopN 占比查询的 Markdown 表格回答"""
    if not is_cross_table_topn_ratio_question(intent.question or question):
        return None

    ranking_label = _format_time_range_label(
        intent.ranking_time_range or intent.time_range
    )
    calculation_label = _format_time_range_label(
        intent.calculation_time_range or intent.time_range
    )
    topn_limit = extract_topn_limit(question) or len(query_result)

    if not query_result:
        return (
            "## 分析结果\n\n"
            f"未检索到{ranking_label}未分配利润排名前{topn_limit}的公司数据，当前无法继续计算比例。"
        )

    valid_rows = [
        row
        for row in query_result
        if row.get("net_profit") is not None and row.get("ratio_percent") is not None
    ]

    def _format_cell(value: Any) -> str:
        if isinstance(value, float):
            return f"{value:,.2f}"
        if isinstance(value, int):
            return str(value)
        if value is None:
            return "-"
        return str(value)

    lines = ["## 分析结果", ""]
    if valid_rows:
        lines.append(
            f"{ranking_label}未分配利润排名前{topn_limit}的公司中，"
            f"共 **{len(valid_rows)} 家** 能够匹配到{calculation_label}净利润数据并计算占比。"
        )
    else:
        lines.append(
            f"{ranking_label}未分配利润排名前{topn_limit}的公司已识别，"
            f"但缺少{calculation_label}净利润数据，当前无法计算净利润占未分配利润比例。"
        )
    lines.extend(
        [
            "",
            "| 股票代码 | 股票简称 | 未分配利润（万元） | 净利润（万元） | 比例（%） |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )

    for row in query_result:
        lines.append(
            "| "
            + " | ".join(
                [
                    _format_cell(row.get("stock_code")),
                    _format_cell(row.get("stock_abbr")),
                    _format_cell(row.get("equity_unappropriated_profit")),
                    _format_cell(row.get("net_profit")),
                    _format_cell(row.get("ratio_percent")),
                ]
            )
            + " |"
        )

    if not valid_rows:
        lines.extend(
            [
                "",
                "### 说明",
                "",
                f"- 排名口径使用 {ranking_label} 的未分配利润。",
                f"- 计算口径需要 {calculation_label} 的净利润；当前该口径在结果中未匹配到有效数据。",
                "- 因此不能将查空误判为未分配利润缺失。",
            ]
        )

    return "\n".join(lines)


def _parse_intent(question: str, context_slots: dict, db: Session):
    """解析用户问题的意图"""
    config = settings.PROMPT_CONFIG.get_chat_config
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

    response_text = invoke_llm(
        system_prompt, user_prompt, max_tokens=32768, temperature=0.0
    )
    logger.info(f"意图解析结果: {response_text[:500]}")

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
                f"指标字段 {metric_field} 为数据库预存派生字段，将先查表再动态计算"
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
        logger.warning(f"意图解析结果构造失败: {exc}")
        derived_metric_type = _detect_derived_metric(question)
        capability = _classify_query_capability(question, None, derived_metric_type)
        return schemas_chat.IntentResult(
            capability=capability,
            derived_metric_type=derived_metric_type,
            confidence=0.0,
            missing_slots=["company", "metric", "time_range"],
            question=question,
        )


def _check_missing_slots(intent: schemas_chat.IntentResult):
    """检查槽位缺失情况，根据查询类型动态判断必需槽位"""
    missing = []
    question = intent.question or ""
    has_company = intent.company is not None and (
        (isinstance(intent.company, dict) and intent.company.get("value"))
        or (isinstance(intent.company, list) and len(intent.company) > 0)
    )
    has_result_companies = intent.has_last_result_companies()
    is_collection_reference = _references_collection_result(question)
    is_aggregation_question = any(
        kw in question for kw in constants_chat.AGGREGATION_RESULT_KEYWORDS
    )

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
            if not any(
                kw in (intent.question or "")
                for kw in [
                    "连续",
                    "均超过",
                    "均低于",
                    "均满足",
                    "都是",
                    "全部",
                    "每一",
                    "连续N个",
                    "连续N年",
                    "连续N季度",
                    "连续N期",
                ]
            ):
                missing.append("period_count")
    else:
        if not is_collection_query and not has_company:
            missing.append("company")
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range:
            missing.append("time_range")
    return missing


def _enrich_clarification(template: str, intent: schemas_chat.IntentResult):
    """用已知的槽位信息丰富澄清话术"""
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
):
    """根据缺失槽位生成澄清话术"""
    config = settings.PROMPT_CONFIG.get_chat_config
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
):
    """构建回答文本"""
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

    config = settings.PROMPT_CONFIG.get_chat_config
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

    response_text = invoke_llm(
        system_prompt, user_prompt, max_tokens=32768, temperature=0.3
    )
    logger.info(f"回答构建完成: length={len(response_text)}")
    return response_text
