"""上下文消解、LLM 调用与通用工具服务"""
import json
import os
import re

from langchain_core.messages import HumanMessage, SystemMessage
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.constants import chat as constants_chat
from app.core.config import settings
from app.models.company_basic_info import CompanyBasicInfo
from app.schemas import chat as schemas_chat
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model
from app.services.chat.metric import (
    _extract_metrics_from_question,
    _get_metric_by_field,
    _load_derived_metrics_config,
    _merge_metric_payload,
    _normalize_metric_payload,
)
from app.services.chat.query_handler import (
    _infer_cross_table_topn_ratio_time_ranges,
    _is_cross_table_topn_ratio_question,
)

logger = setup_logger(__name__)


"""辅助函数"""


def _convert_path_to_url(file_path: str) -> str:
    filename = os.path.basename(file_path)
    return f"/api/v1/chat/images/{filename}"


def _get_chat_config() -> dict:
    return settings.PROMPT_CONFIG.get_chat_config


def _build_schema_info_text() -> str:
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


def _build_schema_ddl_text() -> str:
    lines = []
    for table_name, table_info in constants_chat.SCHEMA_INFO.items():
        lines.append(f"CREATE TABLE {table_name} (")
        all_fields = {}
        if "identity_fields" in table_info:
            for f in table_info["identity_fields"]:
                all_fields[f] = "VARCHAR/INT"
        if "metric_fields" in table_info:
            for f, desc in table_info["metric_fields"].items():
                all_fields[f] = "DECIMAL"
        if "fields" in table_info:
            for f, desc in table_info["fields"].items():
                all_fields[f] = "VARCHAR"
        field_lines = [f"  {f} {t} -- {desc}" for f, t in all_fields.items()]
        lines.append(",\n".join(field_lines))
        lines.append(");")
        lines.append("")
    return "\n".join(lines)


def _get_company_list(db: Session) -> str:
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


def _invoke_llm(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 32768,
    temperature: float = 0.1,
) -> str:
    logger.info("调用LLM: prompt_chars=%d", len(system_prompt) + len(user_prompt))
    try:
        model = get_model.build_chat_model(
            max_tokens=max_tokens, temperature=temperature
        )
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
        response = model.invoke(messages)
    except Exception as exc:
        logger.error("LLM调用失败: error=%s", str(exc))
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "LLM调用失败") from exc

    content = getattr(response, "content", None)
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        text_parts = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                text_parts.append(str(block.get("text", "")))
        return "".join(text_parts).strip()
    return ""


def _extract_json_from_response(response_text: str) -> dict | None:
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


def _resolve_coreference(question: str, context_slots: dict) -> str:
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
) -> schemas_chat.IntentResult:
    if not context_slots:
        return new_intent

    def _is_valid_company(company: dict | list[dict] | None) -> bool:
        if company is None:
            return False
        if isinstance(company, list):
            return any(isinstance(c, dict) and c.get("value") for c in company)
        if isinstance(company, dict):
            return bool(company.get("value"))
        return False

    def _get_company_value(company: dict | list[dict] | None) -> str | None:
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


def _resolve_company(company_text: str, db: Session) -> dict | None:
    stmt = select(
        CompanyBasicInfo.stock_code,
        CompanyBasicInfo.stock_abbr,
        CompanyBasicInfo.company_name,
    )
    results = db.execute(stmt).all()

    for row in results:
        if company_text in (row.stock_code, row.stock_abbr, row.company_name):
            return {
                "value": row.stock_abbr,
                "type": "stock_abbr",
                "stock_code": row.stock_code,
            }

    for row in results:
        if company_text in row.company_name or company_text in row.stock_abbr:
            return {
                "value": row.stock_abbr,
                "type": "stock_abbr",
                "stock_code": row.stock_code,
            }

    return None


def _resolve_time_expression(time_text: str) -> dict | None:
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


def _resolve_metric(metric_text: str) -> dict | None:
    return constants_chat.METRIC_ALIAS_MAP.get(metric_text)


def _contains_continuity_keyword(question: str) -> bool:
    """检测问题中是否包含连续性关键词"""
    continuity_keywords = [
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
    for keyword in continuity_keywords:
        if keyword in question:
            return True
    return False


def _generate_continuity_sql(intent: schemas_chat.IntentResult) -> str | None:
    """生成连续性查询SQL，处理'连续N期满足某条件'的查询"""
    continuity_cfg = intent.continuity_config or {}
    period_count = continuity_cfg.get("period_count")
    condition = continuity_cfg.get("condition")
    start_period = continuity_cfg.get("start_period")
    end_period = continuity_cfg.get("end_period")

    first_metric = intent.get_first_metric()
    if not first_metric:
        return None

    metric_field = first_metric.get("field", "")
    table_name = first_metric.get("table", "core_performance_indicators_sheet")

    if not metric_field or not table_name:
        return None

    if not period_count:
        import re

        question = intent.question or ""
        match = re.search(r"连续(\d+)个", question)
        if match:
            period_count = int(match.group(1))
        else:
            match = re.search(r"连续(\d+)", question)
            if match:
                period_count = int(match.group(1))

    if not period_count:
        period_count = 4

    if not start_period and intent.time_range:
        start_period = intent.time_range

    if not end_period and intent.time_range:
        end_period = intent.time_range

    start_year = (
        start_period.get("report_year") if isinstance(start_period, dict) else None
    )
    start_period_val = (
        start_period.get("report_period") if isinstance(start_period, dict) else None
    )
    end_year = end_period.get("report_year") if isinstance(end_period, dict) else None
    end_period_val = (
        end_period.get("report_period") if isinstance(end_period, dict) else None
    )

    if not start_year:
        start_year = 2022
    if not end_year:
        end_year = 2025

    if not condition:
        condition = f"{metric_field} IS NOT NULL"

    period_order_case = """
        CASE report_period
            WHEN 'Q1' THEN 1
            WHEN 'HY' THEN 2
            WHEN 'Q3' THEN 3
            WHEN 'FY' THEN 4
        END
    """

    sql = f"""
WITH qualified_periods AS (
    SELECT
        stock_code,
        stock_abbr,
        report_year,
        report_period,
        {metric_field},
        ROW_NUMBER() OVER (
            PARTITION BY stock_code
            ORDER BY report_year, {period_order_case}
        ) as rn
    FROM {table_name}
    WHERE {condition}
      AND report_year BETWEEN {start_year} AND {end_year}
),
company_continuous_count AS (
    SELECT
        stock_code,
        stock_abbr,
        COUNT(*) as continuous_count
    FROM qualified_periods
    GROUP BY stock_code, stock_abbr
    HAVING COUNT(*) >= {period_count}
)
SELECT
    q.stock_code,
    q.stock_abbr,
    q.report_year,
    q.report_period,
    q.{metric_field}
FROM qualified_periods q
INNER JOIN company_continuous_count c
    ON q.stock_code = c.stock_code
ORDER BY q.stock_code, q.report_year, {period_order_case}
""".strip()

    return sql


def _detect_unsupported(question: str) -> str | None:
    """检测问题中是否包含不支持的数据源关键词（返回第一个匹配的关键词）"""
    for keyword in constants_chat.UNSUPPORTED_KEYWORDS:
        if keyword in question:
            return keyword
    return None


def _detect_all_unsupported_keywords(question: str) -> list[str]:
    """检测问题中所有不支持的数据源关键词（返回所有匹配的关键词列表）"""
    found = []
    for keyword in constants_chat.UNSUPPORTED_KEYWORDS:
        if keyword in question:
            found.append(keyword)
    return found


def _is_business_definition_response(question: str) -> bool:
    return any(
        pattern in question
        for pattern in constants_chat.BUSINESS_DEFINITION_RESPONSE_PATTERNS
    )


def _is_aggregation_collection_question(question: str) -> bool:
    return any(
        keyword in question for keyword in constants_chat.AGGREGATION_RESULT_KEYWORDS
    )


def _references_collection_result(question: str) -> bool:
    # 如果问题中包含自包含的筛选条件，则"这些公司/那些公司"指代的是当前筛选结果
    # 而非上一轮结果，此时不应视为对上一轮结果的引用
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
            # 如果包含自包含筛选条件，"这些公司"指代的是当前查询的筛选结果
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


def _infer_query_type_from_question(question: str) -> schemas_chat.QueryType | None:
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


def _is_business_definition_followup(intent: schemas_chat.IntentResult) -> bool:
    question = intent.question or ""
    return bool(
        _detect_business_definition_needed(question)
        and _is_business_definition_response(question)
        and intent.query_type == schemas_chat.QueryType.RANKING
        and intent.company is None
        and intent.metric is not None
    )


def _repair_intent_from_question(
    intent: schemas_chat.IntentResult,
) -> schemas_chat.IntentResult:
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
        and _is_aggregation_collection_question(question)
    ):
        inferred_query_type = _infer_query_type_from_question(question)
        if inferred_query_type:
            patched_intent.query_type = inferred_query_type

    if _is_aggregation_collection_question(question):
        patched_intent.capability = schemas_chat.QueryCapability.AGGREGATION

    if not patched_intent.time_range and _is_business_definition_followup(
        patched_intent
    ):
        patched_intent.time_range = dict(constants_chat.DEFAULT_LATEST_TIME_RANGE)

    if (
        not patched_intent.time_range
        and _is_tcm_contest_universe_question(question)
        and patched_intent.query_type != schemas_chat.QueryType.TREND
    ):
        patched_intent.time_range = dict(constants_chat.DEFAULT_LATEST_TIME_RANGE)

    if _is_cross_table_topn_ratio_question(question):
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


def _detect_business_definition_needed(question: str) -> dict | None:
    """检测问题中是否包含需要业务定义澄清的关键词"""
    config = _load_derived_metrics_config()
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
) -> tuple[bool, str]:
    """处理业务定义澄清，返回(是否需要澄清, 澄清问题或空字符串)"""
    business_def = _detect_business_definition_needed(question)
    if not business_def:
        return False, ""

    if business_def.get("unsupported"):
        return (
            True,
            f"抱歉，{business_def.get('clarification', '当前数据源不支持该查询')}",
        )

    if _is_business_definition_response(question):
        fallback_metric = business_def.get("fallback_metric")
        current_metric = intent.get_first_metric()
        current_field = current_metric.get("field") if current_metric else None
        if fallback_metric and current_field == fallback_metric:
            return False, ""

    clarification = business_def.get("clarification", "")
    if clarification:
        return True, clarification

    return False, ""


def _classify_query_capability(
    question: str,
    metric: dict | None,
    derived_metric_type: schemas_chat.DerivedMetricType | None,
) -> schemas_chat.QueryCapability:
    """分类查询能力"""
    unsupported_keyword = _detect_unsupported(question)
    if unsupported_keyword:
        # 有metric时为部分支持，无metric时为完全不支持
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


def _is_tcm_contest_universe_question(question: str) -> bool:
    return any(
        keyword in question for keyword in constants_chat.TCM_CONTEST_UNIVERSE_KEYWORDS
    )
