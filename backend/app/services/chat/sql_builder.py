"""SQL 生成与解析服务"""

import json
import re
from typing import Any

from sqlalchemy.orm import Session

from app.constants import chat as constants_chat
from app.schemas import chat as schemas_chat
from app.schemas.common import ErrorCode
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
)

logger = setup_logger(__name__)


# ========== 公共入口函数 ==========


def generate_sql(intent: schemas_chat.IntentResult, db: Session):
    """根据意图生成 SQL 查询语句"""
    first_metric = intent.get_first_metric()
    metric_field = first_metric.get("field", "") if first_metric else ""
    is_prestored_derived = metric_field in constants_chat.PRESTORED_DERIVED_FIELDS

    multi_metric_topn_sql = generate_multi_metric_topn_intersection_sql(intent)
    if multi_metric_topn_sql:
        logger.info("使用多指标TopN交集模板生成SQL")
        return multi_metric_topn_sql

    cross_table_topn_ratio_sql = generate_cross_table_topn_ratio_sql(intent)
    if cross_table_topn_ratio_sql:
        logger.info("使用跨表TopN占比模板生成SQL")
        return cross_table_topn_ratio_sql

    if intent.query_type == schemas_chat.QueryType.CONTINUITY:
        continuity_sql = generate_continuity_sql(intent)
        if continuity_sql:
            logger.info("使用连续性查询模板生成SQL")
            return continuity_sql

    if (
        intent.derived_metric_type
        and intent.is_derived_query()
        and not is_prestored_derived
        and intent.capability != schemas_chat.QueryCapability.AGGREGATION
    ):
        template_sql = generate_derived_metric_sql(intent, intent.derived_metric_type)
        if template_sql:
            logger.info(f"使用派生指标模板生成SQL: {intent.derived_metric_type.value}")
            return template_sql

    config = settings.PROMPT_CONFIG.get_chat_config
    sql_config = config.get("sql_generate", {})

    schema_ddl = build_schema_ddl_text()
    intent_json = json.dumps(intent.model_dump(), ensure_ascii=False)
    if is_prestored_derived:
        derived_metric_type_str = "无（预存字段，先查表）"
    else:
        derived_metric_type_str = (
            intent.derived_metric_type.value if intent.derived_metric_type else "无"
        )

    system_prompt = sql_config.get("system_prompt", "").replace(
        r"{schema_ddl}", schema_ddl
    )
    user_prompt = (
        sql_config.get("user_prompt_template", "")
        .replace(r"{intent_json}", intent_json)
        .replace(r"{derived_metric_type}", derived_metric_type_str)
    )

    response_text = invoke_llm(
        system_prompt, user_prompt, max_tokens=2048, temperature=0.0
    )
    logger.info(f"SQL生成结果: {response_text[:500]}")

    sql = extract_sql_from_response(response_text)
    if not sql:
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "LLM未能生成有效的SQL语句")
    return sql


"""辅助函数"""


def resolve_current_report_period(report_period: Any):
    """派生指标模板需要一个明确的当前期，列表场景默认取最后一个周期。"""
    if isinstance(report_period, list):
        valid_periods = [
            item for item in report_period if isinstance(item, str) and item
        ]
        if valid_periods:
            return valid_periods[-1]
        return "FY"

    if isinstance(report_period, str) and report_period:
        return report_period

    return "FY"


def _extract_comparison_time_points(time_range: dict | None):
    """提取 comparison 场景下的双时间点结构"""
    if not isinstance(time_range, dict):
        return []

    indexed_points: list[tuple[int, dict[str, Any]]] = []
    for key, value in time_range.items():
        match = re.fullmatch(r"report_year_(\d+)", str(key))
        if not match or not isinstance(value, int):
            continue

        index = int(match.group(1))
        period = time_range.get(f"report_period_{index}")
        if not isinstance(period, str) or not period:
            continue

        indexed_points.append(
            (
                index,
                {
                    "report_year": value,
                    "report_period": period,
                },
            )
        )

    indexed_points.sort(key=lambda item: item[0])
    if indexed_points:
        return [point for _, point in indexed_points]

    report_year = time_range.get("report_year")
    report_period = time_range.get("report_period")
    if (
        isinstance(report_year, list)
        and isinstance(report_period, str)
        and report_period
    ):
        normalized_years = sorted(year for year in report_year if isinstance(year, int))
        if len(normalized_years) == 2:
            return [
                {"report_year": normalized_years[0], "report_period": report_period},
                {"report_year": normalized_years[1], "report_period": report_period},
            ]

    return []


def _resolve_prestored_derived_metric_source(
    metric_field: str,
    table_name: str,
    derived_type: schemas_chat.DerivedMetricType,
):
    """将预存派生字段解析为原始源字段和源表"""
    field_mapping: dict[str, tuple[str, str]] = {}
    if derived_type == schemas_chat.DerivedMetricType.YOY_GROWTH:
        field_mapping = constants_chat.YOY_GROWTH_FIELD_MAPPING
    elif derived_type == schemas_chat.DerivedMetricType.QOQ_GROWTH:
        field_mapping = constants_chat.QOQ_GROWTH_FIELD_MAPPING

    source_metric = field_mapping.get(metric_field)
    if not source_metric:
        return metric_field, table_name

    source_field, source_table = source_metric
    return source_field, source_table or table_name


def _build_company_filter(intent: schemas_chat.IntentResult, table_alias: str = ""):
    """构建公司筛选条件"""
    companies = intent.get_company_list()
    if not companies:
        return "1=1"

    prefix = f"{table_alias}." if table_alias else ""
    conditions = []
    for company in companies:
        value = str(company.get("value", "")).replace("'", "''")
        company_type = company.get("type", "stock_abbr")
        if company_type == "stock_code":
            conditions.append(f"{prefix}stock_code = '{value}'")
        else:
            conditions.append(f"{prefix}stock_abbr = '{value}'")

    if len(conditions) == 1:
        return conditions[0]
    return "(" + " OR ".join(conditions) + ")"


def _generate_qoq_comparison_sql(
    intent: schemas_chat.IntentResult,
    metric_field: str,
    table_name: str,
    comparison_points: list[dict[str, Any]],
    period_sequence: dict[str, dict[str, Any]],
):
    """生成环比对比场景的 SQL"""
    if len(comparison_points) != 2:
        return None

    ordered_points = sorted(
        comparison_points,
        key=lambda point: (
            point.get("report_year", 0),
            str(point.get("report_period", "")),
        ),
    )
    earlier_point, later_point = ordered_points
    earlier_year = earlier_point.get("report_year")
    earlier_period = earlier_point.get("report_period")
    later_year = later_point.get("report_year")
    later_period = later_point.get("report_period")

    if not all(
        isinstance(value, int) for value in (earlier_year, later_year)
    ) or not all(
        isinstance(value, str) and value for value in (earlier_period, later_period)
    ):
        return None

    if not all(
        p in constants_chat.VALID_REPORT_PERIODS for p in (earlier_period, later_period)
    ):
        logger.warning(
            f"环比对比包含非法report_period: {earlier_period}, {later_period}"
        )
        return None

    earlier_period_info = period_sequence.get(
        earlier_period, {"prev_year_offset": 0, "prev_period": "Q1"}
    )
    later_period_info = period_sequence.get(
        later_period, {"prev_year_offset": 0, "prev_period": "Q1"}
    )

    earlier_suffix = str(earlier_year)
    later_suffix = str(later_year)
    earlier_company_filter = _build_company_filter(
        intent, table_alias=f"curr_{earlier_suffix}"
    )
    later_company_filter = _build_company_filter(
        intent, table_alias=f"curr_{later_suffix}"
    )

    earlier_expr = (
        f"ROUND((curr_{earlier_suffix}.{metric_field} - prev_{earlier_suffix}.{metric_field}) "
        f"/ ABS(prev_{earlier_suffix}.{metric_field}) * 100, 2)"
    )
    later_expr = (
        f"ROUND((curr_{later_suffix}.{metric_field} - prev_{later_suffix}.{metric_field}) "
        f"/ ABS(prev_{later_suffix}.{metric_field}) * 100, 2)"
    )

    return (
        "SELECT \n"
        f"  curr_{later_suffix}.stock_abbr,\n"
        f"  curr_{later_suffix}.report_year AS year_{later_suffix},\n"
        f"  curr_{later_suffix}.report_period AS period_{later_suffix},\n"
        f"  prev_{later_suffix}.{metric_field} AS prev_value_{later_suffix},\n"
        f"  curr_{later_suffix}.{metric_field} AS current_value_{later_suffix},\n"
        "  CASE \n"
        f"    WHEN prev_{later_suffix}.{metric_field} IS NULL OR prev_{later_suffix}.{metric_field} = 0 THEN NULL\n"
        f"    ELSE {later_expr}\n"
        f"  END AS qoq_growth_{later_suffix},\n"
        f"  curr_{earlier_suffix}.report_year AS year_{earlier_suffix},\n"
        f"  curr_{earlier_suffix}.report_period AS period_{earlier_suffix},\n"
        f"  prev_{earlier_suffix}.{metric_field} AS prev_value_{earlier_suffix},\n"
        f"  curr_{earlier_suffix}.{metric_field} AS current_value_{earlier_suffix},\n"
        "  CASE \n"
        f"    WHEN prev_{earlier_suffix}.{metric_field} IS NULL OR prev_{earlier_suffix}.{metric_field} = 0 THEN NULL\n"
        f"    ELSE {earlier_expr}\n"
        f"  END AS qoq_growth_{earlier_suffix},\n"
        "  CASE \n"
        f"    WHEN prev_{later_suffix}.{metric_field} IS NULL OR prev_{later_suffix}.{metric_field} = 0\n"
        f"      OR prev_{earlier_suffix}.{metric_field} IS NULL OR prev_{earlier_suffix}.{metric_field} = 0 THEN NULL\n"
        f"    ELSE ROUND({later_expr} - {earlier_expr}, 2)\n"
        "  END AS qoq_growth_change\n"
        f"FROM {table_name} curr_{later_suffix}\n"
        f"LEFT JOIN {table_name} prev_{later_suffix} \n"
        f"  ON curr_{later_suffix}.stock_code = prev_{later_suffix}.stock_code \n"
        f"  AND prev_{later_suffix}.report_year = curr_{later_suffix}.report_year + {later_period_info.get('prev_year_offset', 0)}\n"
        f"  AND prev_{later_suffix}.report_period = '{later_period_info.get('prev_period', 'Q1')}'\n"
        f"  AND prev_{later_suffix}.report_type = 'REPORT'\n"
        f"JOIN {table_name} curr_{earlier_suffix} \n"
        f"  ON curr_{later_suffix}.stock_code = curr_{earlier_suffix}.stock_code\n"
        f"LEFT JOIN {table_name} prev_{earlier_suffix} \n"
        f"  ON curr_{earlier_suffix}.stock_code = prev_{earlier_suffix}.stock_code \n"
        f"  AND prev_{earlier_suffix}.report_year = curr_{earlier_suffix}.report_year + {earlier_period_info.get('prev_year_offset', 0)}\n"
        f"  AND prev_{earlier_suffix}.report_period = '{earlier_period_info.get('prev_period', 'Q1')}'\n"
        f"  AND prev_{earlier_suffix}.report_type = 'REPORT'\n"
        f"WHERE {later_company_filter}\n"
        f"  AND curr_{later_suffix}.report_year = {later_year}\n"
        f"  AND curr_{later_suffix}.report_period = '{later_period}'\n"
        f"  AND curr_{later_suffix}.report_type = 'REPORT'\n"
        f"  AND {earlier_company_filter}\n"
        f"  AND curr_{earlier_suffix}.report_year = {earlier_year}\n"
        f"  AND curr_{earlier_suffix}.report_period = '{earlier_period}'\n"
        f"  AND curr_{earlier_suffix}.report_type = 'REPORT'\n"
        f"ORDER BY curr_{later_suffix}.stock_abbr"
    )


def generate_continuity_sql(intent: schemas_chat.IntentResult):
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
    end_year = end_period.get("report_year") if isinstance(end_period, dict) else None

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


def _is_multi_metric_topn_intersection_question(
    intent: schemas_chat.IntentResult,
):
    """判断是否为多指标 TopN 交集查询"""
    question = intent.question or ""
    metrics = intent.get_metric_list()
    if intent.query_type != schemas_chat.QueryType.RANKING or len(metrics) < 2:
        return False

    return "均排名前" in question and extract_topn_limit(question) is not None


def generate_multi_metric_topn_intersection_sql(
    intent: schemas_chat.IntentResult,
):
    """生成多指标 TopN 交集查询的 SQL"""
    if not _is_multi_metric_topn_intersection_question(intent):
        return None

    time_range = intent.time_range or {}
    report_year = time_range.get("report_year")
    report_period = time_range.get("report_period")
    if not isinstance(report_year, int) or not isinstance(report_period, str):
        return None

    metrics = intent.get_metric_list()
    normalized_metrics: list[dict[str, str]] = []
    seen_fields: set[tuple[str, str]] = set()
    for metric in metrics:
        field = metric.get("field")
        table = metric.get("table")
        if not isinstance(field, str) or not isinstance(table, str):
            return None
        metric_key = (table, field)
        if metric_key in seen_fields:
            continue
        seen_fields.add(metric_key)
        normalized_metrics.append(
            {
                "field": field,
                "table": table,
                "display_name": str(metric.get("display_name") or field),
            }
        )

    if len(normalized_metrics) < 2:
        return None

    base_table = normalized_metrics[0]["table"]
    if any(metric["table"] != base_table for metric in normalized_metrics):
        return None

    limit = extract_topn_limit(intent.question or "")
    if limit is None:
        return None

    table_alias = "t"
    select_columns = [
        f"{table_alias}.stock_code",
        f"{table_alias}.stock_abbr",
    ]
    rank_columns: list[str] = []
    filters: list[str] = []
    order_columns: list[str] = []

    for index, metric in enumerate(normalized_metrics, start=1):
        field = metric["field"]
        select_columns.append(f"{table_alias}.{field}")
        rank_alias = f"metric_rank_{index}"
        rank_columns.append(
            f"RANK() OVER (ORDER BY {table_alias}.{field} DESC) AS {rank_alias}"
        )
        filters.append(f"ranked.{rank_alias} <= {limit}")
        order_columns.append(f"ranked.{rank_alias}")

    inner_select = ",\n            ".join(select_columns + rank_columns)
    outer_select = ",\n    ".join(
        ["ranked.stock_code", "ranked.stock_abbr"]
        + [f"ranked.{metric['field']}" for metric in normalized_metrics]
    )

    return (
        "WITH ranked AS (\n"
        "    SELECT\n"
        f"            {inner_select}\n"
        f"    FROM {base_table} {table_alias}\n"
        f"    WHERE {table_alias}.report_year = {report_year}\n"
        f"      AND {table_alias}.report_period = '{report_period}'\n"
        ")\n"
        "SELECT\n"
        f"    {outer_select}\n"
        "FROM ranked\n"
        f"WHERE {' AND '.join(filters)}\n"
        f"ORDER BY {', '.join(order_columns)}"
    )


def generate_cross_table_topn_ratio_sql(
    intent: schemas_chat.IntentResult,
):
    """生成跨表 TopN 占比查询的 SQL"""
    question = intent.question or ""
    if not is_cross_table_topn_ratio_question(question):
        return None

    topn_limit = extract_topn_limit(question)
    if topn_limit is None:
        return None

    ranking_time_range = intent.ranking_time_range or intent.time_range or {}
    ranking_year = ranking_time_range.get("report_year")
    ranking_period = resolve_current_report_period(
        ranking_time_range.get("report_period", "FY")
    )
    if not isinstance(ranking_year, int) or not isinstance(ranking_period, str):
        return None

    calculation_time_range = intent.calculation_time_range or intent.time_range or {}
    calculation_year = calculation_time_range.get("report_year")
    calculation_period = resolve_current_report_period(
        calculation_time_range.get("report_period", "FY")
    )
    if not isinstance(calculation_year, int) or not isinstance(calculation_period, str):
        return None

    return (
        "WITH top_companies AS (\n"
        "    SELECT\n"
        "        stock_code,\n"
        "        stock_abbr,\n"
        "        equity_unappropriated_profit\n"
        "    FROM balance_sheet\n"
        f"    WHERE report_year = {ranking_year}\n"
        f"      AND report_period = '{ranking_period}'\n"
        "      AND equity_unappropriated_profit IS NOT NULL\n"
        "    ORDER BY equity_unappropriated_profit DESC\n"
        f"    LIMIT {topn_limit}\n"
        ")\n"
        "SELECT\n"
        "    top.stock_code,\n"
        "    top.stock_abbr,\n"
        "    top.equity_unappropriated_profit AS equity_unappropriated_profit,\n"
        "    income.net_profit AS net_profit,\n"
        "    CASE\n"
        "        WHEN income.net_profit IS NULL\n"
        "             OR top.equity_unappropriated_profit IS NULL\n"
        "             OR top.equity_unappropriated_profit = 0 THEN NULL\n"
        "        ELSE ROUND(income.net_profit / ABS(top.equity_unappropriated_profit) * 100, 2)\n"
        "    END AS ratio_percent,\n"
        "    income.report_year AS calculation_report_year,\n"
        "    income.report_period AS calculation_report_period\n"
        "FROM top_companies top\n"
        "LEFT JOIN income_sheet income\n"
        "    ON top.stock_code = income.stock_code\n"
        f"   AND income.report_year = {calculation_year}\n"
        f"   AND income.report_period = '{calculation_period}'\n"
        "ORDER BY top.equity_unappropriated_profit DESC"
    )
