"""指标处理与派生指标计算服务"""
import os
import re
from typing import Any

import yaml
from sqlalchemy.orm import Session

from app.constants import chat as constants_chat
from app.schemas import chat as schemas_chat
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


"""辅助函数"""


def _normalize_time_range(time_range: dict | list | None) -> dict | None:
    """规范化time_range格式

    LLM可能返回数组格式的time_range，需要转换为字典格式：
    - 如果是数组：提取所有report_year组成数组，report_period取第一个
    - 如果是字典：直接返回
    - 如果是None：返回None
    """
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
        logger.info("time_range数组格式已转换为字典: %s", result)
        return result

    return None


def _build_time_range(
    report_year: int | None = None,
    report_period: str | None = None,
    is_range: bool = False,
) -> dict | None:
    if report_year is None and not report_period:
        return None

    result = {"is_range": is_range}
    if report_year is not None:
        result["report_year"] = report_year
    if report_period:
        result["report_period"] = report_period
    return result


def _extract_ordered_time_mentions(question: str) -> list[dict[str, Any]]:
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


def _merge_metric_payload(
    current_metric: dict | list[dict] | None,
    inferred_metrics: list[dict],
) -> dict | list[dict] | None:
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


def _get_metric_by_field(field_name: str | None) -> dict | None:
    if not field_name:
        return None

    for alias, metric in constants_chat.METRIC_ALIAS_MAP.items():
        if metric.get("field") == field_name:
            resolved_metric = dict(metric)
            resolved_metric["display_name"] = alias
            return resolved_metric
    return None


def _normalize_metric_payload(metric_data: Any) -> dict | list[dict] | None:
    """将 LLM 返回的 metric 统一规范成 dict 或 list[dict]。"""
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

                resolved_metric = _get_metric_by_field(raw_field) or {
                    "field": raw_field
                }

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


def _resolve_current_report_period(report_period: Any) -> str:
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


def _extract_comparison_time_points(time_range: dict | None) -> list[dict[str, Any]]:
    """提取 comparison 场景下的双时间点结构。"""
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
) -> tuple[str, str]:
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


def _has_non_null_measure_values(
    rows: list[dict],
    metric_field: str = "",
) -> bool:
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


def _generate_qoq_comparison_sql(
    intent: schemas_chat.IntentResult,
    metric_field: str,
    table_name: str,
    comparison_points: list[dict[str, Any]],
    period_sequence: dict[str, dict[str, Any]],
) -> str | None:
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


def _extract_metrics_from_question(question: str) -> list[dict]:
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


def _load_derived_metrics_config() -> dict:
    """加载派生指标配置文件"""
    app_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    config_path = os.path.join(
        app_dir,
        "config",
        "prompts",
        "derived_metrics.yaml",
    )
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.warning("加载派生指标配置失败: %s", e)
        return {}


def _build_company_filter(
    intent: schemas_chat.IntentResult, table_alias: str = ""
) -> str:
    """构建公司筛选条件"""
    companies = intent.get_company_list()
    if not companies:
        return "1=1"

    prefix = f"{table_alias}." if table_alias else ""
    conditions = []
    for company in companies:
        value = company.get("value", "")
        company_type = company.get("type", "stock_abbr")
        if company_type == "stock_code":
            conditions.append(f"{prefix}stock_code = '{value}'")
        else:
            conditions.append(f"{prefix}stock_abbr = '{value}'")

    if len(conditions) == 1:
        return conditions[0]
    return "(" + " OR ".join(conditions) + ")"


def _generate_derived_metric_sql(
    intent: schemas_chat.IntentResult,
    derived_type: schemas_chat.DerivedMetricType,
) -> str | None:
    """根据派生指标类型生成SQL模板"""
    config = _load_derived_metrics_config()
    if not config:
        return None

    first_metric = intent.get_first_metric()
    metric = first_metric or {}
    metric_field = metric.get("field", "")
    table_name = metric.get("table", "")
    metric_field, table_name = _resolve_prestored_derived_metric_source(
        metric_field,
        table_name,
        derived_type,
    )
    time_range = intent.time_range or {}
    report_year = time_range.get("report_year", 2025)
    report_period = _resolve_current_report_period(
        time_range.get("report_period", "FY")
    )
    comparison_points = _extract_comparison_time_points(time_range)

    if isinstance(report_year, list) and not (
        intent.query_type == schemas_chat.QueryType.COMPARISON
        and len(comparison_points) == 2
    ):
        logger.warning(
            "report_year为数组格式 %s，派生指标模板不支持多年查询，返回None让LLM生成SQL",
            report_year,
        )
        return None

    JOIN_DERIVED_TYPES = {
        schemas_chat.DerivedMetricType.YOY_GROWTH,
        schemas_chat.DerivedMetricType.QOQ_GROWTH,
        schemas_chat.DerivedMetricType.DIFFERENCE,
    }
    table_alias = "t1" if derived_type in JOIN_DERIVED_TYPES else ""
    company_filter = _build_company_filter(intent, table_alias=table_alias)

    type_key = derived_type.value
    template_config = config.get(type_key)
    if not template_config:
        return None

    sql_template = template_config.get("sql_template", "")

    try:
        if derived_type == schemas_chat.DerivedMetricType.YOY_GROWTH:
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                company_filter=company_filter,
                report_year=report_year,
                report_period=report_period,
            )

        elif derived_type == schemas_chat.DerivedMetricType.QOQ_GROWTH:
            period_sequence = template_config.get("period_sequence", {})
            if (
                intent.query_type == schemas_chat.QueryType.COMPARISON
                and len(comparison_points) == 2
            ):
                comparison_sql = _generate_qoq_comparison_sql(
                    intent,
                    metric_field,
                    table_name,
                    comparison_points,
                    period_sequence,
                )
                if comparison_sql:
                    return comparison_sql

            period_info = period_sequence.get(
                report_period, {"prev_year_offset": 0, "prev_period": "Q1"}
            )
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                company_filter=company_filter,
                report_year=report_year,
                report_period=report_period,
                prev_year_offset=period_info.get("prev_year_offset", 0),
                prev_period=period_info.get("prev_period", "Q1"),
            )

        elif derived_type == schemas_chat.DerivedMetricType.CAGR:
            start_year = report_year - 3 if isinstance(report_year, int) else 2022
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                company_filter=company_filter,
                start_year=start_year,
                end_year=report_year,
                report_period=report_period,
            )

        elif derived_type == schemas_chat.DerivedMetricType.RATIO:
            common_ratios = template_config.get("common_ratios", {})
            ratio_config = None
            for ratio_key, ratio_info in common_ratios.items():
                if ratio_info.get("numerator") == metric_field:
                    ratio_config = ratio_info
                    break

            if ratio_config:
                return sql_template.format(
                    numerator_field=ratio_config.get("numerator", metric_field),
                    denominator_field=ratio_config.get(
                        "denominator", "total_operating_income"
                    ),
                    table_name=table_name,
                    company_filter=company_filter,
                    report_year=report_year,
                    report_period=report_period,
                )
            return None

        elif derived_type == schemas_chat.DerivedMetricType.INDUSTRY_AVG:
            metric_name = metric.get("display_name", metric_field)
            return sql_template.format(
                metric_field=metric_field,
                metric_name=metric_name,
                table_name=table_name,
                report_year=report_year,
                report_period=report_period,
            )

        elif derived_type == schemas_chat.DerivedMetricType.MEDIAN:
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                report_year=report_year,
                report_period=report_period,
            )

        elif derived_type == schemas_chat.DerivedMetricType.DIFFERENCE:
            if "," in metric_field or "," in table_name:
                return None
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                year_1=report_year,
                year_2=report_year - 1 if isinstance(report_year, int) else 2024,
                report_period=report_period,
            )

        elif derived_type == schemas_chat.DerivedMetricType.CORRELATION:
            numeric_cols = [metric_field]
            if len(numeric_cols) < 2:
                return None
            start_year = report_year - 3 if isinstance(report_year, int) else 2022
            return sql_template.format(
                metric_field_1=numeric_cols[0],
                metric_field_2=(
                    numeric_cols[1] if len(numeric_cols) > 1 else numeric_cols[0]
                ),
                table_name=table_name,
                company_filter=company_filter,
                start_year=start_year,
                end_year=report_year,
            )

        return None
    except Exception as e:
        logger.warning("派生指标SQL模板生成失败: %s", e)
        return None


def _detect_derived_metric(question: str) -> schemas_chat.DerivedMetricType | None:
    """检测问题中是否包含派生指标关键词"""
    for metric_type, keywords in constants_chat.DERIVED_METRIC_KEYWORDS.items():
        for keyword in keywords:
            if keyword in question:
                return metric_type
    return None


def _format_time_range_label(time_range: dict | None) -> str:
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
