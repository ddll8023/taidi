"""特殊查询处理：跨表 TopN、多指标交集等"""
import re
from typing import Any

from app.constants import chat as constants_chat
from app.schemas import chat as schemas_chat
from app.utils.logger_config import setup_logger
from app.services.chat.metric import (
    _build_time_range,
    _extract_ordered_time_mentions,
    _format_time_range_label,
    _resolve_current_report_period,
)

logger = setup_logger(__name__)


"""辅助函数"""


def _extract_topn_limit(question: str) -> int | None:
    if not question:
        return None

    digit_match = re.search(r"前\s*(\d+)", question)
    if digit_match:
        return int(digit_match.group(1))

    chinese_match = re.search(r"前\s*([一二两三四五六七八九十]+)", question)
    if not chinese_match:
        return None

    numeral = chinese_match.group(1)
    if numeral == "十":
        return 10
    if "十" in numeral:
        parts = numeral.split("十")
        tens = constants_chat.CHINESE_NUMERAL_MAP.get(parts[0], 1) if parts[0] else 1
        ones = (
            constants_chat.CHINESE_NUMERAL_MAP.get(parts[1], 0)
            if len(parts) > 1 and parts[1]
            else 0
        )
        return tens * 10 + ones

    return constants_chat.CHINESE_NUMERAL_MAP.get(numeral)


def _is_cross_table_topn_ratio_question(question: str) -> bool:
    if not question:
        return False
    return (
        "排名前" in question
        and _extract_topn_limit(question) is not None
        and all(
            keyword in question
            for keyword in constants_chat.CROSS_TABLE_TOPN_RATIO_KEYWORDS
        )
        and any(
            keyword in question
            for keyword in ["占未分配利润", "占未分配利润的比例", "比例"]
        )
    )


def _infer_cross_table_topn_ratio_time_ranges(
    question: str,
    fallback_time_range: dict | None,
) -> tuple[dict | None, dict | None]:
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


def _is_multi_metric_topn_intersection_question(
    intent: schemas_chat.IntentResult,
) -> bool:
    question = intent.question or ""
    metrics = intent.get_metric_list()
    if intent.query_type != schemas_chat.QueryType.RANKING or len(metrics) < 2:
        return False

    return "均排名前" in question and _extract_topn_limit(question) is not None


def _generate_multi_metric_topn_intersection_sql(
    intent: schemas_chat.IntentResult,
) -> str | None:
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

    limit = _extract_topn_limit(intent.question or "")
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


def _generate_cross_table_topn_ratio_sql(
    intent: schemas_chat.IntentResult,
) -> str | None:
    question = intent.question or ""
    if not _is_cross_table_topn_ratio_question(question):
        return None

    topn_limit = _extract_topn_limit(question)
    if topn_limit is None:
        return None

    ranking_time_range = intent.ranking_time_range or intent.time_range or {}
    ranking_year = ranking_time_range.get("report_year")
    ranking_period = _resolve_current_report_period(
        ranking_time_range.get("report_period", "FY")
    )
    if not isinstance(ranking_year, int) or not isinstance(ranking_period, str):
        return None

    calculation_time_range = intent.calculation_time_range or intent.time_range or {}
    calculation_year = calculation_time_range.get("report_year")
    calculation_period = _resolve_current_report_period(
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


def _build_multi_metric_topn_intersection_answer(
    question: str,
    query_result: list[dict],
    intent: schemas_chat.IntentResult,
) -> str | None:
    if not _is_multi_metric_topn_intersection_question(intent) or not query_result:
        return None

    metrics = intent.get_metric_list()
    topn_limit = _extract_topn_limit(question)
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
                f"- “均排名前{topn_limit}”表示先分别对各指标做排名，再取交集；"
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
) -> str | None:
    if not _is_cross_table_topn_ratio_question(intent.question or question):
        return None

    ranking_label = _format_time_range_label(
        intent.ranking_time_range or intent.time_range
    )
    calculation_label = _format_time_range_label(
        intent.calculation_time_range or intent.time_range
    )
    topn_limit = _extract_topn_limit(question) or len(query_result)

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
            f"但缺少{calculation_label}净利润数据，当前无法计算“净利润占未分配利润比例”。"
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
