"""SQL 执行与结果后处理服务"""
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.constants import chat as constants_chat
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.services.chat.sql_builder import (
    _build_allowed_column_names,
    _build_ten_thousand_unit_column_names,
    _extract_column_refs_from_select,
    _extract_declared_column_aliases,
    _extract_declared_cte_names,
    _extract_select_columns,
    _normalize_result_column_name,
)

logger = setup_logger(__name__)


"""辅助函数"""


def _validate_sql(sql: str) -> tuple[bool, str]:
    stripped_sql = sql.strip()
    sql_upper = stripped_sql.upper()

    for keyword in constants_chat.FORBIDDEN_KEYWORDS:
        pattern = r"\b" + keyword + r"\b"
        if re.search(pattern, sql_upper):
            return False, f"SQL包含禁止关键字: {keyword}"

    if not re.match(r"(?i)^(WITH|SELECT)\b", stripped_sql):
        return False, "SQL必须以SELECT或WITH开头"

    allowed_table_names = {
        table_name.lower() for table_name in constants_chat.ALLOWED_TABLES
    }
    allowed_table_names.update(_extract_declared_cte_names(stripped_sql))

    found_tables = re.findall(r"(?i)\b(?:FROM|JOIN)\s+(\w+)", stripped_sql)
    for table_name in found_tables:
        if table_name.lower() not in allowed_table_names:
            return False, f"SQL引用了不允许的表: {table_name}"

    raw_columns = _extract_select_columns(stripped_sql)
    bare_refs = _extract_column_refs_from_select(raw_columns)
    allowed_column_names = (
        _build_allowed_column_names() | _extract_declared_column_aliases(stripped_sql)
    )
    for ref in bare_refs:
        if ref.lower() not in allowed_column_names:
            logger.warning("SQL列名可能无效: '%s' (不在schema白名单中)", ref)
            return False, f"SQL列名不在schema白名单中: {ref}"

    if "LIMIT" not in sql_upper:
        sql = stripped_sql.rstrip(";") + " LIMIT 1000"

    return True, ""


def _execute_query(sql: str, db: Session) -> tuple[list[dict], list[dict]]:
    """执行SQL查询，返回(查询结果, 公司列表)"""
    try:
        result = db.execute(text(sql))
        columns = list(result.keys())
        rows = result.fetchall()
        data = [dict(zip(columns, row)) for row in rows]
        data = _normalize_abnormal_unit_rows(data)
        data = _apply_post_normalization_sql_adjustments(data, sql)

        companies = []
        company_columns = ["stock_abbr", "stock_code", "company_name", "company"]
        for row in data:
            company_info = None
            for col in company_columns:
                if col in row and row[col]:
                    company_info = {"value": str(row[col]), "type": col}
                    break
            if company_info and company_info not in companies:
                companies.append(company_info)

        return data, companies
    except Exception as exc:
        logger.error("SQL执行失败: sql=%s error=%s", sql, str(exc))
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR, f"SQL查询执行失败: {str(exc)}"
        ) from exc


def _to_decimal_value(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, Decimal):
        return value

    if isinstance(value, int):
        return Decimal(value)

    if isinstance(value, float):
        return Decimal(str(value))

    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if not cleaned or cleaned.endswith("%"):
            return None
        try:
            return Decimal(cleaned)
        except InvalidOperation:
            return None

    return None


def _restore_numeric_type(original_value: Any, normalized_value: Decimal) -> Any:
    quantized_value = normalized_value.quantize(Decimal("0.01"))

    if isinstance(original_value, Decimal):
        return quantized_value

    if isinstance(original_value, int):
        if quantized_value == quantized_value.to_integral_value():
            return int(quantized_value)
        return float(quantized_value)

    if isinstance(original_value, float):
        return float(quantized_value)

    if isinstance(original_value, str):
        return format(quantized_value, "f")

    return quantized_value


def _median_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None

    sorted_values = sorted(values)
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2 == 1:
        return sorted_values[mid]

    return (sorted_values[mid - 1] + sorted_values[mid]) / Decimal("2")


def _is_ten_thousand_unit_result_column(column: str) -> bool:
    normalized_column = _normalize_result_column_name(column)
    return (
        normalized_column in _build_ten_thousand_unit_column_names()
        or "万元" in str(column)
    )


def _normalize_abnormal_unit_rows(rows: list[dict]) -> list[dict]:
    """仅对标注为万元的异常大值做元->万元统一。"""
    if not rows:
        return rows

    normalized_rows = [dict(row) for row in rows]
    all_columns: list[str] = []
    seen_columns: set[str] = set()
    for row in normalized_rows:
        for column in row.keys():
            if column not in seen_columns:
                seen_columns.add(column)
                all_columns.append(column)

    candidate_columns = [
        column for column in all_columns if _is_ten_thousand_unit_result_column(column)
    ]

    for column in candidate_columns:
        column_values = [_to_decimal_value(row.get(column)) for row in normalized_rows]
        non_zero_values = [
            abs(value) for value in column_values if value is not None and value != 0
        ]
        if not non_zero_values:
            continue

        for index, decimal_value in enumerate(column_values):
            if decimal_value is None:
                continue

            abs_value = abs(decimal_value)
            if abs_value < Decimal("100000000"):
                continue

            peer_values = [
                abs(value)
                for peer_index, value in enumerate(column_values)
                if peer_index != index and value is not None and value != 0
            ]
            peer_median = _median_decimal(peer_values)
            if (
                peer_median is not None
                and peer_median > 0
                and abs_value < peer_median * Decimal("100")
            ):
                continue

            normalized_value = decimal_value / Decimal("10000")
            normalized_rows[index][column] = _restore_numeric_type(
                rows[index].get(column),
                normalized_value,
            )
            logger.warning(
                "检测到疑似元/万元混用，已自动按万元统一: column=%s raw=%s normalized=%s",
                column,
                decimal_value,
                normalized_rows[index][column],
            )

    return normalized_rows


def _extract_where_clause(sql: str) -> str:
    match = re.search(
        r"(?is)\bWHERE\b(?P<where>.*?)(?=\bORDER\s+BY\b|\bGROUP\s+BY\b|\bLIMIT\b|$)",
        sql,
    )
    return match.group("where") if match else ""


def _extract_order_by_clause(sql: str) -> str:
    match = re.search(
        r"(?is)\bORDER\s+BY\b(?P<order>.*?)(?=\bLIMIT\b|$)",
        sql,
    )
    return match.group("order") if match else ""


def _apply_post_normalization_sql_adjustments(rows: list[dict], sql: str) -> list[dict]:
    """对归一化后的结果，重应用万元字段的简单数值过滤与排序。"""
    if not rows:
        return rows

    adjusted_rows = list(rows)
    result_columns = {str(column) for row in adjusted_rows for column in row.keys()}

    where_clause = _extract_where_clause(sql)
    if where_clause:
        numeric_predicates = re.findall(
            r"(?i)(?:\b\w+\.)?(\w+)\s*(>=|<=|>|<|=)\s*(-?\d+(?:\.\d+)?)",
            where_clause,
        )
        for column, operator, raw_threshold in numeric_predicates:
            if column not in result_columns or not _is_ten_thousand_unit_result_column(
                column
            ):
                continue

            threshold = Decimal(raw_threshold)

            def _matches(row: dict) -> bool:
                value = _to_decimal_value(row.get(column))
                if value is None:
                    return False
                if operator == ">":
                    return value > threshold
                if operator == ">=":
                    return value >= threshold
                if operator == "<":
                    return value < threshold
                if operator == "<=":
                    return value <= threshold
                return value == threshold

            adjusted_rows = [row for row in adjusted_rows if _matches(row)]

    if not adjusted_rows:
        return adjusted_rows

    order_clause = _extract_order_by_clause(sql)
    if order_clause:
        order_match = re.match(
            r"(?is)\s*(?:\b\w+\.)?(\w+)(?:\s+(ASC|DESC))?",
            order_clause.strip(),
        )
        if order_match:
            order_column = order_match.group(1)
            direction = (order_match.group(2) or "ASC").upper()
            if order_column in result_columns and _is_ten_thousand_unit_result_column(
                order_column
            ):
                adjusted_rows = sorted(
                    adjusted_rows,
                    key=lambda row: (
                        _to_decimal_value(row.get(order_column))
                        if _to_decimal_value(row.get(order_column)) is not None
                        else Decimal("-Infinity")
                    ),
                    reverse=direction == "DESC",
                )

    return adjusted_rows
