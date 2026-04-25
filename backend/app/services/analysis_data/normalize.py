"""结构化数据规范化校验服务"""
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import Numeric

from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.constants.analysis_data import (
    FACT_MODEL_MAP,
    FACT_IDENTITY_FIELDS,
    FACT_MODEL_COLUMNS,
    FACT_MODEL_FIELD_SET,
)

logger = setup_logger(__name__)


def _normalize_structured_payload(
    payload: dict[str, list[dict[str, Any]]],
    use_full_pdf: bool = False,
) -> dict[str, dict[str, Any]]:
    """将模型输出规范化为事实表可入库的单表单行快照。"""
    if not isinstance(payload, dict):
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "结构化输出顶层必须是对象")

    expected_tables = set(FACT_MODEL_MAP)
    actual_tables = set(payload)
    missing_tables = sorted(expected_tables - actual_tables)
    unexpected_tables = sorted(actual_tables - expected_tables)

    if missing_tables:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"结构化输出缺少表：{', '.join(missing_tables)}",
        )
    if unexpected_tables:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"结构化输出包含未定义表：{', '.join(unexpected_tables)}",
        )

    normalized_records: dict[str, dict[str, Any]] = {}
    empty_tables: list[str] = []
    for table_name in FACT_MODEL_MAP:
        normalized_record = _normalize_single_table_record(
            table_name=table_name,
            raw_records=payload[table_name],
        )
        if normalized_record is None:
            empty_tables.append(table_name)
        else:
            normalized_records[table_name] = normalized_record

    if empty_tables:
        if use_full_pdf:
            logger.warning(
                "使用全部PDF内容后仍有空表，可能是摘要版报告: empty_tables=%s",
                ",".join(empty_tables),
            )
        else:
            logger.warning(
                "部分表返回空记录，允许继续处理: empty_tables=%s",
                ",".join(empty_tables),
            )

    return normalized_records


def _normalize_single_table_record(
    table_name: str,
    raw_records: Any,
) -> dict[str, Any] | None:
    if not isinstance(raw_records, list):
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 的值必须是列表",
        )

    if not raw_records:
        return None

    if len(raw_records) > 1:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 只允许一条记录，当前返回 {len(raw_records)} 条",
        )

    raw_record = raw_records[0]
    if not isinstance(raw_record, dict):
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 的记录必须是对象",
        )

    record_fields = set(raw_record)
    identity_fields = sorted(record_fields & FACT_IDENTITY_FIELDS)
    if identity_fields:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 不允许输出主表身份字段：{', '.join(identity_fields)}",
        )

    unexpected_fields = sorted(record_fields - FACT_MODEL_FIELD_SET[table_name])
    if unexpected_fields:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 存在未定义字段：{', '.join(unexpected_fields)}",
        )

    normalized_record: dict[str, Any] = {}
    for column in FACT_MODEL_COLUMNS[table_name]:
        normalized_record[column.name] = _normalize_metric_value(
            table_name=table_name,
            field_name=column.name,
            column=column,
            value=raw_record.get(column.name),
        )
    return normalized_record


def _normalize_metric_value(
    table_name: str,
    field_name: str,
    column,
    value: Any,
) -> Any:
    if value is None:
        return None

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        value = stripped

    if isinstance(column.type, Numeric):
        return _normalize_numeric_value(table_name, field_name, value)

    return value


def _normalize_numeric_value(
    table_name: str, field_name: str, value: Any
) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name}.{field_name} 必须是数值，不能是布尔值",
        )
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        normalized = value.replace(",", "").replace("，", "").strip()
        if normalized.endswith("%"):
            normalized = normalized[:-1].strip()
        if normalized.lower() in {"null", "none"}:
            return None
        try:
            return Decimal(normalized)
        except InvalidOperation as exc:
            raise ServiceException(
                ErrorCode.AI_SERVICE_ERROR,
                f"{table_name}.{field_name} 不是合法数值：{value}",
            ) from exc

    raise ServiceException(
        ErrorCode.AI_SERVICE_ERROR,
        f"{table_name}.{field_name} 的值类型不受支持：{type(value).__name__}",
    )
