import json
from typing import Any

from sqlalchemy.orm import Session

from app.models import financial_report as models_financial_report
from app.models import validation_log as models_validation_log
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException


STRUCT_SCHEMA_MESSAGE_KEYWORDS = (
    "顶层必须是对象",
    "缺少表",
    "包含未定义表",
    "值必须是列表",
    "记录必须是对象",
    "只允许一条记录",
    "不允许输出主表身份字段",
    "存在未定义字段",
)
COMPANY_MATCH_MESSAGE_KEYWORDS = (
    "company_basic_info",
    "股票简称",
    "股票代码",
    "交易所",
)


def start_validation_stage(
    db: Session,
    *,
    stage: str,
    check_type: str,
    message: str,
    source_file_name: str | None = None,
    report: models_financial_report.FinancialReport | None = None,
    details: dict[str, Any] | None = None,
) -> int:
    entity = models_validation_log.ValidationLog(
        stage=stage,
        check_type=check_type,
        status=models_validation_log.VALIDATION_LOG_STATUS_PROCESSING,
        is_blocking=False,
        error_code=None,
        message=_normalize_message(message),
        details_json=_serialize_details(details),
    )
    _sync_source_file_name(entity, source_file_name)
    _sync_report_snapshot(entity, report)
    db.add(entity)
    db.commit()
    db.refresh(entity)
    return int(entity.id)


def mark_validation_stage_passed(
    db: Session,
    *,
    log_id: int,
    message: str,
    details: dict[str, Any] | None = None,
    source_file_name: str | None = None,
    report: models_financial_report.FinancialReport | None = None,
    check_type: str | None = None,
) -> None:
    entity = _load_validation_log(db, log_id)
    if check_type is not None:
        entity.check_type = check_type
    entity.status = models_validation_log.VALIDATION_LOG_STATUS_PASSED
    entity.is_blocking = False
    entity.error_code = None
    entity.message = _normalize_message(message)
    entity.details_json = _serialize_details(details)
    _sync_source_file_name(entity, source_file_name)
    _sync_report_snapshot(entity, report)
    db.commit()


def mark_validation_stage_failed(
    db: Session,
    *,
    log_id: int,
    message: str,
    details: dict[str, Any] | None = None,
    error_code: int | None = None,
    source_file_name: str | None = None,
    report: models_financial_report.FinancialReport | None = None,
    check_type: str | None = None,
    is_blocking: bool = True,
) -> None:
    entity = _load_validation_log(db, log_id)
    if check_type is not None:
        entity.check_type = check_type
    entity.status = models_validation_log.VALIDATION_LOG_STATUS_FAILED
    entity.is_blocking = is_blocking
    entity.error_code = error_code
    entity.message = _normalize_message(message)
    entity.details_json = _serialize_details(details)
    _sync_source_file_name(entity, source_file_name)
    _sync_report_snapshot(entity, report)
    db.commit()


def infer_report_identity_check_type(message: str) -> str:
    normalized = _normalize_message(message)
    if any(keyword in normalized for keyword in COMPANY_MATCH_MESSAGE_KEYWORDS):
        return models_validation_log.VALIDATION_CHECK_TYPE_COMPANY_MATCH
    return models_validation_log.VALIDATION_CHECK_TYPE_PDF_METADATA


def infer_structured_validation_check_type(message: str) -> str:
    normalized = _normalize_message(message)
    if any(keyword in normalized for keyword in STRUCT_SCHEMA_MESSAGE_KEYWORDS):
        return models_validation_log.VALIDATION_CHECK_TYPE_STRUCT_SCHEMA
    return models_validation_log.VALIDATION_CHECK_TYPE_STRUCT_VALUE


def build_validation_failure_details(
    exc: Exception,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(details or {})
    payload["exception_type"] = type(exc).__name__
    payload["error_message"] = _get_exception_message(exc)
    if isinstance(exc, ServiceException):
        payload["service_error_code"] = int(exc.code)
    return payload


def get_service_error_code(exc: Exception) -> int:
    if isinstance(exc, ServiceException):
        return int(exc.code)
    return int(ErrorCode.INTERNAL_ERROR)


def _load_validation_log(db: Session, log_id: int) -> models_validation_log.ValidationLog:
    entity = db.get(models_validation_log.ValidationLog, log_id)
    if entity is None:
        raise ValueError(f"validation_log 不存在，log_id={log_id}")
    return entity


def _serialize_details(details: dict[str, Any] | None) -> str | None:
    if not details:
        return None
    return json.dumps(details, ensure_ascii=False, sort_keys=True, default=str)


def _normalize_message(message: str) -> str:
    normalized = str(message).strip()
    if not normalized:
        raise ValueError("validation_log.message 不能为空")
    return normalized


def _get_exception_message(exc: Exception) -> str:
    if isinstance(exc, ServiceException):
        return exc.message
    normalized = str(exc).strip()
    return normalized or exc.__class__.__name__


def _sync_source_file_name(
    entity: models_validation_log.ValidationLog,
    source_file_name: str | None,
) -> None:
    if source_file_name is None:
        return
    normalized = str(source_file_name).strip()
    entity.source_file_name = normalized or None


def _sync_report_snapshot(
    entity: models_validation_log.ValidationLog,
    report: models_financial_report.FinancialReport | None,
) -> None:
    if report is None:
        return
    entity.report_id = report.id
    entity.source_file_name = report.source_file_name
    entity.stock_code = report.stock_code
    entity.stock_abbr = report.stock_abbr
    entity.report_year = report.report_year
    entity.report_period = report.report_period
    entity.report_type = report.report_type
