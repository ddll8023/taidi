"""校验日志记录服务"""
import json
from typing import Any

from sqlalchemy.orm import Session

from app.constants import validation_log as constants_validation_log
from app.db.database import commit_or_rollback
from app.models import financial_report as models_financial_report
from app.models import validation_log as models_validation_log
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException


# ========== 公共入口函数 ==========

def start_validation_stage(
    db: Session,
    *,
    stage: str,
    check_type: str,
    message: str,
    source_file_name: str | None = None,
    report: models_financial_report.FinancialReport | None = None,
    details: dict[str, Any] | None = None,
):
    """创建一条处理中状态的校验日志并返回其ID"""
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
    commit_or_rollback(db)
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
):
    """将指定校验日志标记为通过"""
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
    commit_or_rollback(db)


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
):
    """将指定校验日志标记为失败"""
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
    commit_or_rollback(db)


def infer_report_identity_check_type(message: str):
    """根据消息内容推断报告身份校验的检查类型"""
    normalized = _normalize_message(message)
    if any(keyword in normalized for keyword in constants_validation_log.COMPANY_MATCH_MESSAGE_KEYWORDS):
        return models_validation_log.VALIDATION_CHECK_TYPE_COMPANY_MATCH
    return models_validation_log.VALIDATION_CHECK_TYPE_PDF_METADATA


def infer_structured_validation_check_type(message: str):
    """根据消息内容推断结构化数据校验的检查类型"""
    normalized = _normalize_message(message)
    if any(keyword in normalized for keyword in constants_validation_log.STRUCT_SCHEMA_MESSAGE_KEYWORDS):
        return models_validation_log.VALIDATION_CHECK_TYPE_STRUCT_SCHEMA
    return models_validation_log.VALIDATION_CHECK_TYPE_STRUCT_VALUE


def build_validation_failure_details(
    exc: Exception,
    details: dict[str, Any] | None = None,
):
    """从异常对象构建校验失败详情字典"""
    payload = dict(details or {})
    payload["exception_type"] = type(exc).__name__
    payload["error_message"] = _get_exception_message(exc)
    if isinstance(exc, ServiceException):
        payload["service_error_code"] = int(exc.code)
    return payload


def get_service_error_code(exc: Exception):
    """从异常中提取业务错误码"""
    if isinstance(exc, ServiceException):
        return int(exc.code)
    return int(ErrorCode.INTERNAL_ERROR)


"""辅助函数"""


def _load_validation_log(db: Session, log_id: int):
    """根据ID加载校验日志实体，不存在则抛出异常"""
    entity = db.get(models_validation_log.ValidationLog, log_id)
    if entity is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"validation_log 不存在，log_id={log_id}")
    return entity


def _serialize_details(details: dict[str, Any] | None):
    """将详情字典序列化为JSON字符串"""
    if not details:
        return None
    return json.dumps(details, ensure_ascii=False, sort_keys=True, default=str)


def _normalize_message(message: str):
    """规范化消息文本，为空时抛出异常"""
    normalized = str(message).strip()
    if not normalized:
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "校验日志消息不能为空")
    return normalized


def _get_exception_message(exc: Exception):
    """从异常对象中提取可读的错误消息"""
    if isinstance(exc, ServiceException):
        return exc.message
    normalized = str(exc).strip()
    return normalized or exc.__class__.__name__


def _sync_source_file_name(
    entity: models_validation_log.ValidationLog,
    source_file_name: str | None,
):
    """同步源文件名到校验日志实体"""
    if source_file_name is None:
        return
    normalized = str(source_file_name).strip()
    entity.source_file_name = normalized or None


def _sync_report_snapshot(
    entity: models_validation_log.ValidationLog,
    report: models_financial_report.FinancialReport | None,
):
    """同步财报主表快照字段到校验日志实体"""
    if report is None:
        return
    entity.report_id = report.id
    entity.source_file_name = report.source_file_name
    entity.stock_code = report.stock_code
    entity.stock_abbr = report.stock_abbr
    entity.report_year = report.report_year
    entity.report_period = report.report_period
    entity.report_type = report.report_type
