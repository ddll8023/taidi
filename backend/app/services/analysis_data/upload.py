"""财报文件上传与建档服务：单文件/批量上传、PDF保存、身份主表建档"""
import os
import uuid
from typing import NamedTuple

from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.database import commit_or_rollback
from app.models import financial_report as models_financial_report
from app.models import validation_log as models_validation_log
from app.schemas import analysis_data as schemas_analysis_data
from app.schemas.common import ErrorCode
from app.services import financial_report as services_financial_report
from app.services import validation_log as services_validation_log
from app.utils.exception import ServiceException
from app.utils.file import save_file
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


class _UploadedPdfArtifact(NamedTuple):
    source_file_name: str
    storage_path: str


# ========== 公共入口函数 ==========


async def upload_archive_only(db: Session, file_name: str, file_content: bytes):
    """上传单个财报文件并建档"""
    raw_source_file_name = str(file_name or "").strip() or None
    logger.info(f"开始上传财报文件（仅建档）: source_file_name={raw_source_file_name}")

    file_stage_log_id = services_validation_log.start_validation_stage(
        db=db,
        source_file_name=raw_source_file_name,
        stage=models_validation_log.VALIDATION_STAGE_FILE_ARCHIVE,
        check_type=models_validation_log.VALIDATION_CHECK_TYPE_FILE_RULE,
        message="开始保存上传财报文件",
    )

    try:
        archived_pdf = _archive_uploaded_pdf(raw_source_file_name, file_content)
    except ServiceException as exc:
        services_validation_log.mark_validation_stage_failed(
            db=db,
            log_id=file_stage_log_id,
            source_file_name=raw_source_file_name,
            message=exc.message,
            details=services_validation_log.build_validation_failure_details(exc),
            error_code=services_validation_log.get_service_error_code(exc),
            check_type=models_validation_log.VALIDATION_CHECK_TYPE_FILE_RULE,
            is_blocking=True,
        )
        raise
    except Exception as exc:
        logger.error(f"财报文件建档异常：{exc}", exc_info=True)
        message = "财报文件建档失败"
        services_validation_log.mark_validation_stage_failed(
            db=db,
            log_id=file_stage_log_id,
            source_file_name=raw_source_file_name,
            message=message,
            details=services_validation_log.build_validation_failure_details(exc),
            error_code=services_validation_log.get_service_error_code(exc),
            check_type=models_validation_log.VALIDATION_CHECK_TYPE_FILE_RULE,
            is_blocking=True,
        )
        raise ServiceException(ErrorCode.INTERNAL_ERROR, message) from exc

    services_validation_log.mark_validation_stage_passed(
        db=db,
        log_id=file_stage_log_id,
        source_file_name=archived_pdf.source_file_name,
        message="上传财报文件保存成功",
        details={"storage_path": archived_pdf.storage_path},
    )
    logger.info(
        f"财报文件建档完成: source_file_name={archived_pdf.source_file_name} storage_path={archived_pdf.storage_path}"
    )

    identity_stage_log_id = services_validation_log.start_validation_stage(
        db=db,
        source_file_name=archived_pdf.source_file_name,
        stage=models_validation_log.VALIDATION_STAGE_REPORT_IDENTITY,
        check_type=models_validation_log.VALIDATION_CHECK_TYPE_PDF_METADATA,
        message="开始解析财报身份主表",
        details={"storage_path": archived_pdf.storage_path},
    )
    try:
        financial_report = (
            services_financial_report.upsert_financial_report_from_source(
                db=db,
                source_file_name=archived_pdf.source_file_name,
                file_path=archived_pdf.storage_path,
                structured_json_path=None,
            )
        )
    except ServiceException as exc:
        db.rollback()
        services_validation_log.mark_validation_stage_failed(
            db=db,
            log_id=identity_stage_log_id,
            source_file_name=archived_pdf.source_file_name,
            message=exc.message,
            details=services_validation_log.build_validation_failure_details(
                exc,
                {"storage_path": archived_pdf.storage_path},
            ),
            error_code=services_validation_log.get_service_error_code(exc),
            check_type=services_validation_log.infer_report_identity_check_type(
                exc.message
            ),
            is_blocking=True,
        )
        raise
    except Exception as exc:
        db.rollback()
        logger.error(f"财报身份主表建档异常：{exc}", exc_info=True)
        message = "财报身份主表建档失败"
        services_validation_log.mark_validation_stage_failed(
            db=db,
            log_id=identity_stage_log_id,
            source_file_name=archived_pdf.source_file_name,
            message=message,
            details=services_validation_log.build_validation_failure_details(
                exc,
                {"storage_path": archived_pdf.storage_path},
            ),
            error_code=services_validation_log.get_service_error_code(exc),
            check_type=models_validation_log.VALIDATION_CHECK_TYPE_PDF_METADATA,
            is_blocking=True,
        )
        raise ServiceException(ErrorCode.INTERNAL_ERROR, message) from exc

    report_id = int(financial_report.id)
    services_validation_log.mark_validation_stage_passed(
        db=db,
        log_id=identity_stage_log_id,
        report=financial_report,
        message="财报身份主表建档成功",
        details={
            "report_id": report_id,
            "storage_path": archived_pdf.storage_path,
            "stock_code": financial_report.stock_code,
            "stock_abbr": financial_report.stock_abbr,
            "report_year": financial_report.report_year,
            "report_period": financial_report.report_period,
            "report_type": financial_report.report_type,
            "exchange": financial_report.exchange,
        },
    )
    logger.info(
        f"财报身份主表建档完成（仅建档）: report_id={report_id} stock_code={financial_report.stock_code} stock_abbr={financial_report.stock_abbr} report_year={financial_report.report_year} report_period={financial_report.report_period}"
    )

    commit_or_rollback(db)

    return schemas_analysis_data.FinancialReportArchiveResponse(
        report_id=financial_report.id,
        stock_code=financial_report.stock_code,
        stock_abbr=financial_report.stock_abbr,
        report_title=financial_report.report_title,
        parse_status=financial_report.parse_status,
    )


async def upload_archive_batch(db: Session, file_items: list[tuple[str, bytes]]):
    """批量上传财报文件并建档"""
    if not file_items:
        raise ServiceException(ErrorCode.PARAM_ERROR, "请选择至少一个文件")

    total = len(file_items)
    success_count = 0
    failed_count = 0
    success_reports: list[dict] = []
    failed_files: list[dict] = []

    logger.info(f"开始批量上传财报文件: total={total}")

    for file_name, file_content in file_items:
        display_name = file_name or "未知文件名"
        try:
            result = await upload_archive_only(db, file_name, file_content)
            success_count += 1
            success_reports.append(
                {
                    "report_id": result.report_id,
                    "stock_code": result.stock_code,
                    "stock_abbr": result.stock_abbr,
                    "report_title": result.report_title,
                    "file_name": display_name,
                }
            )
            logger.info(f"批量上传成功: file_name={display_name} report_id={result.report_id}")
        except ServiceException as exc:
            failed_count += 1
            failed_files.append(
                {
                    "file_name": display_name,
                    "error": exc.message,
                }
            )
            logger.warning(
                f"批量上传失败: file_name={display_name} error={exc.message}"
            )
        except Exception as exc:
            failed_count += 1
            failed_files.append(
                {
                    "file_name": display_name,
                    "error": "系统内部错误",
                }
            )
            logger.error(
                f"批量上传异常: file_name={display_name} error={exc}", exc_info=True
            )

    logger.info(
        f"批量上传完成: total={total} success={success_count} failed={failed_count}"
    )
    return schemas_analysis_data.BatchUploadResponse(
        total=total,
        success_count=success_count,
        failed_count=failed_count,
        success_reports=success_reports,
        failed_files=failed_files,
    )


"""辅助函数"""


def _archive_uploaded_pdf(source_file_name: str, file_content: bytes):
    """保存上传文件并返回建档信息"""
    if not source_file_name:
        raise ServiceException(ErrorCode.PARAM_ERROR, "上传文件名不能为空")

    storage_path = _save_pdf_data(source_file_name, file_content)
    logger.info(f"财报文件已建档: source_file_name={source_file_name}")
    return _UploadedPdfArtifact(
        source_file_name=source_file_name,
        storage_path=storage_path,
    )


def _save_pdf_data(file_name: str, file_content: bytes):
    """将上传的PDF保存到本地"""
    unique_id = uuid.uuid4().hex
    name, ext = os.path.splitext(file_name)
    new_file_name = f"{name} - {unique_id}{ext}"
    file_path = os.path.join(settings.fujian2_UPLOAD_DIR, new_file_name)
    try:
        save_file(file_content, file_path)
        logger.info(f"文件保存成功: {file_path}")
    except Exception as exc:
        logger.error(f"save_pdf_data错误：{exc}", exc_info=True)
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "文件保存失败") from exc
    return file_path
