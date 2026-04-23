"""财报记录删除服务"""

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import financial_report as models_financial_report
from app.schemas import analysis_data as schemas_analysis_data
from app.schemas.common import ErrorCode
from app.services.analysis_data._constants import FACT_MODEL_MAP
from app.services.analysis_data.parse import (
    _load_financial_report_or_raise,
    _cleanup_report_files,
)
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


# ========== 公共入口函数 ==========


def delete_financial_report(
    db: Session, report_id: int
) -> schemas_analysis_data.FinancialReportDeleteResponse:
    """
    删除财报记录及其关联数据

    Args:
        db: 数据库会话
        report_id: 财报记录 ID

    Returns:
        FinancialReportDeleteResponse: 删除响应

    Raises:
        ServiceException: 记录不存在或删除失败
    """
    financial_report = _load_financial_report_or_raise(db, report_id)

    storage_path = financial_report.storage_path
    structured_json_path = financial_report.structured_json_path

    for table_name, model_class in FACT_MODEL_MAP.items():
        stmt = select(model_class).where(model_class.report_id == report_id)
        existing = db.execute(stmt).scalar_one_or_none()
        if existing is not None:
            db.delete(existing)

    db.delete(financial_report)

    try:
        db.commit()
        logger.info(
            "财报记录删除成功: report_id=%s stock_code=%s stock_abbr=%s",
            report_id,
            financial_report.stock_code,
            financial_report.stock_abbr,
        )
    except Exception as exc:
        db.rollback()
        logger.error(f"删除财报记录失败：{exc}", exc_info=True)
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "删除财报记录失败") from exc

    _cleanup_report_files(
        storage_path=storage_path,
        structured_json_path=structured_json_path,
    )

    return schemas_analysis_data.FinancialReportDeleteResponse(id=report_id)
