"""财报记录删除服务"""

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import financial_report as models_financial_report
from app.schemas import analysis_data as schemas_analysis_data
from app.constants import analysis_data as constants_analysis_data
from app.db.database import commit_or_rollback
from app.services.analysis_data.helpers import load_financial_report_or_raise, cleanup_report_files
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


# ========== 公共入口函数 ==========


def delete_financial_report(db: Session, report_id: int):
    """删除财报记录及其关联数据"""
    financial_report = load_financial_report_or_raise(db, report_id)

    storage_path = financial_report.storage_path
    structured_json_path = financial_report.structured_json_path

    for table_name, model_class in constants_analysis_data.FACT_MODEL_MAP.items():
        stmt = select(model_class).where(model_class.report_id == report_id)
        existing = db.execute(stmt).scalar_one_or_none()
        if existing is not None:
            db.delete(existing)

    db.delete(financial_report)

    commit_or_rollback(db)
    logger.info(
        f"财报记录删除成功: report_id={report_id} stock_code={financial_report.stock_code} stock_abbr={financial_report.stock_abbr}"
    )

    cleanup_report_files(
        storage_path=storage_path,
        structured_json_path=structured_json_path,
    )

    return schemas_analysis_data.FinancialReportDeleteResponse(id=report_id)
