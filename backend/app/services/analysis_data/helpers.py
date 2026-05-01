"""analysis_data 模块跨文件共享辅助函数"""
import os

from sqlalchemy.orm import Session

from app.models import financial_report as models_financial_report
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


"""辅助函数"""


def load_financial_report_or_raise(
    db: Session, report_id: int
):
    """根据 ID 加载财报记录，不存在则抛异常"""
    financial_report = db.get(models_financial_report.FinancialReport, report_id)
    if financial_report is None:
        raise ServiceException(
            ErrorCode.DATA_NOT_FOUND,
            "财报记录不存在",
        )
    return financial_report


def cleanup_report_files(
    storage_path: str | None = None,
    structured_json_path: str | None = None,
):
    """清理财报相关文件"""
    if storage_path:
        try:
            if os.path.exists(storage_path):
                os.remove(storage_path)
                logger.info(f"已清理PDF文件: {storage_path}")
        except Exception as exc:
            logger.warning(f"清理PDF文件失败: {storage_path}, 错误: {exc}")

    if structured_json_path:
        try:
            if os.path.exists(structured_json_path):
                os.remove(structured_json_path)
                logger.info(f"已清理JSON文件: {structured_json_path}")
        except Exception as exc:
            logger.warning(f"清理JSON文件失败: {structured_json_path}, 错误: {exc}")
