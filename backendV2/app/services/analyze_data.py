"""财报分析数据服务"""

from fastapi import UploadFile
from sqlalchemy.orm import Session
from app.db.database import commit_or_rollback
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.utils.file import save_file
import uuid
from app.core.config import settings
from app.models import financial_report as models_financial_report
from sqlalchemy import select, func
import os
import re
from langchain_community.document_loaders import PyPDFLoader
from app.schemas import analyze_data as schemas_analyze_data
from app.constants import (
    financial_report_base_info as constants_financial_report_base_info,
)
from app.models import company_basic_info as models_company_basic_info
from datetime import date
from app.schemas.common import PaginatedResponse, PaginationInfo
import math

logger = setup_logger(__name__)


def upload_report_file(db: Session, file_list: list[UploadFile]):
    """上传财报PDF文件"""
    if file_list is None or len(file_list) == 0:
        logger.error("上传文件列表为空")
        raise ServiceException(ErrorCode.PARAM_ERROR, "上传文件列表为空")

    success_count = 0
    failed_count = 0
    success_reports: list[schemas_analyze_data.SuccessReportItem] = []
    failed_files: list[schemas_analyze_data.FailedFileItem] = []
    logger.info(f"收到上传请求: 共 {len(file_list)} 个文件")
    # 遍历文件列表
    for file in file_list:
        file_path = None
        try:
            logger.info(f"开始处理文件: {file.filename}")
            # 校验文件类型
            if not file.filename.endswith(".pdf"):
                logger.error(f"文件 {file.filename} 不是PDF文件")
                raise ServiceException(
                    ErrorCode.UNSUPPORTED_FILE_FORMAT,
                    f"文件 {file.filename} 不是PDF文件",
                )
            # 保存文件
            bytes_content = file.file.read()
            file_name = f"{file.filename}-{uuid.uuid4().hex}"
            file_path = os.path.join(settings.FINCANCIAL_REPORT_UPLOAD_DIR, file_name)

            save_file(bytes_content, file_path)
            logger.info(f"文件 {file.filename} 已保存到 {file_path}")

            # 读取数据
            content_list = _read_report_data(file_path, 1)

            # 解析数据
            file_metadata_item = _parse_report_data("\n".join(content_list))
            logger.info(
                f"财报元数据解析完成: stock_code={file_metadata_item.stock_code} title={file_metadata_item.report_title}"
            )

            company_basic_info_entity = db.get(
                models_company_basic_info.CompanyBasicInfo,
                file_metadata_item.stock_code,
            )
            if not company_basic_info_entity:
                logger.error(
                    f"未找到股票代码 {file_metadata_item.stock_code} 对应的公司信息"
                )
                raise ServiceException(
                    ErrorCode.DATA_NOT_FOUND,
                    f"未找到股票代码 {file_metadata_item.stock_code} 对应的公司信息",
                )

            # 保存到数据库
            financial_report_entity = models_financial_report.FinancialReport(
                **file_metadata_item.model_dump(),
                period_sort_key=constants_financial_report_base_info.PERIOD_SORT_KEY_MAP[
                    file_metadata_item.report_period
                ],
                source_priority=(
                    0
                    if file_metadata_item.report_type
                    == constants_financial_report_base_info.ReportTypeEnum.REPORT
                    else 1
                ),
                source_file_name=file.filename,
                storage_path=file_path,
                import_status=1,
                stock_abbr=company_basic_info_entity.stock_abbr,
            )
            db.add(financial_report_entity)
            commit_or_rollback(db)
            db.refresh(financial_report_entity)
            logger.info(
                f"财报记录入库成功: report_id={financial_report_entity.id} stock={file_metadata_item.stock_code} title={file_metadata_item.report_title}"
            )

            # 成功计数
            success_count += 1
            success_reports.append(
                schemas_analyze_data.SuccessReportItem(
                    report_id=financial_report_entity.id,
                    stock_code=file_metadata_item.stock_code,
                    stock_abbr=company_basic_info_entity.stock_abbr,
                    report_title=file_metadata_item.report_title,
                    file_name=file.filename,
                )
            )

        except Exception as e:
            logger.error(f"处理文件 {file.filename} 时出错: {e}")
            failed_count += 1
            failed_files.append(
                schemas_analyze_data.FailedFileItem(
                    file_name=file.filename, error=str(e)
                )
            )

            # 删除文件（仅当文件已保存时）
            if file_path:
                try:
                    logger.info(f"删除临时文件: file={file_path}")
                    os.remove(file_path)
                except Exception as exc:
                    logger.error(
                        f"删除临时文件失败: file={file_path} error={exc}", exc_info=True
                    )

    logger.info(
        f"上传处理完成: total={len(file_list)} success={success_count} failed={failed_count}"
    )
    return schemas_analyze_data.UploadFileResponse(
        total=len(file_list),
        success_count=success_count,
        failed_count=failed_count,
        failed_files=failed_files,
        success_reports=success_reports,
    )


def get_report_list(
    db: Session, get_report_list_request: schemas_analyze_data.GetReportListRequest
):
    """获取财报列表"""
    logger.info(
        f"查询财报列表: page={get_report_list_request.page} page_size={get_report_list_request.page_size}"
    )
    base_stmt = select(models_financial_report.FinancialReport)

    # 动态添加筛选条件
    if get_report_list_request.keyword:
        base_stmt = base_stmt.where(
            models_financial_report.FinancialReport.report_title.like(
                f"%{get_report_list_request.keyword}%"
            )
        )
    if get_report_list_request.report_type is not None:
        base_stmt = base_stmt.where(
            models_financial_report.FinancialReport.report_type
            == get_report_list_request.report_type
        )
    if get_report_list_request.report_year is not None:
        base_stmt = base_stmt.where(
            models_financial_report.FinancialReport.report_year
            == get_report_list_request.report_year
        )
    if get_report_list_request.import_status is not None:
        base_stmt = base_stmt.where(
            models_financial_report.FinancialReport.import_status
            == get_report_list_request.import_status
        )
    if get_report_list_request.parse_status is not None:
        base_stmt = base_stmt.where(
            models_financial_report.FinancialReport.parse_status
            == get_report_list_request.parse_status
        )

    # 计算总数
    total = db.scalar(select(func.count()).select_from(base_stmt.subquery()))

    # 分页查询
    report_entity_list = db.scalars(
        base_stmt.order_by(
            models_financial_report.FinancialReport.updated_at.desc()
            if get_report_list_request.sort_order == "desc"
            else models_financial_report.FinancialReport.updated_at.asc()
        )
        .offset((get_report_list_request.page - 1) * get_report_list_request.page_size)
        .limit(get_report_list_request.page_size)
    ).all()

    logger.info(f"查询财报列表完成: total={total} page={get_report_list_request.page}")
    return PaginatedResponse(
        lists=[
            schemas_analyze_data.GetReportListResponse.model_validate(item)
            for item in report_entity_list
        ],
        pagination=PaginationInfo(
            page=get_report_list_request.page,
            page_size=get_report_list_request.page_size,
            total=total,
            total_pages=(
                math.ceil(total / get_report_list_request.page_size) if total else 0
            ),
        ),
    )


"""辅助函数"""


def _read_report_data(file_path: str, max_page: int = 3):
    """读取PDF文件内容"""
    # 加载PDF文件
    try:
        loader = PyPDFLoader(file_path)
        documents = loader.load()
    except Exception as e:
        logger.error(f"读取PDF文件 {file_path} 失败: {e}")
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "财报文件解析失败") from e
    content_list: list[str] = []

    # 遍历文档，提取内容
    for document in documents[:max_page]:
        document_text = document.page_content.strip()
        # 合并并规范化空白
        document_text = document_text.replace("\r", "\n")
        document_text = re.sub(r"[ \t]+", " ", document_text)  # 多个空格 → 一个空格
        document_text = re.sub(r"\n+", "\n", document_text)  # 多个换行 → 一个换行

        content_list.append(document_text)

    return content_list


def _parse_report_data(content_text: str):
    """解析财报PDF,提取元数据"""
    data: dict = {}

    # 提取股票代码
    match = re.search(
        re.compile(
            r"(?:证券代码|股票代码|公司代码|Stock\s+Code)\s*[:：]?\s*"
            r"(?P<stock_code>\d{6})",
            flags=re.IGNORECASE,
        ),
        content_text,
    )
    if match is None:
        logger.error("未找到股票代码")
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "未找到股票代码")
    data["stock_code"] = str(match.group("stock_code")).zfill(6)

    # 从文本中解析报告标题
    match = re.search(
        re.compile(
            r"(?P<title>"
            r"\s*(?P<report_year>20\d{2})\s*年\s*"
            r"(?P<report_label>第一季度报告|一季度报告|半年度报告\s*摘\s*要|"
            r"半年度报告|第三季度报告|三季度报告|年度报告\s*摘\s*要|年度报告)"
            r")",
            flags=re.IGNORECASE,
        ),
        content_text,
    )
    if match is None:
        logger.error("未找到报告标题")
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "未找到报告标题")

    data["report_title"] = str(match.group("title")).strip()

    report_label = re.sub(r"\s+", "", match.group("report_label"))
    if (
        report_label
        not in constants_financial_report_base_info.REPORT_LABEL_TO_META.keys()
    ):
        logger.error(f"未找到报告类型 {report_label}")
        raise ServiceException(
            ErrorCode.DATA_NOT_FOUND, f"未找到报告类型 {report_label}"
        )

    report_period, report_type, report_label = (
        constants_financial_report_base_info.REPORT_LABEL_TO_META[report_label]
    )

    data["report_period"] = report_period
    data["report_type"] = report_type
    data["report_label"] = report_label
    data["report_year"] = int(match.group("report_year"))

    # 提取显示日期
    match = re.search(
        re.compile(
            r"(?P<year>20\d{2})\s*[年/-]\s*(?P<month>\d{1,2})\s*[月/-]\s*(?P<day>\d{1,2})\s*日?",
            flags=re.IGNORECASE,
        ),
        content_text,
    )
    if match:
        data["report_date"] = date(
            int(match.group("year")), int(match.group("month")), int(match.group("day"))
        )

    return schemas_analyze_data.FileMetadataItem(**data)
