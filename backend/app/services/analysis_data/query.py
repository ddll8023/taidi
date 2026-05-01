"""财报数据查询服务：列表查询、详情查询、JSON文件内容读取"""
import json
import math
import os
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import balance_sheet as models_balance_sheet
from app.models import cash_flow_sheet as models_cash_flow_sheet
from app.models import core_performance_indicators_sheet as models_core_performance_indicators_sheet
from app.models import financial_report as models_financial_report
from app.models import income_sheet as models_income_sheet
from app.schemas import analysis_data as schemas_analysis_data
from app.schemas.common import ErrorCode, PaginatedResponse, PaginationInfo
from app.services.analysis_data.helpers import load_financial_report_or_raise
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


# ========== 公共入口函数 ==========


def get_financial_report_list(
    db: Session, data_list_request: schemas_analysis_data.DataListRequest
):
    """获取财报数据列表"""
    base_stmt = select(models_financial_report.FinancialReport)

    if data_list_request.stock_code:
        base_stmt = base_stmt.where(
            models_financial_report.FinancialReport.stock_code
            == data_list_request.stock_code
        )
    if data_list_request.stock_abbr:
        base_stmt = base_stmt.where(
            models_financial_report.FinancialReport.stock_abbr
            == data_list_request.stock_abbr
        )
    if data_list_request.report_year:
        base_stmt = base_stmt.where(
            models_financial_report.FinancialReport.report_year
            == data_list_request.report_year
        )
    if data_list_request.report_period:
        try:
            normalized = models_financial_report.normalize_report_period(
                data_list_request.report_period
            )
            base_stmt = base_stmt.where(
                models_financial_report.FinancialReport.report_period == normalized
            )
        except ValueError:
            pass
    if data_list_request.report_type:
        try:
            normalized = models_financial_report.normalize_report_type(
                data_list_request.report_type
            )
            base_stmt = base_stmt.where(
                models_financial_report.FinancialReport.report_type == normalized
            )
        except ValueError:
            pass
    if data_list_request.import_status is not None:
        base_stmt = base_stmt.where(
            models_financial_report.FinancialReport.import_status
            == data_list_request.import_status
        )
    if data_list_request.keyword:
        keyword_pattern = f"%{data_list_request.keyword}%"
        base_stmt = base_stmt.where(
            models_financial_report.FinancialReport.source_file_name.ilike(
                keyword_pattern
            )
        )
    if data_list_request.parse_status is not None:
        base_stmt = base_stmt.where(
            models_financial_report.FinancialReport.parse_status
            == data_list_request.parse_status
        )
    if data_list_request.vector_status is not None:
        base_stmt = base_stmt.where(
            models_financial_report.FinancialReport.vector_status
            == data_list_request.vector_status
        )

    sort_by = data_list_request.sort_by or "created_at"
    sort_order = data_list_request.sort_order or "desc"

    sort_column = getattr(models_financial_report.FinancialReport, sort_by, None)
    if sort_column is None:
        sort_column = models_financial_report.FinancialReport.updated_at

    total = db.scalar(select(func.count()).select_from(base_stmt.subquery()))

    page = data_list_request.page
    page_size = data_list_request.page_size

    order_clause = sort_column.desc() if sort_order == "desc" else sort_column.asc()
    records = db.scalars(
        base_stmt.order_by(order_clause)
        .offset((page - 1) * page_size)
        .limit(page_size)
    ).all()

    items = [
        schemas_analysis_data.FinancialReportItemResponse(
            id=r.id,
            file_name=r.source_file_name,
            report_title=r.report_title,
            stock_code=r.stock_code,
            stock_abbr=r.stock_abbr,
            report_year=r.report_year,
            report_period=r.report_period,
            report_type=r.report_type,
            parse_status=r.parse_status,
            import_status=r.import_status,
            vector_status=r.vector_status,
            created_at=r.created_at,
        )
        for r in records
    ]

    return PaginatedResponse(
        lists=items,
        pagination=PaginationInfo(
            page=page,
            page_size=page_size,
            total=total,
            total_pages=math.ceil(total / page_size) if total else 0,
        ),
    )


def get_financial_report_detail(
    db: Session, report_id: int
):
    """获取单个财报详情"""
    financial_report = load_financial_report_or_raise(db, report_id)

    core_performance_indicators = None
    balance_sheet = None
    cash_flow_sheet = None
    income_sheet = None

    core_stmt = select(
        models_core_performance_indicators_sheet.CorePerformanceIndicatorsSheet
    ).where(
        models_core_performance_indicators_sheet.CorePerformanceIndicatorsSheet.report_id
        == report_id
    )
    core_result = db.execute(core_stmt).scalar_one_or_none()
    if core_result is not None:
        core_performance_indicators = (
            schemas_analysis_data.CorePerformanceIndicatorsData.model_validate(
                core_result
            )
        )

    balance_stmt = select(models_balance_sheet.BalanceSheet).where(
        models_balance_sheet.BalanceSheet.report_id == report_id
    )
    balance_result = db.execute(balance_stmt).scalar_one_or_none()
    if balance_result is not None:
        balance_sheet = schemas_analysis_data.BalanceSheetData.model_validate(
            balance_result
        )

    cash_flow_stmt = select(models_cash_flow_sheet.CashFlowSheet).where(
        models_cash_flow_sheet.CashFlowSheet.report_id == report_id
    )
    cash_flow_result = db.execute(cash_flow_stmt).scalar_one_or_none()
    if cash_flow_result is not None:
        cash_flow_sheet = schemas_analysis_data.CashFlowSheetData.model_validate(
            cash_flow_result
        )

    income_stmt = select(models_income_sheet.IncomeSheet).where(
        models_income_sheet.IncomeSheet.report_id == report_id
    )
    income_result = db.execute(income_stmt).scalar_one_or_none()
    if income_result is not None:
        income_sheet = schemas_analysis_data.IncomeSheetData.model_validate(
            income_result
        )

    return schemas_analysis_data.FinancialReportDetailResponse(
        id=financial_report.id,
        file_name=financial_report.source_file_name,
        report_title=financial_report.report_title,
        stock_code=financial_report.stock_code,
        stock_abbr=financial_report.stock_abbr,
        report_year=financial_report.report_year,
        report_period=financial_report.report_period,
        report_type=financial_report.report_type,
        report_label=financial_report.report_label,
        exchange=financial_report.exchange,
        report_date=financial_report.report_date,
        parse_status=financial_report.parse_status,
        review_status=financial_report.review_status,
        validate_status=financial_report.validate_status,
        validate_message=financial_report.validate_message,
        import_status=financial_report.import_status,
        vector_status=financial_report.vector_status,
        vector_model=financial_report.vector_model,
        vector_dim=financial_report.vector_dim,
        vector_version=financial_report.vector_version,
        vector_error_message=financial_report.vector_error_message,
        vectorized_at=financial_report.vectorized_at,
        created_at=financial_report.created_at,
        updated_at=financial_report.updated_at,
        core_performance_indicators=core_performance_indicators,
        balance_sheet=balance_sheet,
        cash_flow_sheet=cash_flow_sheet,
        income_sheet=income_sheet,
    )


def get_json_file_content(
    db: Session, report_id: int
):
    """获取结构化JSON文件内容"""
    financial_report = load_financial_report_or_raise(db, report_id)

    if not financial_report.structured_json_path:
        raise ServiceException(
            ErrorCode.DATA_NOT_FOUND,
            f"财报记录缺少结构化JSON路径，report_id={report_id}",
        )

    json_path = financial_report.structured_json_path
    if not os.path.exists(json_path):
        raise ServiceException(
            ErrorCode.DATA_NOT_FOUND,
            "JSON文件不存在",
        )

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            content = json.load(f)

        file_size = os.path.getsize(json_path)

        logger.info(
            f"获取JSON文件内容成功: report_id={report_id} file_size={file_size}"
        )

        return schemas_analysis_data.JsonContentResponse(
            file_name=os.path.basename(json_path),
            file_size=file_size,
            content=content,
        )
    except json.JSONDecodeError as exc:
        logger.error(f"JSON文件解析失败: {exc}", exc_info=True)
        raise ServiceException(
            ErrorCode.INTERNAL_ERROR,
            "JSON文件解析失败",
        ) from exc
    except Exception as exc:
        logger.error(f"读取JSON文件失败: {exc}", exc_info=True)
        raise ServiceException(
            ErrorCode.INTERNAL_ERROR,
            "读取JSON文件失败",
        ) from exc
