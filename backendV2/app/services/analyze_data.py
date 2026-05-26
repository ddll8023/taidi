"""财报分析数据服务"""

from fastapi import UploadFile, BackgroundTasks
from sqlalchemy.orm import Session
from app.db.database import commit_or_rollback
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.utils.file import save_file
import uuid
from app.core.config import settings
from app.models import financial_report as models_financial_report
from app.models import operation_log as models_operation_log
from app.models import (
    balance_sheet as models_balance_sheet,
    cash_flow_sheet as models_cash_flow_sheet,
    income_sheet as models_income_sheet,
    core_performance_indicators_sheet as models_core_performance_indicators_sheet,
)
from sqlalchemy import select, func
import os
import re
from langchain_community.document_loaders import PyPDFLoader
from pypdf import PdfReader
from app.schemas import analyze_data as schemas_analyze_data
from app.constants import (
    financial_report_base_info as constants_financial_report_base_info,
)
from app.models import company_basic_info as models_company_basic_info
from datetime import date
from app.schemas.common import PaginatedResponse, PaginationInfo
import math
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.db.database import get_background_db_session
from app.utils.model_factory import get_model
from app.constants import analyze_data as constants_analyze_data
from concurrent.futures import Future
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate

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
            file_name = f"{file.filename.removesuffix('.pdf')}-{uuid.uuid4().hex}.pdf"
            file_path = os.path.join(settings.FINCANCIAL_REPORT_UPLOAD_DIR, file_name)

            save_file(bytes_content, file_path)
            logger.info(f"文件 {file.filename} 已保存到 {file_path}")

            # 写入操作日志：处理中
            log_entity = models_operation_log.OperationLog(
                operation_type=models_operation_log.OPERATION_UPLOAD,
                operation_status=models_operation_log.STATUS_PROCESSING,
                source_file_name=file.filename,
                storage_path=file_path,
            )
            db.add(log_entity)
            db.flush()

            # 读取数据
            content_list = _read_report_data(file_path, 10)

            # 解析数据
            file_metadata_dict: dict = _parse_report_data("\n".join(content_list))
            if file_metadata_dict.get("stock_code") is None:
                file_metadata_dict = {
                    **file_metadata_dict,
                    **_extract_report_data_from_filename(db, file.filename),
                }
            file_metadata_item = schemas_analyze_data.FileMetadataItem(
                **file_metadata_dict
            )
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

            file_metadata_item.report_title = (
                company_basic_info_entity.stock_abbr
                + " "
                + file_metadata_item.report_title
            )

            # 校验报告ID是否已存在，如果存在则删除旧记录，更新新记录
            financial_report_entity = db.scalar(
                select(models_financial_report.FinancialReport).where(
                    models_financial_report.FinancialReport.stock_code
                    == file_metadata_item.stock_code,
                    models_financial_report.FinancialReport.report_period
                    == file_metadata_item.report_period,
                    models_financial_report.FinancialReport.report_type
                    == file_metadata_item.report_type,
                    models_financial_report.FinancialReport.report_year
                    == file_metadata_item.report_year,
                )
            )
            if financial_report_entity:
                # 删除旧记录
                try:
                    os.remove(financial_report_entity.storage_path)
                except FileNotFoundError:
                    logger.warning(
                        f"文件 {financial_report_entity.storage_path} 不存在，无需删除"
                    )
                db.delete(financial_report_entity)
                commit_or_rollback(db)
                logger.info(
                    f"财报记录删除成功: report_id={financial_report_entity.id} stock={file_metadata_item.stock_code} title={file_metadata_item.report_title}"
                )

            # 新增到数据库
            financial_report_new_entity = models_financial_report.FinancialReport(
                stock_code=file_metadata_item.stock_code,
                report_title=company_basic_info_entity.stock_abbr
                + " "
                + file_metadata_item.report_title,
                stock_abbr=company_basic_info_entity.stock_abbr,
                report_year=file_metadata_item.report_year,
                report_period=file_metadata_item.report_period,
                report_type=file_metadata_item.report_type,
                report_label=file_metadata_item.report_label,
                source_file_name=file.filename,
                storage_path=file_path,
                import_status=1,
                period_sort_key=constants_financial_report_base_info.PERIOD_SORT_KEY_MAP[
                    file_metadata_item.report_period
                ],
                exchange=company_basic_info_entity.exchange,
                source_priority=(
                    0
                    if file_metadata_item.report_type
                    == constants_financial_report_base_info.ReportTypeEnum.REPORT
                    else 1
                ),
            )
            db.add(financial_report_new_entity)
            db.flush()

            logger.info(
                f"财报记录入库成功: report_id={financial_report_new_entity.id} stock={file_metadata_item.stock_code} title={file_metadata_item.report_title}"
            )

            # 更新操作日志：成功
            log_entity.operation_status = models_operation_log.STATUS_SUCCESS
            log_entity.stock_code = file_metadata_item.stock_code
            log_entity.report_id = financial_report_new_entity.id

            # 成功计数
            success_count += 1
            success_reports.append(
                schemas_analyze_data.SuccessReportItem(
                    report_id=financial_report_new_entity.id,
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
            try:
                os.remove(file_path)
                logger.info(f"文件 {file_path} 删除成功")
            except FileNotFoundError:
                logger.warning(f"文件 {file_path} 不存在，无需删除")
            # 更新操作日志：失败
            if log_entity:
                log_entity.operation_status = models_operation_log.STATUS_FAILED
                log_entity.error_message = str(e)
                db.flush()

            logger.error(f"文件 {file.filename} 入库失败: {e}，错误信息已记录")

    commit_or_rollback(db)
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


def parse_report(
    db: Session,
    background_tasks: BackgroundTasks,
    parse_report_request: schemas_analyze_data.ParseReportRequest,
):
    """解析财报"""
    if len(parse_report_request.report_ids) == 0:
        raise ServiceException(
            ErrorCode.INVALID_REQUEST, "请提供要解析的财报记录ID列表"
        )
    logger.info(
        f"收到解析请求: report_count={len(parse_report_request.report_ids)} ids={parse_report_request.report_ids}"
    )
    start_parse_count = len(parse_report_request.report_ids)
    skip_report_ids: list[dict[int, str]] = []

    for report_id in parse_report_request.report_ids:
        report_entity = db.get(models_financial_report.FinancialReport, report_id)
        # 检查记录是否存在
        if report_entity is None:
            logger.warning(f"跳过解析: report_id={report_id} reason=未找到财报记录")
            skip_report_ids.append({"report_id": report_id, "reason": "未找到财报记录"})
            start_parse_count -= 1
            continue
        if not report_entity.storage_path:
            logger.error(f"财报缺少存储路径: report_id={report_id}")
            skip_report_ids.append({"report_id": report_id, "reason": "缺少存储路径"})
            start_parse_count -= 1
            continue
        # 更新解析状态为解析中
        report_entity.parse_status = (
            constants_financial_report_base_info.ParseStatusEnum.PARSING.value
        )
        logger.info(f"解析状态已更新: report_id={report_id} parse_status=PARSING")

    commit_or_rollback(db)

    # 提交到后台任务队列
    background_tasks.add_task(_run_parse_in_background, parse_report_request.report_ids)
    logger.info(
        f"解析任务已提交后台: total={len(parse_report_request.report_ids)} start={start_parse_count} skip={len(skip_report_ids)}"
    )

    return schemas_analyze_data.ParseReportResponse(
        total=len(parse_report_request.report_ids),
        skip_report_ids=skip_report_ids,
        start_parse_count=start_parse_count,
    )


def get_report_detail(
    db: Session, get_report_detail_request: schemas_analyze_data.GetReportDetailRequest
):
    """获取财报详情"""
    report_id = get_report_detail_request.report_id
    logger.info(f"查询财报详情: report_id={report_id}")

    report_entity = db.get(models_financial_report.FinancialReport, report_id)
    if report_entity is None:
        logger.warning(f"财报记录不存在: report_id={report_id}")
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "未找到财报记录")
    logger.info(
        f"财报基础信息查询完成: report_id={report_id} title={report_entity.report_title}"
    )

    get_report_detail_response_data = (
        schemas_analyze_data.GetReportDetailResponse.model_validate(report_entity)
    )

    core_performance_indicators_entity = db.get(
        models_core_performance_indicators_sheet.CorePerformanceIndicatorsSheet,
        report_id,
    )
    if core_performance_indicators_entity:
        get_report_detail_response_data.core_performance_indicators = schemas_analyze_data.StructCorePerformanceIndicatorsSheetItem.model_validate(
            core_performance_indicators_entity
        )
        logger.info(f"核心业绩指标查询完成: report_id={report_id}")

    balance_sheet_entity = db.get(
        models_balance_sheet.BalanceSheet,
        report_id,
    )
    if balance_sheet_entity:
        get_report_detail_response_data.balance_sheet = (
            schemas_analyze_data.StructBalanceSheetItem.model_validate(
                balance_sheet_entity
            )
        )
        logger.info(f"资产负债表查询完成: report_id={report_id}")

    income_sheet_entity = db.get(
        models_income_sheet.IncomeSheet,
        report_id,
    )
    if income_sheet_entity:
        get_report_detail_response_data.income_sheet = (
            schemas_analyze_data.StructIncomeSheetItem.model_validate(
                income_sheet_entity
            )
        )
        logger.info(f"利润表查询完成: report_id={report_id}")

    cash_flow_sheet_entity = db.get(
        models_cash_flow_sheet.CashFlowSheet,
        report_id,
    )
    if cash_flow_sheet_entity:
        get_report_detail_response_data.cash_flow_sheet = (
            schemas_analyze_data.StructCashFlowSheetItem.model_validate(
                cash_flow_sheet_entity
            )
        )
        logger.info(f"现金流量表查询完成: report_id={report_id}")

    logger.info(f"财报详情查询完成: report_id={report_id}")
    return get_report_detail_response_data


def delete_report(
    db: Session, delete_report_request: schemas_analyze_data.DeleteReportRequest
):
    """删除财报"""
    report_id = delete_report_request.report_id
    logger.info(f"删除财报: report_id={report_id}")

    report_entity = db.get(models_financial_report.FinancialReport, report_id)
    if report_entity is None:
        logger.warning(f"待删除财报记录不存在: report_id={report_id}")
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "未找到财报记录")
    db.delete(report_entity)
    commit_or_rollback(db)
    logger.info(
        f"财报删除成功: report_id={report_id} title={report_entity.report_title}"
    )
    return schemas_analyze_data.DeleteReportResponse(id=report_id)


"""辅助函数"""


def _read_report_data(file_path: str, max_page: int = 3):
    """读取PDF文件内容"""
    # 加载PDF文件
    try:
        reader = PdfReader(file_path)
        documents = reader.pages
    except Exception as e:
        logger.error(f"读取PDF文件 {file_path} 失败: {e}")
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "财报文件解析失败") from e
    content_list: list[str] = []

    # 防止超出文档范围 / 默认0页则读取全部页面
    if len(documents) < max_page or max_page == 0:
        max_page = len(documents)

    # 遍历文档，提取内容
    for document in documents[:max_page]:
        document_text = document.extract_text()
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

    if match:
        data["stock_code"] = str(match.group("stock_code")).zfill(6)
    else:
        logger.error(f"文件文本中未找到股票代码,需要查找文件名")

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

    return data


def _extract_report_data_from_filename(
    db: Session,
    filename: str,
):
    """从文件名中提取元数据"""
    data = {}
    filename = filename.removesuffix(".pdf")

    # 格式1：{6位股票代码}_{日期}_{随机码}.pdf — 上交所文件名格式
    match = re.match(
        r"^(?P<stock_code>\d{6})[_\s]" r"(?P<report_date>\d{8})[_\s]",
        filename,
    )
    if match:
        data["stock_code"] = str(match.group("stock_code")).zfill(6)
        data["report_date"] = date(
            int(match.group("report_date")[:4]),
            int(match.group("report_date")[4:6]),
            int(match.group("report_date")[6:]),
        )
        return data

    # 格式2：{公司简称}：{报告年份}年{报告期}报告.pdf — 深交所文件名格式
    match = re.match(
        r"^(?P<company_abbr>[^：:]+)\s*[：:]\s*",
        filename,
    )
    if not match:
        raise ServiceException(
            ErrorCode.DATA_NOT_FOUND, f"无法从文件名中解析元数据: {filename}"
        )

    # 根据公司简称查询股票代码
    company_abbr = match.group("company_abbr")
    company = db.scalar(
        select(models_company_basic_info.CompanyBasicInfo).where(
            models_company_basic_info.CompanyBasicInfo.stock_abbr == company_abbr
        )
    )
    if company is None:
        raise ServiceException(
            ErrorCode.DATA_NOT_FOUND,
            f"未找到公司简称 [{company_abbr}] 对应的股票代码",
        )
    data["stock_code"] = company.stock_code

    return data


def _run_parse_in_background(report_ids: list[int]):
    """在后台解析财报"""
    logger.info(f"后台解析任务启动: report_count={len(report_ids)} ids={report_ids}")

    async def _run_batch():
        sem = asyncio.Semaphore(5)  # 最多 5 个同时跑
        loop = asyncio.get_event_loop()

        async def _parse_one(report_id: int):
            async with sem:  # 拿不到信号量就等
                await loop.run_in_executor(
                    ThreadPoolExecutor(
                        max_workers=constants_analyze_data.MAX_PARSE_REPORT_WORKERS,
                        thread_name_prefix="parse_worker",
                    ),
                    _parse_single_report,  # 同步函数，在线程池里跑
                    report_id,
                )

        # 启动所有任务，但 Semaphore 会限制实际并发数
        await asyncio.gather(*[_parse_one(rid) for rid in report_ids])

    # asyncio.run() 启动事件循环（当前线程是 BackgroundTasks 的工作线程）
    asyncio.run(_run_batch())


def _parse_single_report(report_id: int):
    """在独立线程+独立DB会话中执行单个财报解析"""

    db = get_background_db_session()
    try:
        logger.info(f"开始解析财报: report_id={report_id}")

        # ────────── 1. 查询财报记录 ──────────
        report_entity = db.get(models_financial_report.FinancialReport, report_id)

        # ────────── 2. PDF 全文提取 ──────────
        logger.info(f"读取PDF: report_id={report_id} path={report_entity.storage_path}")
        content_text_list = _read_report_data(report_entity.storage_path, 0)
        content_text = "\n".join(content_text_list)

        # 3. LLM 抽取（四张表并行）
        table_results = _extract_tables_parallel(content_text)

        # 4. 转换为schemas格式
        core_performance_indicators_sheet_schemas = schemas_analyze_data.StructCorePerformanceIndicatorsSheetItem.model_validate(
            table_results["core_performance_indicators_sheet"]
        )
        balance_sheet_schemas = (
            schemas_analyze_data.StructBalanceSheetItem.model_validate(
                table_results["balance_sheet"]
            )
        )
        cash_flow_sheet_schemas = (
            schemas_analyze_data.StructCashFlowSheetItem.model_validate(
                table_results["cash_flow_sheet"]
            )
        )
        income_sheet_schemas = (
            schemas_analyze_data.StructIncomeSheetItem.model_validate(
                table_results["income_sheet"]
            )
        )

        # ────────── 5. 写入四张事实表 ──────────
        core_performance_indicators_sheet_entity = (
            models_core_performance_indicators_sheet.CorePerformanceIndicatorsSheet(
                report_id=report_entity.id,
                stock_code=report_entity.stock_code,
                stock_abbr=report_entity.stock_abbr,
                report_year=report_entity.report_year,
                report_period=report_entity.report_period,
                report_type=report_entity.report_type,
                **core_performance_indicators_sheet_schemas.model_dump(),
            )
        )
        balance_sheet_entity = models_balance_sheet.BalanceSheet(
            report_id=report_entity.id,
            stock_code=report_entity.stock_code,
            stock_abbr=report_entity.stock_abbr,
            report_year=report_entity.report_year,
            report_period=report_entity.report_period,
            report_type=report_entity.report_type,
            **balance_sheet_schemas.model_dump(),
        )
        cash_flow_sheet_entity = models_cash_flow_sheet.CashFlowSheet(
            report_id=report_entity.id,
            stock_code=report_entity.stock_code,
            stock_abbr=report_entity.stock_abbr,
            report_year=report_entity.report_year,
            report_period=report_entity.report_period,
            report_type=report_entity.report_type,
            **cash_flow_sheet_schemas.model_dump(),
        )
        income_sheet_entity = models_income_sheet.IncomeSheet(
            report_id=report_entity.id,
            stock_code=report_entity.stock_code,
            stock_abbr=report_entity.stock_abbr,
            report_year=report_entity.report_year,
            report_period=report_entity.report_period,
            report_type=report_entity.report_type,
            **income_sheet_schemas.model_dump(),
        )
        db.add(core_performance_indicators_sheet_entity)
        db.add(balance_sheet_entity)
        db.add(cash_flow_sheet_entity)
        db.add(income_sheet_entity)
        logger.info(f"四张事实表全部入库: report_id={report_entity.id}")
        commit_or_rollback(db)

        # ────────── 6. 标记成功 ──────────
        _mark_parse_success(db, report_entity)
        logger.info(f"财报解析完成: report_id={report_entity.id} parse_status=SUCCESS")

    except Exception as e:
        logger.error(f"解析系统异常: report_id={report_entity.id} error={e}")
        _mark_parse_failed(db, report_entity, str(e))
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "解析系统异常") from e
    finally:
        db.close()


def _mark_parse_success(
    db: Session, report_entity: models_financial_report.FinancialReport
):
    """标记解析成功"""
    if report_entity:
        report_entity.parse_status = (
            constants_financial_report_base_info.ParseStatusEnum.SUCCESS.value
        )  # = 1
        commit_or_rollback(db)


def _mark_parse_failed(
    db: Session, report_entity: models_financial_report.FinancialReport, message: str
):
    """标记解析失败"""
    if report_entity:
        report_entity.parse_status = (
            constants_financial_report_base_info.ParseStatusEnum.FAIL.value
        )  # = 2
        log_entity = models_operation_log.OperationLog(
            operation_type=models_operation_log.OPERATION_PARSE,
            operation_status=models_operation_log.STATUS_FAILED,
            source_file_name=report_entity.source_file_name,
            stock_code=report_entity.stock_code,
            report_id=report_entity.id,
            error_message=message,
        )
        db.add(log_entity)
    commit_or_rollback(db)


def _extract_single_table(table_name: str, context_text: str):
    """对单张表调用一次 LLM"""
    start_time = time.time()
    logger.info(f"LLM开始抽取: 数据表名={table_name}")

    # 1. 构建 Prompt
    prompt_template = PromptTemplate.from_template(
        settings.PROMPT_CONFIG.get_struct_config["shared_rules"]
        + settings.PROMPT_CONFIG.get_struct_config["table_prompts"][table_name]
    )

    # 2. 调用 LLM
    try:
        model = get_model.chat_model
        if model is None:
            raise ServiceException(ErrorCode.INTERNAL_ERROR, "LLM 模型未配置")
        chain = prompt_template | model | JsonOutputParser()
        result: dict = chain.invoke({"context_text": context_text})
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(
            f"LLM 调用异常: 数据表名={table_name} elapsed={elapsed:.1f}s error={e}"
        )
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "LLM 调用异常") from e

    elapsed = time.time() - start_time
    logger.info(
        f"LLM抽取完成: 数据表名={table_name} 字段数={len(result)} 时长={elapsed:.1f}s"
    )
    return result


def _extract_tables_parallel(context_text: str):
    """四张表同时调 LLM"""
    total_start = time.time()
    logger.info(f"开始LLM并行抽取: 数据表名={constants_analyze_data.TABLE_NAMES}")
    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures: dict[Future, str] = {}
        for table_name in constants_analyze_data.TABLE_NAMES:
            futures[
                executor.submit(_extract_single_table, table_name, context_text)
            ] = table_name
        for future in as_completed(futures):
            table_name = futures[future]
            result: dict = future.result()
            results[table_name] = result
            elapsed = time.time() - total_start
            logger.info(
                f"并行抽取单表完成: 数据表名={table_name} 字段数={len(result)} 时长={elapsed:.1f}s"
            )
    total_elapsed = time.time() - total_start
    logger.info(
        f"LLM并行抽取全部完成: 数据表名={list(results.keys())} 总时长={total_elapsed:.1f}s"
    )
    return results
