"""财报解析服务：提交解析任务、执行结构化抽取、状态管理"""
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import financial_report as models_financial_report
from app.models import validation_log as models_validation_log
from app.schemas import analysis_data as schemas_analysis_data
from app.schemas.common import ErrorCode
from app.schemas import financial_report as schemas_financial_report
from app.services import structured_report_extraction as services_structured_report_extraction
from app.services import validation_log as services_validation_log
from app.services.analysis_data._constants import UNSET, MAX_CONCURRENT_PARSES
from app.services.analysis_data.normalize import _normalize_structured_payload
from app.services.analysis_data.persist import _build_report_persistence_bundle, _persist_report_bundle
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)

_parse_executor = ThreadPoolExecutor(
    max_workers=MAX_CONCURRENT_PARSES, thread_name_prefix="parse_worker"
)


# ========== 公共入口函数 ==========


def submit_single_parse(
    db: Session, report_id: int, force: bool = False
) -> schemas_analysis_data.SingleParseSubmitResponse:
    """
    提交单个财报解析任务
    - 检查报告是否存在
    - 检查解析状态（已成功/处理中则跳过）
    - 更新状态为处理中
    - 返回提交结果

    Args:
        db: 数据库会话
        report_id: 财报ID
        force: 是否强制重新解析

    Returns:
        SingleParseSubmitResponse: 提交结果

    Raises:
        ServiceException: 报告不存在
    """
    financial_report = _load_financial_report_or_raise(db, report_id)

    if not force and financial_report.parse_status == schemas_financial_report.ParseStatus.SUCCESS:
        return schemas_analysis_data.SingleParseSubmitResponse(
            report_id=report_id,
            status="already_success",
            message="财报已解析成功，无需重复处理",
        )

    if financial_report.parse_status == schemas_financial_report.ParseStatus.PROCESSING:
        return schemas_analysis_data.SingleParseSubmitResponse(
            report_id=report_id,
            status="processing",
            message="财报正在解析中，请稍后查询状态",
        )

    _update_parse_status_to_processing(db, report_id)

    return schemas_analysis_data.SingleParseSubmitResponse(
        report_id=report_id,
        status="processing",
        message="解析任务已提交，请稍后刷新查看结果",
    )


def submit_batch_parse(
    db: Session, report_ids: list[int]
) -> schemas_analysis_data.BatchParseSubmitResponse:
    """
    提交批量解析任务
    - 遍历检查每个报告状态
    - 过滤已成功/处理中的报告
    - 更新待处理报告状态为处理中
    - 返回提交结果

    Args:
        db: 数据库会话
        report_ids: 报告ID列表

    Returns:
        BatchParseSubmitResponse: 批量提交结果
    """
    valid_report_ids: list[int] = []
    skipped_report_ids: list[int] = []

    for report_id in report_ids:
        try:
            financial_report = _load_financial_report_or_raise(db, report_id)
            if financial_report.parse_status in (
                schemas_financial_report.ParseStatus.SUCCESS,
                schemas_financial_report.ParseStatus.PROCESSING,
            ):
                skipped_report_ids.append(report_id)
            else:
                _update_parse_status_to_processing(db, report_id)
                valid_report_ids.append(report_id)
        except ServiceException:
            skipped_report_ids.append(report_id)

    return schemas_analysis_data.BatchParseSubmitResponse(
        submitted_count=len(valid_report_ids),
        skipped_count=len(skipped_report_ids),
        submitted_report_ids=valid_report_ids,
        skipped_report_ids=skipped_report_ids,
        message=f"已提交 {len(valid_report_ids)} 个解析任务",
    )


def submit_all_pending_parse(
    db: Session, limit: int = 100
) -> schemas_analysis_data.BatchParseSubmitResponse:
    """
    提交所有待处理财报的解析任务
    - 查询所有待解析报告
    - 更新状态为处理中
    - 返回提交结果

    Args:
        db: 数据库会话
        limit: 最大处理数量

    Returns:
        BatchParseSubmitResponse: 批量提交结果
    """
    pending_reports = _get_pending_parse_reports(db, limit)
    report_ids = [r.id for r in pending_reports]

    if not report_ids:
        return schemas_analysis_data.BatchParseSubmitResponse(
            submitted_count=0,
            skipped_count=0,
            submitted_report_ids=[],
            skipped_report_ids=[],
            message="没有待处理的财报",
        )

    for report_id in report_ids:
        _update_parse_status_to_processing(db, report_id)

    return schemas_analysis_data.BatchParseSubmitResponse(
        submitted_count=len(report_ids),
        skipped_count=0,
        submitted_report_ids=report_ids,
        skipped_report_ids=[],
        message=f"已提交 {len(report_ids)} 个解析任务",
    )


def get_batch_parse_status(
    db: Session, report_ids: list[int]
) -> schemas_analysis_data.BatchParseStatusResponse:
    """
    批量查询解析状态

    Args:
        db: 数据库会话
        report_ids: 报告ID列表

    Returns:
        BatchParseStatusResponse: 批量状态查询结果
    """
    status_map = {
        schemas_financial_report.ParseStatus.PENDING: "pending",
        schemas_financial_report.ParseStatus.SUCCESS: "success",
        schemas_financial_report.ParseStatus.FAILED: "failed",
        schemas_financial_report.ParseStatus.PROCESSING: "processing",
    }

    results: dict[int, schemas_analysis_data.BatchParseStatusItem] = {}
    for report_id in report_ids:
        try:
            report = _load_financial_report_or_raise(db, report_id)
            results[report_id] = schemas_analysis_data.BatchParseStatusItem(
                parse_status=report.parse_status,
                parse_status_text=status_map.get(report.parse_status, "unknown"),
                validate_message=report.validate_message,
            )
        except ServiceException:
            results[report_id] = schemas_analysis_data.BatchParseStatusItem(
                parse_status=-1,
                parse_status_text="not_found",
                validate_message=None,
            )

    processing_count = sum(
        1 for r in results.values() if r.parse_status == schemas_financial_report.ParseStatus.PROCESSING
    )
    completed_count = sum(
        1 for r in results.values() if r.parse_status in (
            schemas_financial_report.ParseStatus.SUCCESS,
            schemas_financial_report.ParseStatus.FAILED,
        )
    )

    return schemas_analysis_data.BatchParseStatusResponse(
        results=results,
        processing_count=processing_count,
        completed_count=completed_count,
        total_count=len(report_ids),
    )


def parse_report(db: Session, report_id: int) -> bool:
    """
    阶段二：解析结构化并入库
    - 从 financial_report 获取 storage_path
    - 执行结构化抽取
    - 执行规范化校验
    - 执行事实表入库
    - 更新 parse_status 和 import_status
    - 返回处理结果

    Args:
        db: 数据库会话
        report_id: 财报ID

    Returns:
        bool: 解析是否成功

    Raises:
        ServiceException: 解析失败
    """
    financial_report = _load_financial_report_or_raise(db, report_id)

    if not financial_report.storage_path:
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            f"财报记录缺少存储路径，report_id={report_id}",
        )

    if financial_report.parse_status == schemas_financial_report.ParseStatus.SUCCESS:
        logger.info("财报已解析成功，跳过: report_id=%s", report_id)
        return True

    logger.info(
        "开始解析财报: report_id=%s storage_path=%s",
        report_id,
        financial_report.storage_path,
    )

    extract_stage_log_id = services_validation_log.start_validation_stage(
        db=db,
        report=_load_financial_report_or_raise(db, report_id),
        stage=models_validation_log.VALIDATION_STAGE_STRUCTURED_EXTRACT,
        check_type=models_validation_log.VALIDATION_CHECK_TYPE_PIPELINE,
        message="开始抽取四张事实表结构化结果",
        details={"storage_path": financial_report.storage_path},
    )
    try:
        extraction = services_structured_report_extraction.extract_structured_report(
            file_path=financial_report.storage_path,
            financial_report=financial_report,
        )
        financial_report = _update_financial_report_state(
            db,
            report_id,
            structured_json_path=extraction.structured_json_path,
        )
    except ServiceException as exc:
        db.rollback()
        financial_report = _update_financial_report_state(
            db,
            report_id,
            parse_status=schemas_financial_report.ParseStatus.FAILED,
        )
        services_validation_log.mark_validation_stage_failed(
            db=db,
            log_id=extract_stage_log_id,
            report=financial_report,
            message=exc.message,
            details=services_validation_log.build_validation_failure_details(
                exc,
                {"storage_path": financial_report.storage_path},
            ),
            error_code=services_validation_log.get_service_error_code(exc),
            check_type=models_validation_log.VALIDATION_CHECK_TYPE_PIPELINE,
            is_blocking=True,
        )
        raise
    except Exception as exc:
        db.rollback()
        logger.error(f"结构化抽取异常：{exc}", exc_info=True)
        financial_report = _update_financial_report_state(
            db,
            report_id,
            parse_status=schemas_financial_report.ParseStatus.FAILED,
        )
        message = "结构化抽取失败"
        services_validation_log.mark_validation_stage_failed(
            db=db,
            log_id=extract_stage_log_id,
            report=financial_report,
            message=message,
            details=services_validation_log.build_validation_failure_details(
                exc,
                {"storage_path": financial_report.storage_path},
            ),
            error_code=services_validation_log.get_service_error_code(exc),
            check_type=models_validation_log.VALIDATION_CHECK_TYPE_PIPELINE,
            is_blocking=True,
        )
        raise ServiceException(ErrorCode.INTERNAL_ERROR, message) from exc

    services_validation_log.mark_validation_stage_passed(
        db=db,
        log_id=extract_stage_log_id,
        report=financial_report,
        message="结构化抽取完成",
        details={
            "storage_path": financial_report.storage_path,
            "structured_json_path": extraction.structured_json_path,
            "table_contexts": extraction.trace.get("table_contexts", {}),
            "table_results": extraction.trace.get("table_results", {}),
        },
    )
    logger.info(
        "结构化抽取完成: report_id=%s structured_json_path=%s",
        report_id,
        extraction.structured_json_path,
    )

    validate_stage_log_id = services_validation_log.start_validation_stage(
        db=db,
        report=_load_financial_report_or_raise(db, report_id),
        stage=models_validation_log.VALIDATION_STAGE_STRUCTURED_VALIDATE,
        check_type=models_validation_log.VALIDATION_CHECK_TYPE_STRUCT_SCHEMA,
        message="开始规范化校验结构化结果",
        details={"structured_json_path": extraction.structured_json_path},
    )
    try:
        normalized_records = _normalize_structured_payload(
            extraction.payload, extraction.use_full_pdf
        )
        financial_report = _update_financial_report_state(
            db,
            report_id,
            parse_status=schemas_financial_report.ParseStatus.SUCCESS,
            validate_status=schemas_financial_report.ValidateStatus.PASSED,
            validate_message=None,
            import_status=schemas_financial_report.ImportStatus.PENDING,
        )
        bundle = _build_report_persistence_bundle(
            financial_report=financial_report,
            normalized_records=normalized_records,
        )
    except ServiceException as exc:
        db.rollback()
        financial_report = _update_financial_report_state(
            db,
            report_id,
            parse_status=schemas_financial_report.ParseStatus.FAILED,
            validate_status=schemas_financial_report.ValidateStatus.FAILED,
            validate_message=exc.message,
            import_status=schemas_financial_report.ImportStatus.PENDING,
        )
        _cleanup_report_files(structured_json_path=extraction.structured_json_path)
        services_validation_log.mark_validation_stage_failed(
            db=db,
            log_id=validate_stage_log_id,
            report=financial_report,
            message=exc.message,
            details=services_validation_log.build_validation_failure_details(
                exc,
                {"structured_json_path": extraction.structured_json_path},
            ),
            error_code=services_validation_log.get_service_error_code(exc),
            check_type=services_validation_log.infer_structured_validation_check_type(
                exc.message
            ),
            is_blocking=True,
        )
        raise
    except Exception as exc:
        db.rollback()
        logger.error(f"结构化结果规范化校验异常：{exc}", exc_info=True)
        message = "结构化结果规范化校验失败"
        financial_report = _update_financial_report_state(
            db,
            report_id,
            parse_status=schemas_financial_report.ParseStatus.FAILED,
            validate_status=schemas_financial_report.ValidateStatus.FAILED,
            validate_message=message,
            import_status=schemas_financial_report.ImportStatus.PENDING,
        )
        _cleanup_report_files(structured_json_path=extraction.structured_json_path)
        services_validation_log.mark_validation_stage_failed(
            db=db,
            log_id=validate_stage_log_id,
            report=financial_report,
            message=message,
            details=services_validation_log.build_validation_failure_details(
                exc,
                {"structured_json_path": extraction.structured_json_path},
            ),
            error_code=services_validation_log.get_service_error_code(exc),
            check_type=models_validation_log.VALIDATION_CHECK_TYPE_STRUCT_SCHEMA,
            is_blocking=True,
        )
        raise ServiceException(ErrorCode.INTERNAL_ERROR, message) from exc

    services_validation_log.mark_validation_stage_passed(
        db=db,
        log_id=validate_stage_log_id,
        report=financial_report,
        message="结构化结果规范化校验通过",
        details={
            "structured_json_path": extraction.structured_json_path,
            "fact_tables": [record.table_name for record in bundle.fact_records],
            "fact_table_count": len(bundle.fact_records),
        },
    )
    logger.info(
        "结构化结果规范化校验完成: report_id=%s fact_table_count=%d",
        report_id,
        len(bundle.fact_records),
    )

    persist_stage_log_id = services_validation_log.start_validation_stage(
        db=db,
        report=_load_financial_report_or_raise(db, report_id),
        stage=models_validation_log.VALIDATION_STAGE_FACT_PERSIST,
        check_type=models_validation_log.VALIDATION_CHECK_TYPE_FACT_SYNC,
        message="开始同步事实表快照",
        details={"fact_tables": [record.table_name for record in bundle.fact_records]},
    )
    try:
        _persist_report_bundle(db, bundle)
    except ServiceException as exc:
        db.rollback()
        financial_report = _update_financial_report_state(
            db,
            report_id,
            import_status=schemas_financial_report.ImportStatus.FAILED,
        )
        services_validation_log.mark_validation_stage_failed(
            db=db,
            log_id=persist_stage_log_id,
            report=financial_report,
            message=exc.message,
            details=services_validation_log.build_validation_failure_details(
                exc,
                {"fact_tables": [record.table_name for record in bundle.fact_records]},
            ),
            error_code=services_validation_log.get_service_error_code(exc),
            check_type=models_validation_log.VALIDATION_CHECK_TYPE_FACT_SYNC,
            is_blocking=True,
        )
        raise
    except Exception as exc:
        db.rollback()
        logger.error(f"事实表同步异常：{exc}", exc_info=True)
        message = "事实表同步失败"
        financial_report = _update_financial_report_state(
            db,
            report_id,
            import_status=schemas_financial_report.ImportStatus.FAILED,
        )
        services_validation_log.mark_validation_stage_failed(
            db=db,
            log_id=persist_stage_log_id,
            report=financial_report,
            message=message,
            details=services_validation_log.build_validation_failure_details(
                exc,
                {"fact_tables": [record.table_name for record in bundle.fact_records]},
            ),
            error_code=services_validation_log.get_service_error_code(exc),
            check_type=models_validation_log.VALIDATION_CHECK_TYPE_FACT_SYNC,
            is_blocking=True,
        )
        raise ServiceException(ErrorCode.INTERNAL_ERROR, message) from exc

    financial_report = _load_financial_report_or_raise(db, report_id)
    services_validation_log.mark_validation_stage_passed(
        db=db,
        log_id=persist_stage_log_id,
        report=financial_report,
        message="事实表快照同步完成",
        details={
            "report_id": report_id,
            "fact_tables": [record.table_name for record in bundle.fact_records],
            "fact_table_count": len(bundle.fact_records),
        },
    )
    logger.info(
        "事实表同步完成: report_id=%s fact_tables=%s",
        report_id,
        ",".join(record.table_name for record in bundle.fact_records),
    )

    logger.info("财报解析完成: report_id=%s", report_id)
    try:
        db.commit()
    except Exception:
        db.rollback()
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "操作失败")
    return True


def run_parse_in_background(report_id: int) -> None:
    """
    后台任务：执行财报解析（单个）
    使用独立的数据库会话，确保与请求会话隔离

    Args:
        report_id: 财报ID
    """
    from app.db.database import get_background_db_session

    db = get_background_db_session()
    try:
        logger.info("后台解析任务开始: report_id=%s", report_id)
        parse_report(db, report_id)
        logger.info("后台解析任务完成: report_id=%s", report_id)
    except ServiceException as e:
        logger.error("后台解析业务异常: report_id=%s error=%s", report_id, e.message)
        try:
            _update_parse_status_by_result(
                db, report_id, success=False, error_message=e.message
            )
        except Exception:
            pass
    except Exception as e:
        logger.error(
            "后台解析系统异常: report_id=%s error=%s", report_id, e, exc_info=True
        )
        try:
            _update_parse_status_by_result(
                db, report_id, success=False, error_message=str(e)
            )
        except Exception:
            pass
    finally:
        db.close()


def run_parse_batch_in_background(report_ids: list[int]) -> None:
    """
    后台任务：批量执行财报解析（同步包装，供 BackgroundTasks 调用）

    Args:
        report_ids: 报告ID列表
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(_run_parse_batch_in_background_async(report_ids))
        else:
            loop.run_until_complete(_run_parse_batch_in_background_async(report_ids))
    except RuntimeError:
        asyncio.run(_run_parse_batch_in_background_async(report_ids))


"""辅助函数"""


def _load_financial_report_or_raise(
    db: Session, report_id: int
) -> models_financial_report.FinancialReport:
    financial_report = db.get(models_financial_report.FinancialReport, report_id)
    if financial_report is None:
        raise ServiceException(
            ErrorCode.DATA_NOT_FOUND,
            f"financial_report 不存在，report_id={report_id}",
        )
    return financial_report


def _update_financial_report_state(
    db: Session,
    report_id: int,
    *,
    structured_json_path: str | None | object = UNSET,
    parse_status: schemas_financial_report.ParseStatus | int | object = UNSET,
    validate_status: schemas_financial_report.ValidateStatus | int | object = UNSET,
    validate_message: str | None | object = UNSET,
    import_status: schemas_financial_report.ImportStatus | int | object = UNSET,
) -> models_financial_report.FinancialReport:
    financial_report = _load_financial_report_or_raise(db, report_id)
    if structured_json_path is not UNSET:
        financial_report.structured_json_path = structured_json_path
    if parse_status is not UNSET:
        financial_report.parse_status = parse_status
    if validate_status is not UNSET:
        financial_report.validate_status = validate_status
    if validate_message is not UNSET:
        financial_report.validate_message = validate_message
    if import_status is not UNSET:
        financial_report.import_status = import_status
    return financial_report


def _update_parse_status_to_processing(
    db: Session, report_id: int
) -> models_financial_report.FinancialReport:
    """将解析状态更新为"处理中" """
    financial_report = _load_financial_report_or_raise(db, report_id)
    financial_report.parse_status = schemas_financial_report.ParseStatus.PROCESSING
    db.commit()
    db.refresh(financial_report)
    logger.info("解析状态已更新为处理中: report_id=%s", report_id)
    return financial_report


def _update_parse_status_by_result(
    db: Session, report_id: int, success: bool, error_message: str | None = None
) -> models_financial_report.FinancialReport:
    """根据解析结果更新状态"""
    financial_report = _load_financial_report_or_raise(db, report_id)
    if success:
        financial_report.parse_status = schemas_financial_report.ParseStatus.SUCCESS
    else:
        financial_report.parse_status = schemas_financial_report.ParseStatus.FAILED
        if error_message:
            financial_report.validate_message = error_message
    db.commit()
    db.refresh(financial_report)
    return financial_report


def _get_pending_parse_reports(
    db: Session, limit: int = 100
) -> list[models_financial_report.FinancialReport]:
    """获取待解析的报告列表"""
    stmt = (
        select(models_financial_report.FinancialReport)
        .where(
            models_financial_report.FinancialReport.parse_status
            == schemas_financial_report.ParseStatus.PENDING
        )
        .order_by(models_financial_report.FinancialReport.created_at.asc())
        .limit(limit)
    )
    results = db.execute(stmt).scalars().all()
    logger.info("获取待解析报告列表: count=%d limit=%d", len(results), limit)
    return list(results)


def _cleanup_report_files(
    storage_path: str | None = None,
    structured_json_path: str | None = None,
) -> None:
    """清理财报相关文件"""
    if storage_path:
        try:
            if os.path.exists(storage_path):
                os.remove(storage_path)
                logger.info("已清理PDF文件: %s", storage_path)
        except Exception as exc:
            logger.warning("清理PDF文件失败: %s, 错误: %s", storage_path, exc)

    if structured_json_path:
        try:
            if os.path.exists(structured_json_path):
                os.remove(structured_json_path)
                logger.info("已清理JSON文件: %s", structured_json_path)
        except Exception as exc:
            logger.warning("清理JSON文件失败: %s, 错误: %s", structured_json_path, exc)


def _parse_report_with_own_session(report_id: int) -> tuple[int, bool, str | None]:
    """在独立线程中执行解析，使用独立的数据库会话"""
    from app.db.database import get_background_db_session

    db = get_background_db_session()
    try:
        logger.info("并行解析任务开始: report_id=%s", report_id)
        parse_report(db, report_id)
        logger.info("并行解析任务完成: report_id=%s", report_id)
        return (report_id, True, None)
    except ServiceException as e:
        logger.error("并行解析业务异常: report_id=%s error=%s", report_id, e.message)
        try:
            _update_parse_status_by_result(
                db, report_id, success=False, error_message=e.message
            )
        except Exception:
            pass
        return (report_id, False, e.message)
    except Exception as e:
        logger.error(
            "并行解析系统异常: report_id=%s error=%s", report_id, e, exc_info=True
        )
        try:
            _update_parse_status_by_result(
                db, report_id, success=False, error_message=str(e)
            )
        except Exception:
            pass
        return (report_id, False, str(e))
    finally:
        db.close()


async def _parse_single_async(report_id: int) -> tuple[int, bool, str | None]:
    """异步包装：在线程池中执行单个解析任务"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _parse_executor, _parse_report_with_own_session, report_id
    )


async def _run_parse_batch_in_background_async(report_ids: list[int]) -> dict:
    """后台任务：批量执行财报解析（异步并行，最大并发数为 MAX_CONCURRENT_PARSES）"""
    logger.info(
        "后台批量解析任务开始: report_ids=%s, max_concurrent=%d",
        report_ids,
        MAX_CONCURRENT_PARSES,
    )

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_PARSES)

    async def parse_with_semaphore(rid: int) -> tuple[int, bool, str | None]:
        async with semaphore:
            return await _parse_single_async(rid)

    tasks = [parse_with_semaphore(rid) for rid in report_ids]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    success_count = 0
    failed_count = 0
    failed_messages: dict[int, str] = {}

    for result in results:
        if isinstance(result, Exception):
            failed_count += 1
            logger.error("并行解析任务异常: %s", result)
        else:
            rid, success, error_message = result
            if success:
                success_count += 1
            else:
                failed_count += 1
                if error_message:
                    failed_messages[rid] = error_message

    logger.info(
        "后台批量解析任务完成: total=%d success=%d failed=%d",
        len(report_ids),
        success_count,
        failed_count,
    )

    return {
        "total": len(report_ids),
        "success_count": success_count,
        "failed_count": failed_count,
        "failed_messages": failed_messages,
    }
