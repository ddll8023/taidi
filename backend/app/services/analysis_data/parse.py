"""财报解析服务：提交解析任务、执行结构化抽取、状态管理"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, InvalidOperation
from typing import Any, NamedTuple

from sqlalchemy import Numeric, select
from sqlalchemy.orm import Session

from app.db.database import commit_or_rollback
from app.models import financial_report as models_financial_report
from app.models import validation_log as models_validation_log
from app.schemas import analysis_data as schemas_analysis_data
from app.schemas.common import ErrorCode
from app.schemas import financial_report as schemas_financial_report
from app.services import financial_report as services_financial_report
from app.services import structured_report_extraction as services_structured_report_extraction
from app.services import validation_log as services_validation_log
from app.constants import analysis_data as constants_analysis_data
from app.services.analysis_data.helpers import load_financial_report_or_raise, cleanup_report_files
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)

_parse_executor = ThreadPoolExecutor(
    max_workers=constants_analysis_data.MAX_CONCURRENT_PARSES, thread_name_prefix="parse_worker"
)


# ========== 公共入口函数 ==========


def submit_and_run_single_parse(
    db: Session, report_id: int, force: bool, background_tasks
):
    """提交单个财报解析任务并注册后台执行"""
    result = submit_single_parse(db, report_id, force)
    if result.status == "processing":
        background_tasks.add_task(run_parse_in_background, report_id)
    return result


def submit_and_run_batch_parse(
    db: Session, report_ids: list[int], background_tasks
):
    """提交批量解析任务并注册后台执行"""
    result = submit_batch_parse(db, report_ids)
    if result.submitted_report_ids:
        background_tasks.add_task(
            run_parse_batch_in_background, result.submitted_report_ids
        )
    return result


def submit_and_run_all_pending_parse(
    db: Session, limit: int, background_tasks
):
    """提交所有待处理财报解析任务并注册后台执行"""
    result = submit_all_pending_parse(db, limit)
    if result.submitted_report_ids:
        background_tasks.add_task(
            run_parse_batch_in_background, result.submitted_report_ids
        )
    return result


def submit_single_parse(
    db: Session, report_id: int, force: bool = False
):
    """提交单个财报解析任务"""
    financial_report = load_financial_report_or_raise(db, report_id)

    if not force and financial_report.parse_status == schemas_financial_report.ParseStatus.SUCCESS:
        return schemas_analysis_data.SingleParseSubmitResponse(
            report_id=report_id,
            status="already_success",
        )

    if financial_report.parse_status == schemas_financial_report.ParseStatus.PROCESSING:
        return schemas_analysis_data.SingleParseSubmitResponse(
            report_id=report_id,
            status="processing",
        )

    _update_parse_status_to_processing(db, report_id)

    return schemas_analysis_data.SingleParseSubmitResponse(
        report_id=report_id,
        status="processing",
    )


def submit_batch_parse(
    db: Session, report_ids: list[int]
):
    """提交批量解析任务"""
    valid_report_ids: list[int] = []
    skipped_report_ids: list[int] = []

    for report_id in report_ids:
        try:
            financial_report = load_financial_report_or_raise(db, report_id)
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
    )


def submit_all_pending_parse(
    db: Session, limit: int = 100
):
    """提交所有待处理财报的解析任务"""
    pending_reports = _get_pending_parse_reports(db, limit)
    report_ids = [r.id for r in pending_reports]

    if not report_ids:
        return schemas_analysis_data.BatchParseSubmitResponse(
            submitted_count=0,
            skipped_count=0,
            submitted_report_ids=[],
            skipped_report_ids=[],
        )

    for report_id in report_ids:
        _update_parse_status_to_processing(db, report_id)

    return schemas_analysis_data.BatchParseSubmitResponse(
        submitted_count=len(report_ids),
        skipped_count=0,
        submitted_report_ids=report_ids,
        skipped_report_ids=[],
    )


def get_batch_parse_status(
    db: Session, report_ids: list[int]
):
    """批量查询解析状态"""
    status_map = {
        schemas_financial_report.ParseStatus.PENDING: "pending",
        schemas_financial_report.ParseStatus.SUCCESS: "success",
        schemas_financial_report.ParseStatus.FAILED: "failed",
        schemas_financial_report.ParseStatus.PROCESSING: "processing",
    }

    results: list[schemas_analysis_data.BatchParseStatusItem] = []
    for report_id in report_ids:
        try:
            report = load_financial_report_or_raise(db, report_id)
            results.append(schemas_analysis_data.BatchParseStatusItem(
                report_id=report_id,
                parse_status=report.parse_status,
                parse_status_text=status_map.get(report.parse_status, "unknown"),
                validate_message=report.validate_message,
            ))
        except ServiceException:
            results.append(schemas_analysis_data.BatchParseStatusItem(
                report_id=report_id,
                parse_status=-1,
                parse_status_text="not_found",
                validate_message=None,
            ))

    processing_count = sum(
        1 for r in results if r.parse_status == schemas_financial_report.ParseStatus.PROCESSING
    )
    completed_count = sum(
        1 for r in results if r.parse_status in (
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


def parse_report(db: Session, report_id: int):
    """解析结构化并入库"""
    financial_report = load_financial_report_or_raise(db, report_id)

    if not financial_report.storage_path:
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            "财报记录缺少存储路径",
        )

    if financial_report.parse_status == schemas_financial_report.ParseStatus.SUCCESS:
        logger.info(f"财报已解析成功，跳过: report_id={report_id}")
        return True

    logger.info(
        f"开始解析财报: report_id={report_id} storage_path={financial_report.storage_path}"
    )

    extract_stage_log_id = services_validation_log.start_validation_stage(
        db=db,
        report=load_financial_report_or_raise(db, report_id),
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
        f"结构化抽取完成: report_id={report_id} structured_json_path={extraction.structured_json_path}"
    )

    validate_stage_log_id = services_validation_log.start_validation_stage(
        db=db,
        report=load_financial_report_or_raise(db, report_id),
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
        cleanup_report_files(structured_json_path=extraction.structured_json_path)
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
        cleanup_report_files(structured_json_path=extraction.structured_json_path)
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
        f"结构化结果规范化校验完成: report_id={report_id} fact_table_count={len(bundle.fact_records)}"
    )

    persist_stage_log_id = services_validation_log.start_validation_stage(
        db=db,
        report=load_financial_report_or_raise(db, report_id),
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

    financial_report = load_financial_report_or_raise(db, report_id)
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
        f"事实表同步完成: report_id={report_id} fact_tables={','.join(record.table_name for record in bundle.fact_records)}"
    )

    logger.info(f"财报解析完成: report_id={report_id}")
    commit_or_rollback(db)


def run_parse_in_background(report_id: int):
    """后台任务：执行单个财报解析"""
    from app.db.database import get_background_db_session

    db = get_background_db_session()
    try:
        logger.info(f"后台解析任务开始: report_id={report_id}")
        parse_report(db, report_id)
        logger.info(f"后台解析任务完成: report_id={report_id}")
    except ServiceException as e:
        logger.error(f"后台解析业务异常: report_id={report_id} error={e.message}")
        try:
            _update_parse_status_by_result(
                db, report_id, success=False, error_message=e.message
            )
        except Exception:
            pass
    except Exception as e:
        logger.error(
            f"后台解析系统异常: report_id={report_id} error={e}", exc_info=True
        )
        try:
            _update_parse_status_by_result(
                db, report_id, success=False, error_message="系统内部错误"
            )
        except Exception:
            pass
    finally:
        db.close()


def run_parse_batch_in_background(report_ids: list[int]):
    """后台任务：批量执行财报解析"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(_run_parse_batch_in_background_async(report_ids))
        else:
            loop.run_until_complete(_run_parse_batch_in_background_async(report_ids))
    except RuntimeError:
        asyncio.run(_run_parse_batch_in_background_async(report_ids))


"""辅助函数"""


def _update_financial_report_state(
    db: Session,
    report_id: int,
    *,
    structured_json_path: str | None | object = constants_analysis_data.UNSET,
    parse_status: schemas_financial_report.ParseStatus | int | object = constants_analysis_data.UNSET,
    validate_status: schemas_financial_report.ValidateStatus | int | object = constants_analysis_data.UNSET,
    validate_message: str | None | object = constants_analysis_data.UNSET,
    import_status: schemas_financial_report.ImportStatus | int | object = constants_analysis_data.UNSET,
):
    financial_report = load_financial_report_or_raise(db, report_id)
    if structured_json_path is not constants_analysis_data.UNSET:
        financial_report.structured_json_path = structured_json_path
    if parse_status is not constants_analysis_data.UNSET:
        financial_report.parse_status = parse_status
    if validate_status is not constants_analysis_data.UNSET:
        financial_report.validate_status = validate_status
    if validate_message is not constants_analysis_data.UNSET:
        financial_report.validate_message = validate_message
    if import_status is not constants_analysis_data.UNSET:
        financial_report.import_status = import_status
    return financial_report


def _update_parse_status_to_processing(
    db: Session, report_id: int
):
    """将解析状态更新为处理中"""
    financial_report = load_financial_report_or_raise(db, report_id)
    financial_report.parse_status = schemas_financial_report.ParseStatus.PROCESSING
    commit_or_rollback(db)
    db.refresh(financial_report)
    logger.info(f"解析状态已更新为处理中: report_id={report_id}")
    return financial_report


def _update_parse_status_by_result(
    db: Session, report_id: int, success: bool, error_message: str | None = None
):
    """根据解析结果更新解析状态"""
    financial_report = load_financial_report_or_raise(db, report_id)
    if success:
        financial_report.parse_status = schemas_financial_report.ParseStatus.SUCCESS
    else:
        financial_report.parse_status = schemas_financial_report.ParseStatus.FAILED
        if error_message:
            financial_report.validate_message = error_message
    commit_or_rollback(db)
    db.refresh(financial_report)
    return financial_report


def _get_pending_parse_reports(
    db: Session, limit: int = 100
):
    """查询待解析报告列表"""
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
    logger.info(f"获取待解析报告列表: count={len(results)} limit={limit}")
    return list(results)


def _parse_report_with_own_session(report_id: int):
    """在独立线程中执行解析"""
    from app.db.database import get_background_db_session

    db = get_background_db_session()
    try:
        logger.info(f"并行解析任务开始: report_id={report_id}")
        parse_report(db, report_id)
        logger.info(f"并行解析任务完成: report_id={report_id}")
        return (report_id, True, None)
    except ServiceException as e:
        logger.error(f"并行解析业务异常: report_id={report_id} error={e.message}")
        try:
            _update_parse_status_by_result(
                db, report_id, success=False, error_message=e.message
            )
        except Exception:
            pass
        return (report_id, False, e.message)
    except Exception as e:
        logger.error(
            f"并行解析系统异常: report_id={report_id} error={e}", exc_info=True
        )
        try:
            _update_parse_status_by_result(
                db, report_id, success=False, error_message="系统内部错误"
            )
        except Exception:
            pass
        return (report_id, False, "系统内部错误")
    finally:
        db.close()


async def _parse_single_async(report_id: int):
    """在线程池中异步执行单个解析任务"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _parse_executor, _parse_report_with_own_session, report_id
    )


async def _run_parse_batch_in_background_async(report_ids: list[int]):
    """异步并行批量执行财报解析"""
    logger.info(
        f"后台批量解析任务开始: report_ids={report_ids}, max_concurrent={constants_analysis_data.MAX_CONCURRENT_PARSES}"
    )

    semaphore = asyncio.Semaphore(constants_analysis_data.MAX_CONCURRENT_PARSES)

    async def parse_with_semaphore(rid: int):
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
            logger.error(f"并行解析任务异常: {result}")
        else:
            rid, success, error_message = result
            if success:
                success_count += 1
            else:
                failed_count += 1
                if error_message:
                    failed_messages[rid] = error_message

    logger.info(
        f"后台批量解析任务完成: total={len(report_ids)} success={success_count} failed={failed_count}"
    )

    return {
        "total": len(report_ids),
        "success_count": success_count,
        "failed_count": failed_count,
        "failed_messages": failed_messages,
    }


class _PreparedFactRecord(NamedTuple):
    table_name: str
    payload: dict[str, Any]


class _ReportPersistenceBundle(NamedTuple):
    financial_report: Any
    fact_records: tuple[_PreparedFactRecord, ...]


def _build_report_persistence_bundle(
    financial_report,
    normalized_records: dict[str, dict[str, Any]],
):
    """构建事实表持久化数据包"""
    report_identity = services_financial_report.build_report_fact_identity_payload(
        financial_report
    )
    prepared_fact_records = tuple(
        _PreparedFactRecord(
            table_name=table_name,
            payload=_build_fact_record_payload(
                constants_analysis_data.FACT_MODEL_MAP[table_name],
                record,
                report_identity,
            ),
        )
        for table_name, record in normalized_records.items()
    )
    logger.info(
        f"结构化结果已完成规范化校验: report_id={financial_report.id} fact_tables={','.join(record.table_name for record in prepared_fact_records)}"
    )
    return _ReportPersistenceBundle(
        financial_report=financial_report,
        fact_records=prepared_fact_records,
    )


def _persist_report_bundle(db: Session, bundle: _ReportPersistenceBundle):
    """同步写入主表与事实表数据"""
    prepared_map = {record.table_name: record for record in bundle.fact_records}
    report_id = bundle.financial_report.id

    for table_name, model_class in constants_analysis_data.FACT_MODEL_MAP.items():
        stmt = select(model_class).where(model_class.report_id == report_id)
        existing = db.execute(stmt).scalar_one_or_none()
        prepared = prepared_map.get(table_name)

        if prepared is None:
            if existing is not None:
                db.delete(existing)
            continue

        persistence_payload = dict(prepared.payload)
        persistence_payload["report_id"] = report_id

        if existing is None:
            db.add(model_class(**persistence_payload))
            continue

        for key, value in persistence_payload.items():
            setattr(existing, key, value)

    bundle.financial_report.import_status = (
        schemas_financial_report.ImportStatus.SUCCESS
    )
    logger.info(f"事实表入库数据已同步: report_id={report_id}")


def _build_fact_record_payload(
    model_class,
    record: dict[str, Any],
    report_identity: dict[str, Any],
):
    """构建单张事实表的入库数据"""
    if not isinstance(record, dict):
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR, "结构化结果中的记录必须是对象"
        )

    payload = {
        column.name: record.get(column.name)
        for column in model_class.__table__.columns
        if column.name not in constants_analysis_data.FACT_IDENTITY_FIELDS
    }
    payload.update(report_identity)
    return payload


def _normalize_structured_payload(
    payload: dict[str, list[dict[str, Any]]],
    use_full_pdf: bool = False,
):
    """将模型输出规范化为事实表可入库的单表单行快照"""
    if not isinstance(payload, dict):
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "结构化输出顶层必须是对象")

    expected_tables = set(constants_analysis_data.FACT_MODEL_MAP)
    actual_tables = set(payload)
    missing_tables = sorted(expected_tables - actual_tables)
    unexpected_tables = sorted(actual_tables - expected_tables)

    if missing_tables:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"结构化输出缺少表：{', '.join(missing_tables)}",
        )
    if unexpected_tables:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"结构化输出包含未定义表：{', '.join(unexpected_tables)}",
        )

    normalized_records: dict[str, dict[str, Any]] = {}
    empty_tables: list[str] = []
    for table_name in constants_analysis_data.FACT_MODEL_MAP:
        normalized_record = _normalize_single_table_record(
            table_name=table_name,
            raw_records=payload[table_name],
        )
        if normalized_record is None:
            empty_tables.append(table_name)
        else:
            normalized_records[table_name] = normalized_record

    if empty_tables:
        if use_full_pdf:
            logger.warning(
                f"使用全部PDF内容后仍有空表，可能是摘要版报告: empty_tables={','.join(empty_tables)}"
            )
        else:
            logger.warning(
                f"部分表返回空记录，允许继续处理: empty_tables={','.join(empty_tables)}"
            )

    return normalized_records


def _normalize_single_table_record(
    table_name: str,
    raw_records: Any,
):
    """校验并规范化单表的单条记录"""
    if not isinstance(raw_records, list):
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 的值必须是列表",
        )

    if not raw_records:
        return None

    if len(raw_records) > 1:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 只允许一条记录，当前返回 {len(raw_records)} 条",
        )

    raw_record = raw_records[0]
    if not isinstance(raw_record, dict):
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 的记录必须是对象",
        )

    record_fields = set(raw_record)
    identity_fields = sorted(
        record_fields & constants_analysis_data.FACT_IDENTITY_FIELDS
    )
    if identity_fields:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 不允许输出主表身份字段：{', '.join(identity_fields)}",
        )

    unexpected_fields = sorted(
        record_fields - constants_analysis_data.FACT_MODEL_FIELD_SET[table_name]
    )
    if unexpected_fields:
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name} 存在未定义字段：{', '.join(unexpected_fields)}",
        )

    normalized_record: dict[str, Any] = {}
    for column in constants_analysis_data.FACT_MODEL_COLUMNS[table_name]:
        normalized_record[column.name] = _normalize_metric_value(
            table_name=table_name,
            field_name=column.name,
            column=column,
            value=raw_record.get(column.name),
        )
    return normalized_record


def _normalize_metric_value(
    table_name: str,
    field_name: str,
    column,
    value: Any,
):
    """根据字段类型规范化单个指标值"""
    if value is None:
        return None

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        value = stripped

    if isinstance(column.type, Numeric):
        return _normalize_numeric_value(table_name, field_name, value)

    return value


def _normalize_numeric_value(table_name: str, field_name: str, value: Any):
    """将原始值规范化为 Decimal 数值类型"""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"{table_name}.{field_name} 必须是数值，不能是布尔值",
        )
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        normalized = value.replace(",", "").replace("，", "").strip()
        if normalized.endswith("%"):
            normalized = normalized[:-1].strip()
        if normalized.lower() in {"null", "none"}:
            return None
        try:
            return Decimal(normalized)
        except InvalidOperation as exc:
            raise ServiceException(
                ErrorCode.AI_SERVICE_ERROR,
                f"{table_name}.{field_name} 不是合法数值：{value}",
            ) from exc

    raise ServiceException(
        ErrorCode.AI_SERVICE_ERROR,
        f"{table_name}.{field_name} 的值类型不受支持：{type(value).__name__}",
    )
