from fastapi import APIRouter, Depends, Query, File, Path, Body, UploadFile, BackgroundTasks
from typing import Annotated
from sqlalchemy.orm import Session
from app.db.database import get_db
from app.services import analysis_data as services_analysis_data
from app.services import company_basic_info as services_company_basic_info
from app.schemas.response import success, error
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.schemas import analysis_data as schemas_analysis_data

router = APIRouter(prefix="/api/v1/data", tags=["数据上传处理"])
logger = setup_logger(__name__)


@router.post("/upload")
async def upload_data(
    db: Annotated[Session, Depends(get_db)], file: Annotated[UploadFile, File()]
):
    """上传PDF文件，仅执行建档入库（阶段一）"""
    try:
        result = await services_analysis_data.upload_archive_only(db, file)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"上传异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/upload/batch")
async def upload_batch_data(
    db: Annotated[Session, Depends(get_db)],
    files: Annotated[list[UploadFile], File(description="PDF文件列表")]
):
    """批量上传PDF文件，仅执行建档入库（阶段一）"""
    try:
        if not files or len(files) == 0:
            return error(code=ErrorCode.PARAM_ERROR, message="请选择至少一个文件")
        result = await services_analysis_data.upload_archive_batch(db, files)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"批量上传异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/parse/batch")
async def parse_batch_reports(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    request: Annotated[schemas_analysis_data.BatchParseRequest, Body()]
):
    """提交批量解析任务（异步）"""
    try:
        result = services_analysis_data.submit_batch_parse(db, request.report_ids)

        if result.submitted_report_ids:
            background_tasks.add_task(
                services_analysis_data.run_parse_batch_in_background,
                result.submitted_report_ids,
            )

        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"提交批量解析任务异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/parse/{report_id}")
async def parse_single_report(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    report_id: Annotated[int, Path(description="财报ID")],
    force: Annotated[bool, Query(description="强制重新解析（包括已解析成功的）")] = False
):
    """提交单个财报解析任务（异步）"""
    try:
        result = services_analysis_data.submit_single_parse(db, report_id, force)

        if result.status == "processing":
            background_tasks.add_task(
                services_analysis_data.run_parse_in_background,
                report_id,
            )

        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"提交解析任务异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/parse/all")
async def parse_all_pending_reports(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    limit: Annotated[int, Query(description="最大处理数量")] = 100
):
    """提交所有待处理财报的解析任务（异步）"""
    try:
        result = services_analysis_data.submit_all_pending_parse(db, limit)

        if result.submitted_report_ids:
            background_tasks.add_task(
                services_analysis_data.run_parse_batch_in_background,
                result.submitted_report_ids,
            )

        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"提交一键解析任务异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/parse/status/batch")
async def get_batch_parse_status(
    db: Annotated[Session, Depends(get_db)],
    request: Annotated[schemas_analysis_data.BatchStatusRequest, Body()]
):
    """批量查询解析状态"""
    try:
        result = services_analysis_data.get_batch_parse_status(db, request.report_ids)
        return success(data=result)
    except Exception as e:
        logger.error(f"批量状态查询异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.get("")
async def get_data_list(
    db: Annotated[Session, Depends(get_db)],
    data_list_request: Annotated[schemas_analysis_data.DataListRequest, Depends()],
):
    """获取pdf数据列表"""
    try:
        result = services_analysis_data.get_financial_report_list(db, data_list_request)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"未预期异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/import-companies")
async def import_companies(
    db: Annotated[Session, Depends(get_db)], file: Annotated[UploadFile, File()]
):
    """导入附件1公司基本信息"""
    try:
        if not file.filename or not file.filename.endswith(('.xlsx', '.xls')):
            return error(code=ErrorCode.PARAM_ERROR, message="仅支持 Excel 文件（.xlsx 或 .xls）")

        import tempfile
        import os

        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name

        try:
            result = services_company_basic_info.upsert_company_basic_info_records(db, tmp_path)
            return success(result)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"导入公司信息异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="导入公司信息失败")


@router.delete("/{report_id}")
async def delete_data(
    db: Annotated[Session, Depends(get_db)],
    report_id: Annotated[int, Path(description="财报记录ID")],
):
    """删除财报记录及其关联数据"""
    try:
        result = services_analysis_data.delete_financial_report(db, report_id)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"未预期异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.get("/{report_id}")
async def get_data_detail(
    db: Annotated[Session, Depends(get_db)],
    report_id: Annotated[int, Path(description="财报记录ID")],
):
    """获取单个财报详情"""
    try:
        result = services_analysis_data.get_financial_report_detail(db, report_id)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"未预期异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.get("/{report_id}/json")
async def get_json_content(
    db: Annotated[Session, Depends(get_db)],
    report_id: Annotated[int, Path(description="财报记录ID")],
):
    """获取结构化JSON文件内容"""
    try:
        result = services_analysis_data.get_json_file_content(db, report_id)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"获取JSON内容异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")
