"""数据上传处理 API 路由"""
import os
import tempfile
from typing import Annotated

from fastapi import APIRouter, Depends, Query, File, Path, Body, UploadFile, BackgroundTasks
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services import analysis_data as services_analysis_data
from app.services import company_basic_info as services_company_basic_info
from app.schemas.response import success, error
from app.schemas.common import ApiResponse, ErrorCode
from app.utils.exception import ServiceException
from app.schemas import analysis_data as schemas_analysis_data

router = APIRouter(prefix="/api/v1/data", tags=["数据上传处理"])


@router.post("/upload", response_model=ApiResponse)
async def upload_data(
    db: Annotated[Session, Depends(get_db)],
    file: Annotated[UploadFile, File(description="PDF文件")],
):
    """上传PDF文件，仅执行建档入库（阶段一）"""
    try:
        file_content = await file.read()
        result = await services_analysis_data.upload_archive_only(
            db, file.filename, file_content
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/upload/batch", response_model=ApiResponse)
async def upload_batch_data(
    db: Annotated[Session, Depends(get_db)],
    files: Annotated[list[UploadFile], File(description="PDF文件列表")],
):
    """批量上传PDF文件，仅执行建档入库（阶段一）"""
    try:
        file_items = [(f.filename or "", await f.read()) for f in files]
        result = await services_analysis_data.upload_archive_batch(db, file_items)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/parse/batch", response_model=ApiResponse)
async def parse_batch_reports(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    request: Annotated[schemas_analysis_data.BatchParseRequest, Body(description="批量解析请求")],
):
    """提交批量解析任务（异步）"""
    try:
        result = services_analysis_data.submit_and_run_batch_parse(
            db, request.report_ids, background_tasks
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/parse/{report_id}", response_model=ApiResponse)
async def parse_single_report(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    report_id: Annotated[int, Path(description="财报ID")],
    force: Annotated[bool, Query(description="强制重新解析（包括已解析成功的）")] = False,
):
    """提交单个财报解析任务（异步）"""
    try:
        result = services_analysis_data.submit_and_run_single_parse(
            db, report_id, force, background_tasks
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/parse/all", response_model=ApiResponse)
async def parse_all_pending_reports(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    limit: Annotated[int, Query(description="最大处理数量")] = 100,
):
    """提交所有待处理财报的解析任务（异步）"""
    try:
        result = services_analysis_data.submit_and_run_all_pending_parse(
            db, limit, background_tasks
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/parse/status/batch", response_model=ApiResponse)
async def get_batch_parse_status(
    db: Annotated[Session, Depends(get_db)],
    request: Annotated[schemas_analysis_data.BatchStatusRequest, Body(description="批量状态查询请求")],
):
    """批量查询解析状态"""
    try:
        result = services_analysis_data.get_batch_parse_status(db, request.report_ids)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("", response_model=ApiResponse)
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


@router.post("/import-companies", response_model=ApiResponse)
async def import_companies(
    db: Annotated[Session, Depends(get_db)],
    file: Annotated[UploadFile, File(description="Excel文件")],
):
    """导入附件1公司基本信息"""
    try:
        if not file.filename or not file.filename.endswith(('.xlsx', '.xls')):
            raise ServiceException(ErrorCode.PARAM_ERROR, "仅支持 Excel 文件（.xlsx 或 .xls）")

        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name

        try:
            result = services_company_basic_info.upsert_company_basic_info_records(db, tmp_path)
            return success(data=result)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.delete("/{report_id}", response_model=ApiResponse)
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


@router.get("/{report_id}", response_model=ApiResponse)
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


@router.get("/{report_id}/json", response_model=ApiResponse)
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
