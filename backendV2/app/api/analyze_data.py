from fastapi import (
    APIRouter,
    Depends,
    Query,
    File,
    Path,
    Body,
    UploadFile,
    BackgroundTasks,
)
from typing import Annotated
from sqlalchemy.orm import Session
from app.db.database import get_db
from app.schemas.response import success, error
from app.schemas.common import ApiResponse, ErrorCode, PaginatedResponse
from app.utils.exception import ServiceException
from app.services import analyze_data as services_analyze_data
from app.schemas import analyze_data as schemas_analyze_data


router = APIRouter(prefix="/api/v1/analyze-data", tags=["结构化财报PDF"])


@router.post(
    "/upload",
    response_model=ApiResponse[schemas_analyze_data.UploadFileResponse],
    description="上传财报PDF文件",
)
def upload_report_file(
    db: Annotated[Session, Depends(get_db)],
    file_list: Annotated[
        list[UploadFile], File(..., description="要上传的PDF文件列表")
    ],
):
    """上传财报PDF文件"""
    try:
        return success(services_analyze_data.upload_report_file(db, file_list))
    except ServiceException as e:
        return error(e.code, e.message)


@router.post(
    "/list",
    response_model=ApiResponse[PaginatedResponse],
    description="获取财报列表",
)
def get_report_list(
    db: Annotated[Session, Depends(get_db)],
    get_report_list_request: Annotated[
        schemas_analyze_data.GetReportListRequest,
        Body(..., description="获取财报列表请求"),
    ],
):
    """获取财报列表"""
    try:
        return success(
            services_analyze_data.get_report_list(db, get_report_list_request)
        )
    except ServiceException as e:
        return error(e.code, e.message)


@router.post(
    "/parse",
    response_model=ApiResponse[schemas_analyze_data.ParseReportResponse],
    description="解析财报PDF文件",
)
def parse_report(
    db: Annotated[Session, Depends(get_db)],
    background_tasks: BackgroundTasks,
    parse_report_request: Annotated[
        schemas_analyze_data.ParseReportRequest,
        Body(..., description="解析财报PDF文件请求"),
    ],
):
    """解析财报PDF文件"""
    try:
        return success(
            services_analyze_data.parse_report(
                db, background_tasks, parse_report_request
            )
        )
    except ServiceException as e:
        return error(e.code, e.message)


@router.post(
    "/detail",
    response_model=ApiResponse[schemas_analyze_data.GetReportDetailResponse],
    description="获取财报详情",
)
def get_report_detail(
    db: Annotated[Session, Depends(get_db)],
    get_report_detail_request: Annotated[
        schemas_analyze_data.GetReportDetailRequest,
        Body(..., description="获取财报详情请求"),
    ],
):
    """获取财报详情"""
    try:
        return success(
            services_analyze_data.get_report_detail(db, get_report_detail_request)
        )
    except ServiceException as e:
        return error(e.code, e.message)


@router.post(
    "/delete",
    response_model=ApiResponse[schemas_analyze_data.DeleteReportResponse],
    description="删除财报",
)
def delete_report(
    db: Annotated[Session, Depends(get_db)],
    delete_report_request: Annotated[
        schemas_analyze_data.DeleteReportRequest,
        Body(..., description="删除财报请求"),
    ],
):
    """删除财报"""
    try:
        return success(services_analyze_data.delete_report(db, delete_report_request))
    except ServiceException as e:
        return error(e.code, e.message)
