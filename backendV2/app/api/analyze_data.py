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
from app.schemas.common import ApiResponse, ErrorCode
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
