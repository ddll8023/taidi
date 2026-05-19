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
from app.services import import_company_base_info as services_import_company_base_info
from app.schemas import import_company_base_info as schemas_import_company_base_info

router = APIRouter(prefix="/api/v1/company_base_info", tags=["公司基本信息导入"])


@router.post(
    "/upload",
    response_model=ApiResponse[
        schemas_import_company_base_info.ImportCompanyBaseInfoResponse
    ],
    description="导入公司基本信息",
)
def import_company_base_info(
    db: Annotated[Session, Depends(get_db)],
    file: Annotated[UploadFile, File(...)],
):
    """导入公司基本信息"""
    try:
        return success(
            services_import_company_base_info.import_company_base_info(db, file)
        )
    except ServiceException as e:
        return error(e.code, e.message)
