"""知识库管理路由"""

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    UploadFile,
)
from typing import Annotated
from sqlalchemy.orm import Session
from app.db.database import get_db
from app.schemas.response import success, error
from app.schemas.common import ApiResponse
from app.utils.exception import ServiceException
from app.services import knowledge_base as services_knowledge_base
from app.schemas import knowledge_base as schemas_knowledge_base
router = APIRouter(prefix="/api/v1/knowledge-base", tags=["知识库管理"])


@router.post(
    "/init",
    response_model=ApiResponse[schemas_knowledge_base.InitKnowledgeBaseResponse],
    description="系统初始化：加载研报元数据到知识库",
)
def init_knowledge_base(
    db: Annotated[Session, Depends(get_db)],
    file: Annotated[UploadFile, File(..., description="研报Excel文件")],
    doc_type: Annotated[
        str,
        Form(
            ...,
            description="文档类型：RESEARCH_REPORT个股研报 / INDUSTRY_REPORT行业研报",
        ),
    ],
):
    """系统初始化"""
    try:
        return success(services_knowledge_base.init_knowledge_base(db, file, doc_type))
    except ServiceException as e:
        return error(e.code, e.message)


@router.post(
    "/init-status",
    response_model=ApiResponse[schemas_knowledge_base.InitStatusResponse],
    description="查询系统初始化状态",
)
def get_init_status(
    db: Annotated[Session, Depends(get_db)],
):
    """查询系统初始化状态"""
    try:
        return success(services_knowledge_base.get_init_status(db))
    except ServiceException as e:
        return error(e.code, e.message)
