"""知识库管理路由"""

from fastapi import (
    APIRouter,
    Body,
    Depends,
    File,
    Form,
    UploadFile,
)
from typing import Annotated
from sqlalchemy.orm import Session
from app.db.database import get_db
from app.schemas.response import success, error
from app.schemas.common import ApiResponse, PaginatedResponse
from app.utils.exception import ServiceException
from app.services import knowledge_base as services_knowledge_base
from app.schemas import knowledge_base as schemas_knowledge_base

router = APIRouter(prefix="/api/v1/knowledge-base", tags=["知识库管理"])


@router.post(
    "/list",
    response_model=ApiResponse[PaginatedResponse],
    description="获取知识库文档列表",
)
def get_knowledge_document_list(
    db: Annotated[Session, Depends(get_db)],
    body: Annotated[
        schemas_knowledge_base.GetKnowledgeDocumentListRequest,
        Body(..., description="知识库文档列表请求"),
    ],
):
    """获取知识库文档列表"""
    try:
        return success(data=services_knowledge_base.get_knowledge_document_list(db, body))
    except ServiceException as e:
        return error(code=e.code, message=e.message)


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
        return success(data=services_knowledge_base.init_knowledge_base(db, file, doc_type))
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post(
    "/init-status",
    response_model=ApiResponse[schemas_knowledge_base.GetInitStatusResponse],
    description="查询系统初始化状态",
)
def get_init_status(
    db: Annotated[Session, Depends(get_db)],
):
    """查询系统初始化状态"""
    try:
        return success(data=services_knowledge_base.get_init_status(db))
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post(
    "/stats",
    response_model=ApiResponse[schemas_knowledge_base.GetKnowledgeBaseStatsResponse],
    description="获取知识库整体统计信息",
)
def get_knowledge_base_stats(
    db: Annotated[Session, Depends(get_db)],
):
    """获取知识库整体统计信息"""
    try:
        return success(data=services_knowledge_base.get_knowledge_base_stats(db))
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post(
    "/parse",
    response_model=ApiResponse[schemas_knowledge_base.ParseDocumentsResponse],
    description="批量解析文档：调用MinerU对PDF进行结构化解析",
)
def parse_documents(
    db: Annotated[Session, Depends(get_db)],
    body: Annotated[
        schemas_knowledge_base.ParseDocumentsRequest,
        Body(..., description="解析请求"),
    ],
):
    """批量解析文档"""
    try:
        return success(data=services_knowledge_base.parse_documents(db, body))
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post(
    "/parse-result",
    response_model=ApiResponse[schemas_knowledge_base.GetParseResultResponse],
    description="获取文档解析结果",
)
def get_parse_result(
    db: Annotated[Session, Depends(get_db)],
    body: Annotated[
        schemas_knowledge_base.GetParseResultRequest,
        Body(..., description="解析结果请求"),
    ],
):
    """获取文档解析结果"""
    try:
        return success(data=services_knowledge_base.get_parse_result(db, body.document_id))
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post(
    "/upload",
    response_model=ApiResponse[schemas_knowledge_base.UploadKnowledgeDocumentResponse],
    description="批量上传知识库文档PDF：按文件名匹配元数据并入库",
)
def upload_knowledge_documents(
    db: Annotated[Session, Depends(get_db)],
    file_list: Annotated[list[UploadFile], File(..., description="PDF文件列表")],
):
    """批量上传知识库文档PDF"""
    try:
        return success(
            data=services_knowledge_base.upload_knowledge_documents(db, file_list)
        )
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post(
    "/save-parse-result",
    response_model=ApiResponse[schemas_knowledge_base.SaveParseResultResponse],
    description="保存清洗后的Markdown解析结果",
)
def save_parse_result(
    db: Annotated[Session, Depends(get_db)],
    body: Annotated[
        schemas_knowledge_base.SaveParseResultRequest,
        Body(..., description="保存请求"),
    ],
):
    """保存清洗后的Markdown"""
    try:
        return success(data=services_knowledge_base.save_parse_result(db, body))
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post(
    "/toggle-clean",
    response_model=ApiResponse[schemas_knowledge_base.ToggleCleanStatusResponse],
    description="切换文档清洗标记（已清洗↔未清洗）",
)
def toggle_clean_status(
    db: Annotated[Session, Depends(get_db)],
    body: Annotated[
        schemas_knowledge_base.ToggleCleanStatusRequest,
        Body(..., description="切换清洗标记请求"),
    ],
):
    """切换清洗标记"""
    try:
        return success(
            data=services_knowledge_base.toggle_clean_status(db, body.document_id)
        )
    except ServiceException as e:
        return error(code=e.code, message=e.message)
