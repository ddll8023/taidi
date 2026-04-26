"""知识库管理 API 路由"""

from typing import Annotated, Optional

from fastapi import (
    APIRouter,
    Depends,
    Query,
    Body,
    File,
    UploadFile,
    BackgroundTasks,
    Path,
)
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services import knowledge_base as services_knowledge_base
from app.schemas import knowledge_base as schemas_kb
from app.schemas.response import success, error
from app.schemas.common import ApiResponse
from app.utils.exception import ServiceException

router = APIRouter(prefix="/api/v1/knowledge-base", tags=["知识库管理"])


@router.post("/chunk/{document_id}", response_model=ApiResponse)
async def chunk_document(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    document_id: Annotated[int, Path(description="文档ID")],
    force: Annotated[bool, Query(description="强制重新切块")] = False,
):
    """提交单个文档的切块任务（异步）"""
    try:
        result = services_knowledge_base.submit_and_run_chunk_task(
            db, document_id, force, background_tasks
        )
        return success(data=result.to_dict())
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/chunk/batch", response_model=ApiResponse)
async def chunk_documents_batch(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    request: Annotated[schemas_kb.BatchChunkRequest, Body()],
):
    """提交批量切块任务（异步）"""
    try:
        result = services_knowledge_base.submit_and_run_batch_chunk(
            db, request.document_ids, background_tasks
        )
        return success(data=result.to_dict())
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/chunk/all", response_model=ApiResponse)
async def chunk_all_pending(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    request: Annotated[schemas_kb.ChunkAllRequest, Body()],
):
    """提交所有待处理文档的切块任务（异步）"""
    try:
        result = services_knowledge_base.submit_and_run_all_pending_chunk(
            db, request.limit, request.doc_type, background_tasks
        )
        return success(data=result.to_dict())
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/vectorize/{document_id}", response_model=ApiResponse)
async def vectorize_document(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    document_id: Annotated[int, Path(description="文档ID")],
    batch_size: Annotated[int, Query(description="每批处理数量")] = 20,
    force: Annotated[bool, Query(description="是否强制重试失败/已完成切块")] = False,
):
    """向量化单个文档的所有切块（异步）"""
    try:
        result = services_knowledge_base.submit_and_run_vectorize_task(
            db, document_id, force, batch_size, background_tasks
        )
        return success(data=result.to_dict())
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/vectorize", response_model=ApiResponse)
async def vectorize_chunks(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    request: Annotated[schemas_kb.VectorizeDocumentRequest, Body()],
):
    """批量向量化待处理的文档切块（异步）"""
    try:
        result = services_knowledge_base.submit_and_run_batch_vectorize(
            db, request.batch_size, request.force, background_tasks
        )
        return success(data=result.to_dict())
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/reset-vector-status/{document_id}", response_model=ApiResponse)
def reset_vector_status(
    db: Annotated[Session, Depends(get_db)],
    document_id: Annotated[int, Path(description="文档ID")],
    target_status: Annotated[int, Query(description="目标状态（默认0-PENDING）")] = 0,
):
    """重置文档的向量状态（用于取消处理中任务或重新向量化）"""
    try:
        result = services_knowledge_base.reset_vector_status(
            db,
            document_id,
            target_status,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("/stats", response_model=ApiResponse)
def get_stats(
    db: Annotated[Session, Depends(get_db)],
):
    """获取知识库整体统计信息"""
    try:
        result = services_knowledge_base.get_knowledge_base_stats(db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("/documents", response_model=ApiResponse)
def list_documents(
    db: Annotated[Session, Depends(get_db)],
    doc_type: Annotated[Optional[str], Query(description="按文档类型筛选")] = None,
    stock_code: Annotated[Optional[str], Query(description="按股票代码筛选")] = None,
    metadata_status: Annotated[
        Optional[int], Query(description="按元数据状态筛选")
    ] = None,
    chunk_status: Annotated[Optional[int], Query(description="按切块状态筛选")] = None,
    vector_status: Annotated[Optional[int], Query(description="按向量状态筛选")] = None,
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=100, description="每页数量")] = 20,
):
    """分页查询知识库文档列表"""
    try:
        result = services_knowledge_base.get_document_list(
            db,
            doc_type=doc_type,
            stock_code=stock_code,
            metadata_status=metadata_status,
            chunk_status=chunk_status,
            vector_status=vector_status,
            page=page,
            page_size=page_size,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/documents/status/batch", response_model=ApiResponse)
def get_documents_status_batch(
    db: Annotated[Session, Depends(get_db)],
    request: Annotated[schemas_kb.BatchStatusRequest, Body()],
):
    """批量查询文档状态"""
    try:
        results = services_knowledge_base.get_documents_status_batch(
            db, request.document_ids
        )
        return success(data=results)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("/chunks", response_model=ApiResponse)
def list_chunks(
    db: Annotated[Session, Depends(get_db)],
    document_id: Annotated[Optional[int], Query(description="按文档ID筛选")] = None,
    vector_status: Annotated[Optional[int], Query(description="按向量状态筛选")] = None,
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=100, description="每页数量")] = 20,
):
    """分页查询知识库切块列表"""
    try:
        result = services_knowledge_base.get_chunk_list(
            db,
            document_id=document_id,
            vector_status=vector_status,
            page=page,
            page_size=page_size,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/search", response_model=ApiResponse)
def search_knowledge(
    request: schemas_kb.SearchRequest,
):
    """知识库语义检索（调试用）"""
    try:
        results = services_knowledge_base.search_and_format_evidence(
            query_text=request.query,
            stock_code=request.stock_code,
            doc_type=request.doc_type,
            top_k=request.top_k,
        )
        return success(data={"results": results})
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/init", response_model=ApiResponse)
async def init_system(
    db: Annotated[Session, Depends(get_db)],
    stock_excel: Annotated[UploadFile, File(description="个股研报Excel文件")],
    industry_excel: Annotated[UploadFile, File(description="行业研报Excel文件")],
    force_reload: Annotated[bool, Query(description="是否强制重新加载")] = False,
):
    """系统初始化：加载Excel元数据到knowledge_document表"""
    try:
        result = await services_knowledge_base.init_system_from_upload(
            db=db,
            stock_excel=stock_excel,
            industry_excel=industry_excel,
            force_reload=force_reload,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/upload-pdf", response_model=ApiResponse)
async def upload_pdf_incremental(
    db: Annotated[Session, Depends(get_db)],
    pdfs: Annotated[list[UploadFile], File(description="PDF文件列表")],
    doc_type: Annotated[
        str, Query(description="文档类型：RESEARCH_REPORT或INDUSTRY_REPORT")
    ] = "RESEARCH_REPORT",
):
    """增量上传PDF文件，立即处理（匹配元数据+切块）"""
    try:
        result = await services_knowledge_base.upload_pdf_incremental(
            db=db,
            pdfs=pdfs,
            doc_type=doc_type,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("/progress", response_model=ApiResponse)
def get_progress(
    db: Annotated[Session, Depends(get_db)],
):
    """查询增量处理进度"""
    try:
        result = services_knowledge_base.get_processing_progress(db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("/init-status", response_model=ApiResponse)
def get_init_status(
    db: Annotated[Session, Depends(get_db)],
):
    """查询系统初始化状态"""
    try:
        result = services_knowledge_base.get_init_status(db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/upload-single-pdf", response_model=ApiResponse)
async def upload_single_pdf(
    db: Annotated[Session, Depends(get_db)],
    document_id: Annotated[int, Query(description="文档ID")],
    pdf_file: Annotated[UploadFile, File(description="PDF文件")],
):
    """上传单个文档的PDF（用于文档列表中的"上传PDF"按钮）"""
    try:
        result = await services_knowledge_base.upload_single_pdf_for_document(
            db=db,
            document_id=document_id,
            pdf_file=pdf_file,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/retry-failed", response_model=ApiResponse)
async def retry_failed(
    db: Annotated[Session, Depends(get_db)],
    document_id: Annotated[int, Query(description="文档ID")],
    pdf_file: Annotated[UploadFile, File(description="PDF文件")],
):
    """重试失败的文档"""
    try:
        result = await services_knowledge_base.retry_failed_document(
            db=db,
            document_id=document_id,
            pdf_file=pdf_file,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
