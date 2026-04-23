from fastapi import APIRouter, Depends, Query, Body, File, UploadFile, BackgroundTasks, Path
from typing import Annotated, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func
import os
import tempfile

from app.db.database import get_db
from app.services import knowledge_base as services_knowledge_base
from app.models.knowledge_document import KnowledgeDocument
from app.models.knowledge_chunk import KnowledgeChunk
from app.schemas import knowledge_base as schemas_kb
from app.schemas.response import success, error
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

router = APIRouter(prefix="/api/v1/knowledge-base", tags=["知识库管理"])
logger = setup_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 切块处理接口
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/chunk/{document_id}")
async def chunk_document(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    document_id: Annotated[int, Path(description="文档ID")],
    force: Annotated[bool, Query(description="强制重新切块")] = False,
):
    """提交单个文档的切块任务（异步）"""
    try:
        result = services_knowledge_base.submit_chunk_task(db, document_id, force)

        if result.status == "processing":
            background_tasks.add_task(
                services_knowledge_base.run_chunk_in_background,
                document_id,
            )

        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"提交切块任务异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/chunk/batch")
async def chunk_documents_batch(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    request: Annotated[schemas_kb.BatchChunkRequest, Body()],
):
    """提交批量切块任务（异步）"""
    try:
        result = services_knowledge_base.submit_batch_chunk(db, request.document_ids)

        if result.submitted_ids:
            background_tasks.add_task(
                services_knowledge_base.run_chunk_batch_in_background,
                result.submitted_ids,
            )

        return success(data={
            "submitted": result.submitted,
            "skipped": result.skipped,
            "message": f"已提交{result.submitted}个文档的切块任务",
        })
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"提交批量切块任务异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/chunk/all")
async def chunk_all_pending(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    request: Annotated[schemas_kb.ChunkAllRequest, Body()],
):
    """提交所有待处理文档的切块任务（异步）"""
    try:
        result = services_knowledge_base.submit_all_pending_chunk(
            db,
            limit=request.limit,
            doc_type=request.doc_type,
        )

        if result.submitted_ids:
            background_tasks.add_task(
                services_knowledge_base.run_chunk_batch_in_background,
                result.submitted_ids,
            )

        return success(data={
            "submitted": result.submitted,
            "message": f"已提交{result.submitted}个文档的切块任务",
        })
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"提交一键切块任务异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


# ─────────────────────────────────────────────────────────────────────────────
# 向量化接口
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/vectorize/{document_id}")
async def vectorize_document(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    document_id: Annotated[int, Path(description="文档ID")],
    batch_size: Annotated[int, Query(description="每批处理数量")] = 20,
    force: Annotated[bool, Query(description="是否强制重试失败/已完成切块")] = False,
):
    """向量化单个文档的所有切块（异步）"""
    try:
        result = services_knowledge_base.submit_vectorize_task(db, document_id, force=force)

        if result.status == "processing":
            background_tasks.add_task(
                services_knowledge_base.run_vectorize_in_background,
                document_id,
                batch_size,
            )

        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"提交向量化任务异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/vectorize")
async def vectorize_chunks(
    background_tasks: BackgroundTasks,
    db: Annotated[Session, Depends(get_db)],
    request: Annotated[schemas_kb.VectorizeDocumentRequest, Body()],
):
    """批量向量化待处理的文档切块（异步）"""
    try:
        result = services_knowledge_base.submit_batch_vectorize(
            db,
            batch_size=request.batch_size,
            force=request.force,
        )

        if result.submitted_count > 0:
            background_tasks.add_task(
                services_knowledge_base.run_vectorize_batch_in_background,
                request.batch_size,
                result.chunk_ids,
            )

        return success(data={
            "submitted": result.submitted_count,
            "message": f"已提交{result.submitted_count}个切块的向量化任务",
        })
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"提交批量向量化任务异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/reset-vector-status/{document_id}")
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
    except Exception as e:
        logger.error(f"重置向量状态异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


# ─────────────────────────────────────────────────────────────────────────────
# 状态查询接口
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/stats")
def get_stats(
    db: Annotated[Session, Depends(get_db)],
):
    """获取知识库整体统计信息"""
    total_docs = db.query(func.count(KnowledgeDocument.id)).scalar() or 0

    doc_by_chunk_status = dict(
        db.query(
            KnowledgeDocument.chunk_status,
            func.count(KnowledgeDocument.id),
        )
        .group_by(KnowledgeDocument.chunk_status)
        .all()
    )

    doc_by_vector_status = dict(
        db.query(
            KnowledgeDocument.vector_status,
            func.count(KnowledgeDocument.id),
        )
        .group_by(KnowledgeDocument.vector_status)
        .all()
    )

    doc_by_type = dict(
        db.query(
            KnowledgeDocument.doc_type,
            func.count(KnowledgeDocument.id),
        )
        .group_by(KnowledgeDocument.doc_type)
        .all()
    )

    total_chunks = db.query(func.count(KnowledgeChunk.id)).scalar() or 0

    chunk_by_vector_status = dict(
        db.query(
            KnowledgeChunk.vector_status,
            func.count(KnowledgeChunk.id),
        )
        .group_by(KnowledgeChunk.vector_status)
        .all()
    )

    return {
        "documents": {
            "total": total_docs,
            "by_chunk_status": doc_by_chunk_status,
            "by_vector_status": doc_by_vector_status,
            "by_doc_type": doc_by_type,
        },
        "chunks": {
            "total": total_chunks,
            "by_vector_status": chunk_by_vector_status,
        },
    }


@router.get("/documents")
def list_documents(
    db: Annotated[Session, Depends(get_db)],
    doc_type: Annotated[Optional[str], Query(description="按文档类型筛选")] = None,
    stock_code: Annotated[Optional[str], Query(description="按股票代码筛选")] = None,
    metadata_status: Annotated[Optional[int], Query(description="按元数据状态筛选")] = None,
    chunk_status: Annotated[Optional[int], Query(description="按切块状态筛选")] = None,
    vector_status: Annotated[Optional[int], Query(description="按向量状态筛选")] = None,
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=100, description="每页数量")] = 20,
):
    """分页查询知识库文档列表"""
    items, total = services_knowledge_base.get_document_list(
        db,
        doc_type=doc_type,
        stock_code=stock_code,
        metadata_status=metadata_status,
        chunk_status=chunk_status,
        vector_status=vector_status,
        page=page,
        page_size=page_size,
    )
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.post("/documents/status/batch")
def get_documents_status_batch(
    db: Annotated[Session, Depends(get_db)],
    request: Annotated[schemas_kb.BatchStatusRequest, Body()],
):
    """批量查询文档状态"""
    results = []
    for doc_id in request.document_ids:
        doc = db.query(KnowledgeDocument).filter(KnowledgeDocument.id == doc_id).first()
        if doc:
            chunk_count = db.query(func.count(KnowledgeChunk.id)).filter(
                KnowledgeChunk.document_id == doc_id
            ).scalar() or 0
            results.append({
                "id": doc.id,
                "chunk_status": doc.chunk_status,
                "vector_status": doc.vector_status,
                "chunk_count": chunk_count,
            })
    return success(data=results)


# ─────────────────────────────────────────────────────────────────────────────
# 切块列表接口
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/chunks")
def list_chunks(
    db: Annotated[Session, Depends(get_db)],
    document_id: Annotated[Optional[int], Query(description="按文档ID筛选")] = None,
    vector_status: Annotated[Optional[int], Query(description="按向量状态筛选")] = None,
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=100, description="每页数量")] = 20,
):
    """分页查询知识库切块列表"""
    items, total = services_knowledge_base.get_chunk_list(
        db,
        document_id=document_id,
        vector_status=vector_status,
        page=page,
        page_size=page_size,
    )
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 知识检索接口
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/search")
def search_knowledge(
    request: schemas_kb.SearchRequest,
):
    """知识库语义检索（调试用）"""
    results = services_knowledge_base.search_and_format_evidence(
        query_text=request.query,
        stock_code=request.stock_code,
        doc_type=request.doc_type,
        top_k=request.top_k,
    )
    return {"results": results}


# ─────────────────────────────────────────────────────────────────────────────
# 增量处理模式接口
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/init")
async def init_system(
    db: Annotated[Session, Depends(get_db)],
    stock_excel: Annotated[UploadFile, File(description="个股研报Excel文件")],
    industry_excel: Annotated[UploadFile, File(description="行业研报Excel文件")],
    force_reload: Annotated[bool, Query(description="是否强制重新加载")] = False,
):
    """系统初始化：加载Excel元数据到knowledge_document表"""
    try:
        if not stock_excel.filename or not stock_excel.filename.lower().endswith(('.xlsx', '.xls')):
            return error(code=ErrorCode.PARAM_ERROR, message="个股研报文件仅支持 Excel 格式（.xlsx/.xls）")
        if not industry_excel.filename or not industry_excel.filename.lower().endswith(('.xlsx', '.xls')):
            return error(code=ErrorCode.PARAM_ERROR, message="行业研报文件仅支持 Excel 格式（.xlsx/.xls）")

        stock_content = await stock_excel.read()
        industry_content = await industry_excel.read()

        result = await services_knowledge_base.init_knowledge_base_metadata(
            db=db,
            stock_excel_content=stock_content,
            industry_excel_content=industry_content,
            force_reload=force_reload,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"系统初始化异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"初始化失败: {str(e)[:200]}")


@router.post("/upload-pdf")
async def upload_pdf_incremental(
    db: Annotated[Session, Depends(get_db)],
    pdfs: Annotated[list[UploadFile], File(description="PDF文件列表")],
    doc_type: Annotated[str, Query(description="文档类型：RESEARCH_REPORT或INDUSTRY_REPORT")] = "RESEARCH_REPORT",
):
    """增量上传PDF文件，立即处理（匹配元数据+切块）"""
    try:
        if not pdfs or len(pdfs) == 0:
            return error(code=ErrorCode.PARAM_ERROR, message="请选择至少一个PDF文件")

        for pdf_file in pdfs:
            if not pdf_file.filename or not pdf_file.filename.lower().endswith('.pdf'):
                return error(code=ErrorCode.PARAM_ERROR, message="仅支持PDF文件")

        result = await services_knowledge_base.upload_pdf_incremental(
            db=db,
            pdfs=pdfs,
            doc_type=doc_type,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"增量上传PDF异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"上传失败: {str(e)[:200]}")


@router.get("/progress")
def get_progress(
    db: Annotated[Session, Depends(get_db)],
):
    """查询增量处理进度"""
    try:
        result = services_knowledge_base.get_processing_progress(db)
        return success(data=result)
    except Exception as e:
        logger.error(f"查询进度异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="查询进度失败")


@router.get("/init-status")
def get_init_status(
    db: Annotated[Session, Depends(get_db)],
):
    """查询系统初始化状态"""
    try:
        result = services_knowledge_base.get_init_status(db)
        return success(data=result)
    except Exception as e:
        logger.error(f"查询初始化状态异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="查询初始化状态失败")


@router.post("/upload-single-pdf")
async def upload_single_pdf(
    db: Annotated[Session, Depends(get_db)],
    document_id: Annotated[int, Query(description="文档ID")],
    pdf_file: Annotated[UploadFile, File(description="PDF文件")],
):
    """上传单个文档的PDF（用于文档列表中的"上传PDF"按钮）"""
    try:
        if not pdf_file.filename or not pdf_file.filename.lower().endswith('.pdf'):
            return error(code=ErrorCode.PARAM_ERROR, message="仅支持PDF文件")

        result = await services_knowledge_base.upload_single_pdf_for_document(
            db=db,
            document_id=document_id,
            pdf_file=pdf_file,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"单文档上传PDF异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"上传失败: {str(e)[:200]}")


@router.post("/retry-failed")
async def retry_failed(
    db: Annotated[Session, Depends(get_db)],
    document_id: Annotated[int, Query(description="文档ID")],
    pdf_file: Annotated[UploadFile, File(description="PDF文件")],
):
    """重试失败的文档"""
    try:
        if not pdf_file.filename or not pdf_file.filename.lower().endswith('.pdf'):
            return error(code=ErrorCode.PARAM_ERROR, message="仅支持PDF文件")

        result = await services_knowledge_base.retry_failed_document(
            db=db,
            document_id=document_id,
            pdf_file=pdf_file,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"重试文档异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"重试失败: {str(e)[:200]}")



