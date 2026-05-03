"""知识库元数据管理、缓存与统计服务"""
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.models import knowledge_chunk as models_knowledge_chunk
from app.models import knowledge_document as models_knowledge_document
from app.schemas import knowledge_base as schemas_knowledge_base
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.db.database import commit_or_rollback

logger = setup_logger(__name__)


# ========== 公共入口函数 ==========


def get_init_status(db: Session):
    """获取系统初始化状态"""
    doc_model = models_knowledge_document.KnowledgeDocument
    loaded_filter = doc_model.metadata_status >= models_knowledge_document.METADATA_STATUS_LOADED

    initialized = db.scalar(
        select(func.count(doc_model.id)).where(loaded_filter)
    ) > 0

    stock_count = db.scalar(
        select(func.count(doc_model.id)).where(
            doc_model.doc_type == "RESEARCH_REPORT",
            loaded_filter,
        )
    )

    industry_count = db.scalar(
        select(func.count(doc_model.id)).where(
            doc_model.doc_type == "INDUSTRY_REPORT",
            loaded_filter,
        )
    )

    return schemas_knowledge_base.InitStatusResponse(
        initialized=initialized,
        stock_metadata_count=stock_count,
        industry_metadata_count=industry_count,
        total_metadata_count=stock_count + industry_count,
    )


def reset_vector_status(
    db: Session,
    document_id: int,
    target_status: int = models_knowledge_chunk.CHUNK_VECTOR_STATUS_PENDING,
) -> dict:
    """重置文档的向量状态（用于取消处理中任务或重置已完成任务）"""
    doc_model = models_knowledge_document.KnowledgeDocument
    doc = db.scalar(
        select(doc_model).where(doc_model.id == document_id)
    )
    if doc is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"文档不存在: id={document_id}")

    if doc.vector_status not in [
        models_knowledge_document.VECTOR_STATUS_PROCESSING,
        models_knowledge_document.VECTOR_STATUS_SUCCESS,
        models_knowledge_document.VECTOR_STATUS_FAILED,
    ]:
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            f"仅支持重置处理中(PROCESSING)、成功(SUCCESS)、失败(FAILED)状态的文档"
        )

    old_status = doc.vector_status
    doc.vector_status = models_knowledge_document.VECTOR_STATUS_PENDING

    chunk_model = models_knowledge_chunk.KnowledgeChunk
    chunk_reset_count = db.execute(
        update(chunk_model)
        .where(chunk_model.document_id == document_id)
        .values(vector_status=models_knowledge_chunk.CHUNK_VECTOR_STATUS_PENDING)
    ).rowcount

    commit_or_rollback(db)

    logger.info(
        f"[reset_vector_status] 重置文档向量状态: document_id={document_id}, old_status={old_status}, chunk_reset_count={chunk_reset_count}"
    )

    return schemas_knowledge_base.ResetVectorStatusResponse(
        document_id=document_id,
        old_status=old_status,
        new_status=models_knowledge_document.VECTOR_STATUS_PENDING,
        chunk_reset_count=chunk_reset_count,
        message=f"已重置文档状态和{chunk_reset_count}个切块状态为待处理",
    )


def get_knowledge_base_stats(db: Session):
    """获取知识库整体统计信息"""
    doc_model = models_knowledge_document.KnowledgeDocument
    chunk_model = models_knowledge_chunk.KnowledgeChunk

    total_docs = db.scalar(select(func.count(doc_model.id))) or 0

    doc_by_chunk_status = dict(
        db.execute(
            select(doc_model.chunk_status, func.count(doc_model.id))
            .group_by(doc_model.chunk_status)
        ).all()
    )

    doc_by_vector_status = dict(
        db.execute(
            select(doc_model.vector_status, func.count(doc_model.id))
            .group_by(doc_model.vector_status)
        ).all()
    )

    doc_by_type = dict(
        db.execute(
            select(doc_model.doc_type, func.count(doc_model.id))
            .group_by(doc_model.doc_type)
        ).all()
    )

    total_chunks = db.scalar(select(func.count(chunk_model.id))) or 0

    chunk_by_vector_status = dict(
        db.execute(
            select(chunk_model.vector_status, func.count(chunk_model.id))
            .group_by(chunk_model.vector_status)
        ).all()
    )

    return schemas_knowledge_base.KnowledgeBaseStatsResponse(
        documents=schemas_knowledge_base.DocumentStatsItem(
            total=total_docs,
            by_chunk_status=doc_by_chunk_status,
            by_vector_status=doc_by_vector_status,
            by_doc_type=doc_by_type,
        ),
        chunks=schemas_knowledge_base.ChunkStatsItem(
            total=total_chunks,
            by_vector_status=chunk_by_vector_status,
        ),
    )


def get_documents_status_batch(db: Session, document_ids: list[int]):
    """批量查询文档状态"""
    doc_model = models_knowledge_document.KnowledgeDocument
    chunk_model = models_knowledge_chunk.KnowledgeChunk
    results = []
    for doc_id in document_ids:
        doc = db.scalar(
            select(doc_model).where(doc_model.id == doc_id)
        )
        if doc:
            chunk_count = db.scalar(
                select(func.count(chunk_model.id)).where(chunk_model.document_id == doc_id)
            ) or 0
            results.append(schemas_knowledge_base.DocumentStatusItem(
                id=doc.id,
                chunk_status=doc.chunk_status,
                vector_status=doc.vector_status,
                chunk_count=chunk_count,
            ))
    return results
