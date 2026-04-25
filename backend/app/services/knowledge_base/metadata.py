"""知识库元数据管理、缓存与统计服务"""
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.knowledge_chunk import (
    CHUNK_VECTOR_STATUS_PENDING,
    KnowledgeChunk,
)
from app.models.knowledge_document import (
    CHUNK_STATUS_COMPLETED,
    METADATA_STATUS_LOADED,
    VECTOR_STATUS_PENDING,
    VECTOR_STATUS_PROCESSING,
    VECTOR_STATUS_SUCCESS,
    VECTOR_STATUS_FAILED,
    KnowledgeDocument,
)
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)

_metadata_cache: dict | None = None
_metadata_cache_loaded: bool = False


"""辅助函数"""


def _get_metadata_map():
    """获取元数据映射缓存"""
    global _metadata_cache, _metadata_cache_loaded

    if _metadata_cache_loaded and _metadata_cache is not None:
        return _metadata_cache

    try:
        from app.services.fujian5_data_processor import parse_fujian5_excel_data

        stock_research_data, industry_research_data = parse_fujian5_excel_data()

        metadata_map: dict = {}

        for record in stock_research_data:
            title = record.get("title", "").strip()
            if title:
                metadata_map[title] = {
                    "stock_code": record.get("stockCode"),
                    "stock_abbr": record.get("stockName"),
                    "org_name": record.get("orgName"),
                    "publish_date": record.get("publishDate"),
                    "industry_name": None,
                    "doc_type": "RESEARCH_REPORT",
                }

        for record in industry_research_data:
            title = record.get("title", "").strip()
            if title:
                metadata_map[title] = {
                    "stock_code": None,
                    "stock_abbr": None,
                    "org_name": record.get("orgName"),
                    "publish_date": record.get("publishDate"),
                    "industry_name": record.get("industryName"),
                    "doc_type": "INDUSTRY_REPORT",
                }

        _metadata_cache = metadata_map
        _metadata_cache_loaded = True
        logger.info(f"元数据缓存加载完成: {len(metadata_map)} 条")
        return metadata_map

    except Exception as e:
        logger.error(f"加载元数据缓存失败: {e}")
        _metadata_cache = {}
        _metadata_cache_loaded = True
        return {}


# ========== 公共入口函数 ==========


def reload_metadata_cache():
    """重新加载元数据缓存"""
    global _metadata_cache_loaded
    _metadata_cache_loaded = False
    return _get_metadata_map()


def get_init_status(db: Session):
    initialized = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.metadata_status >= METADATA_STATUS_LOADED
    ).count() > 0

    stock_count = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.doc_type == "RESEARCH_REPORT",
        KnowledgeDocument.metadata_status >= METADATA_STATUS_LOADED,
    ).count()

    industry_count = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.doc_type == "INDUSTRY_REPORT",
        KnowledgeDocument.metadata_status >= METADATA_STATUS_LOADED,
    ).count()

    return {
        "initialized": initialized,
        "stock_metadata_count": stock_count,
        "industry_metadata_count": industry_count,
        "total_metadata_count": stock_count + industry_count,
    }


def reset_vector_status(
    db: Session,
    document_id: int,
    target_status: int = CHUNK_VECTOR_STATUS_PENDING,
) -> dict:
    """重置文档的向量状态（用于取消处理中任务或重置已完成任务）"""
    doc = db.query(KnowledgeDocument).filter(KnowledgeDocument.id == document_id).first()
    if doc is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"文档不存在: id={document_id}")

    if doc.vector_status not in [
        VECTOR_STATUS_PROCESSING,
        VECTOR_STATUS_SUCCESS,
        VECTOR_STATUS_FAILED,
    ]:
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            f"仅支持重置处理中(PROCESSING)、成功(SUCCESS)、失败(FAILED)状态的文档"
        )

    # 重置文档状态
    old_status = doc.vector_status
    doc.vector_status = VECTOR_STATUS_PENDING

    # 重置所有切块状态为 PENDING
    chunk_reset_count = (
        db.query(KnowledgeChunk)
        .filter(KnowledgeChunk.document_id == document_id)
        .update({"vector_status": CHUNK_VECTOR_STATUS_PENDING}, synchronize_session=False)
    )

    db.commit()

    logger.info(
        "[reset_vector_status] 重置文档向量状态: document_id=%d, old_status=%d, chunk_reset_count=%d",
        document_id,
        old_status,
        chunk_reset_count,
    )

    return {
        "document_id": document_id,
        "old_status": old_status,
        "new_status": VECTOR_STATUS_PENDING,
        "chunk_reset_count": chunk_reset_count,
        "message": f"已重置文档状态和{chunk_reset_count}个切块状态为待处理",
    }


def get_knowledge_base_stats(db: Session):
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


def get_documents_status_batch(db: Session, document_ids: list[int]):
    """批量查询文档状态"""
    results = []
    for doc_id in document_ids:
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
    return results
