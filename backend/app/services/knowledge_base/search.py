"""知识库语义检索与证据格式化服务"""

import re

from sqlalchemy import select

from app.core.config import settings
from app.db.database import get_background_db_session
from app.db.milvus import get_kb_collection
from app.models import knowledge_chunk as models_knowledge_chunk
from app.models import knowledge_document as models_knowledge_document
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model
from app.constants import knowledge_base as constants_kb

logger = setup_logger(__name__)


# ========== 公共入口函数 ==========


def search_knowledge(
    query_text: str,
    *,
    stock_code: str | None = None,
    doc_type=None,
    top_k: int = 5,
):
    """语义检索知识库切块"""
    logger.info(
        f"[search_knowledge] 开始检索: query长度={len(query_text)}, stock_code={stock_code}, doc_type={doc_type}, top_k={top_k}"
    )

    embedding_model = get_model.embedding_model
    logger.debug("[search_knowledge] 调用 embed_query...")
    try:
        query_embedding = embedding_model.embed_query(query_text)
    except ServiceException:
        raise
    except Exception as exc:
        logger.error(f"[search_knowledge] Embedding 调用异常: {exc}", exc_info=True)
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "服务调用失败，请稍后重试") from exc
    logger.debug(
        f"[search_knowledge] embed_query 返回: embedding长度={len(query_embedding) if query_embedding else 'None'}"
    )

    if not query_embedding or len(query_embedding) != settings.EMBEDDING_DIM:
        logger.error(
            f"[search_knowledge] Embedding 维度异常: expected={settings.EMBEDDING_DIM}, got={len(query_embedding) if query_embedding else 'None'}"
        )
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "服务调用失败，请稍后重试")

    logger.debug("[search_knowledge] 正在获取 Milvus Collection...")
    try:
        collection = get_kb_collection()
        logger.debug("[search_knowledge] Milvus Collection 获取成功")

        doc_types = _normalize_doc_types(doc_type)
        filter_expr = _build_kb_filter_expr(stock_code, doc_types)
        effective_filter_expr = filter_expr

        search_params = {
            "metric_type": "COSINE",
            "params": {"nprobe": 10},
        }

        output_fields = [
            "chunk_id",
            "document_id",
            "doc_type",
            "stock_code",
            "vector_version",
        ]

        logger.info(
            f"[search_knowledge] Milvus检索参数: filter_expr={filter_expr or '<none>'}, normalized_doc_types={doc_types or []}, top_k={top_k}"
        )

        results = collection.search(
            data=[query_embedding],
            anns_field="embedding",
            param=search_params,
            limit=top_k,
            expr=filter_expr,
            output_fields=output_fields,
        )
        logger.info(
            f"[search_knowledge] Milvus初次检索完成: hit_count={_count_search_hits(results)}, filter_expr={filter_expr or '<none>'}"
        )

        if not _has_search_hits(results) and stock_code:
            fallback_expr = _build_kb_filter_expr(None, doc_types)
            logger.info(
                f"[search_knowledge] 股票代码过滤无命中，放宽stock_code重试: original={filter_expr}, fallback={fallback_expr}"
            )
            results = collection.search(
                data=[query_embedding],
                anns_field="embedding",
                param=search_params,
                limit=top_k,
                expr=fallback_expr,
                output_fields=output_fields,
            )
            effective_filter_expr = fallback_expr
            logger.info(
                f"[search_knowledge] 放宽stock_code后检索完成: hit_count={_count_search_hits(results)}, filter_expr={fallback_expr or '<none>'}"
            )

        if not _has_search_hits(results) and doc_types:
            fallback_expr = _build_kb_filter_expr(stock_code, [])
            logger.info(
                f"[search_knowledge] 文档类型过滤无命中，放宽doc_type重试: original={filter_expr}, fallback={fallback_expr}"
            )
            results = collection.search(
                data=[query_embedding],
                anns_field="embedding",
                param=search_params,
                limit=top_k,
                expr=fallback_expr,
                output_fields=output_fields,
            )
            effective_filter_expr = fallback_expr
            logger.info(
                f"[search_knowledge] 放宽doc_type后检索完成: hit_count={_count_search_hits(results)}, filter_expr={fallback_expr or '<none>'}"
            )
    except ServiceException:
        raise
    except Exception as exc:
        logger.error(f"[search_knowledge] Milvus 检索异常: {exc}", exc_info=True)
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "服务调用失败，请稍后重试") from exc

    if not results or not results[0]:
        logger.info(f"[search_knowledge] 检索无命中: query={query_text[:120]}")
        return []

    hits = results[0]
    chunk_ids = [
        hit.entity.get("chunk_id") for hit in hits if hit.entity.get("chunk_id")
    ]

    db = get_background_db_session()
    try:
        chunk_records = db.scalars(
            select(models_knowledge_chunk.KnowledgeChunk).where(
                models_knowledge_chunk.KnowledgeChunk.id.in_(chunk_ids)
            )
        ).all()
        chunk_map = {c.id: c for c in chunk_records}

        doc_ids = list(set(c.document_id for c in chunk_records))
        doc_records = db.scalars(
            select(models_knowledge_document.KnowledgeDocument).where(
                models_knowledge_document.KnowledgeDocument.id.in_(doc_ids)
            )
        ).all()
        doc_map = {d.id: d for d in doc_records}

        search_results: list[dict] = []
        for hit in hits:
            chunk_id = hit.entity.get("chunk_id")
            if chunk_id is None:
                continue

            chunk = chunk_map.get(chunk_id)
            if chunk is None:
                continue

            doc = doc_map.get(chunk.document_id)

            search_results.append(
                {
                    "chunk_id": chunk_id,
                    "document_id": chunk.document_id,
                    "page_no": chunk.page_no,
                    "chunk_text": chunk.chunk_text,
                    "score": float(hit.score),
                    "doc_type": doc.doc_type if doc else None,
                    "title": doc.title if doc else None,
                    "source_path": doc.source_path if doc else None,
                    "stock_code": doc.stock_code if doc else None,
                    "stock_abbr": doc.stock_abbr if doc else None,
                }
            )

        logger.info(
            f"[search_knowledge] 回表完成: result_count={len(search_results)}, effective_filter_expr={effective_filter_expr or '<none>'}, top_hits={_summarize_search_results(search_results)}"
        )
        return search_results

    finally:
        db.close()


def search_and_format_evidence(
    query_text: str,
    *,
    stock_code: str | None = None,
    doc_type=None,
    top_k: int = 5,
):
    """检索知识库并格式化为证据列表"""
    results = search_knowledge(
        query_text,
        stock_code=stock_code,
        doc_type=doc_type,
        top_k=top_k,
    )

    evidence_list: list[dict] = []
    for item in results:
        evidence = {
            "paper_path": item.get("source_path"),
            "text": item.get("chunk_text", "")[:500],
            "page_no": item.get("page_no"),
            "score": item.get("score"),
            "doc_type": item.get("doc_type"),
            "title": item.get("title"),
            "stock_code": item.get("stock_code"),
            "stock_abbr": item.get("stock_abbr"),
        }
        evidence_list.append(evidence)

    logger.info(
        f"[search_and_format_evidence] 证据格式化完成: evidence_count={len(evidence_list)}, query={query_text[:120]}"
    )
    return evidence_list


"""辅助函数"""


def _normalize_doc_types(doc_type):
    """规范化文档类型参数为统一列表"""
    if not doc_type:
        return []

    raw_items = []
    if isinstance(doc_type, str):
        raw_items = re.split(r"[,，、/\s]+", doc_type)
    elif isinstance(doc_type, (list, tuple, set)):
        for item in doc_type:
            if isinstance(item, str):
                raw_items.extend(re.split(r"[,，、/\s]+", item))
    else:
        raw_items = [str(doc_type)]

    normalized = []
    for item in raw_items:
        value = item.strip().upper()
        if value in constants_kb.VALID_KB_DOC_TYPES and value not in normalized:
            normalized.append(value)
    return normalized


def _build_kb_filter_expr(stock_code: str | None, doc_types: list[str]):
    """构建 Milvus 过滤表达式"""
    filters = []
    if stock_code:
        filters.append(f'stock_code == "{stock_code}"')
    if len(doc_types) == 1:
        filters.append(f'doc_type == "{doc_types[0]}"')
    elif len(doc_types) > 1:
        doc_type_values = ", ".join(f'"{item}"' for item in doc_types)
        filters.append(f"doc_type in [{doc_type_values}]")
    if not filters:
        return None
    return " and ".join(filters)


def _has_search_hits(results):
    """判断检索结果是否有命中"""
    return bool(results and results[0])


def _count_search_hits(results):
    """统计检索命中数"""
    if not results or not results[0]:
        return 0
    return len(results[0])


def _summarize_search_results(search_results: list[dict], limit: int = 3):
    """摘要展示检索结果"""
    summary = []
    for item in search_results[:limit]:
        summary.append(
            {
                "title": item.get("title"),
                "page_no": item.get("page_no"),
                "score": round(float(item.get("score", 0)), 4),
                "doc_type": item.get("doc_type"),
                "stock_code": item.get("stock_code"),
                "stock_abbr": item.get("stock_abbr"),
            }
        )
    return summary
