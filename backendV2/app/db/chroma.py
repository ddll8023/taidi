"""Chroma 向量数据库连接管理"""

from app.core.config import settings
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model
from langchain_chroma import Chroma
from langchain_core.documents import Document

logger = setup_logger(__name__)

_vectorstore = None


def _normalize_where(filter_dict: dict | None) -> dict | None:
    """将平铺的多字段筛选条件转换为 Chroma $and 格式"""
    if filter_dict is None:
        return None
    keys = list(filter_dict.keys())
    if len(keys) <= 1:
        return filter_dict
    return {"$and": [{k: v} for k, v in filter_dict.items()]}


def get_kb_vectorstore():
    """获取或创建知识库向量存储（单例）"""
    global _vectorstore
    if _vectorstore is not None:
        return _vectorstore

    logger.info(
        f"[Chroma] 初始化向量存储: collection={settings.CHROMA_KB_COLLECTION} "
        f"persist_dir={settings.CHROMA_PERSIST_DIR}"
    )
    _vectorstore = Chroma(
        collection_name=settings.CHROMA_KB_COLLECTION,
        embedding_function=get_model.embedding_model,
        persist_directory=settings.CHROMA_PERSIST_DIR,
    )
    logger.info("[Chroma] 向量存储初始化完成")
    return _vectorstore


def add_texts_to_kb(
    texts: list[str],
    metadatas: list[dict] | None = None,
    ids: list[str] | None = None,
):
    """批量写入文本到知识库向量存储"""
    vs = get_kb_vectorstore()
    logger.info(
        f"[Chroma] 批量写入: count={len(texts)} collection={settings.CHROMA_KB_COLLECTION}"
    )
    return vs.add_texts(texts=texts, metadatas=metadatas, ids=ids)


def search_kb(
    query: str,
    filter_dict: dict | None = None,
    k: int = 10,
):
    """带元数据过滤的语义搜索，返回 (Document, score) 列表"""
    vs = get_kb_vectorstore()
    where = _normalize_where(filter_dict)
    logger.info(
        f"[Chroma] 搜索: query_preview={query[:50]} where={where} k={k}"
    )
    return vs.similarity_search_with_score(query=query, filter=where, k=k)


def delete_by_filter(filter_dict: dict):
    """按条件删除向量（通过查询后按 ID 删除）"""
    vs = get_kb_vectorstore()
    where = _normalize_where(filter_dict)
    results = vs.get(where=where)
    ids = results.get("ids", [])
    if ids:
        logger.info(f"[Chroma] 删除向量: count={len(ids)} filter={filter_dict}")
        vs.delete(ids=ids)
    return len(ids)
