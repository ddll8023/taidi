"""Chroma 向量数据库连接管理"""

from app.core.config import settings
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model
from langchain_chroma import Chroma

logger = setup_logger(__name__)

_vectorstore = None


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
