import logging

from pymilvus import Collection, CollectionSchema, DataType, FieldSchema, connections
from app.core.config import settings

logger = logging.getLogger(__name__)


def get_connection():
    """获取 Milvus"""
    logger.info("[Milvus] 正在连接 Milvus: uri=%s", settings.MILVUS_URI)
    try:
        connections.connect(uri=settings.MILVUS_URI)
        logger.info("[Milvus] Milvus 连接成功")
    except Exception as e:
        logger.error("[Milvus] Milvus 连接失败: uri=%s, error=%s", settings.MILVUS_URI, str(e))
        raise


def get_collection():
    """获取或创建财报向量 Collection"""
    get_connection()

    try:
        collection = Collection(settings.MILVUS_COLLECTION)
        collection.load()
        return collection
    except Exception:
        pass

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="report_id", dtype=DataType.INT64),
        FieldSchema(name="chunk_index", dtype=DataType.INT64),
        FieldSchema(name="vector_version", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(
            name="embedding", dtype=DataType.FLOAT_VECTOR, dim=settings.EMBEDDING_DIM
        ),
    ]
    schema = CollectionSchema(fields, description="财报向量库")
    collection = Collection(settings.MILVUS_COLLECTION, schema)

    index_params = {
        "index_type": "IVF_FLAT",
        "params": {"nlist": 128},
        "metric_type": "COSINE",
    }
    collection.create_index("embedding", index_params)
    collection.load()

    return collection


def get_kb_collection():
    """获取或创建知识库向量 Collection"""
    get_connection()

    try:
        logger.info("[Milvus] 尝试获取 Collection: %s", settings.MILVUS_KB_COLLECTION)
        collection = Collection(settings.MILVUS_KB_COLLECTION)
        collection.load()
        logger.info("[Milvus] Collection 获取并加载成功: %s", settings.MILVUS_KB_COLLECTION)
        return collection
    except Exception as e:
        logger.warning(
            "[Milvus] Collection 不存在或加载失败，将创建新 Collection: %s, error=%s",
            settings.MILVUS_KB_COLLECTION,
            str(e),
        )

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="chunk_id", dtype=DataType.INT64),
        FieldSchema(name="document_id", dtype=DataType.INT64),
        FieldSchema(name="doc_type", dtype=DataType.VARCHAR, max_length=32),
        FieldSchema(name="stock_code", dtype=DataType.VARCHAR, max_length=6),
        FieldSchema(name="vector_version", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(
            name="embedding", dtype=DataType.FLOAT_VECTOR, dim=settings.EMBEDDING_DIM
        ),
    ]
    schema = CollectionSchema(fields, description="知识库向量库")
    collection = Collection(settings.MILVUS_KB_COLLECTION, schema)
    logger.info("[Milvus] 新 Collection 创建成功: %s", settings.MILVUS_KB_COLLECTION)

    index_params = {
        "index_type": "IVF_FLAT",
        "params": {"nlist": 128},
        "metric_type": "COSINE",
    }
    collection.create_index("embedding", index_params)
    logger.info("[Milvus] Collection 索引创建成功")
    collection.load()
    logger.info("[Milvus] Collection 加载成功")

    return collection
