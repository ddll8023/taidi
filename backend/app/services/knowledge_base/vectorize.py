"""知识库向量化处理、向量化任务提交与进度查询服务"""
from datetime import datetime

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.database import get_background_db_session
from app.db.milvus import get_kb_collection
from app.models.knowledge_chunk import (
    CHUNK_VECTOR_STATUS_COMPLETED,
    CHUNK_VECTOR_STATUS_FAILED,
    CHUNK_VECTOR_STATUS_PENDING,
    CHUNK_VECTOR_STATUS_PROCESSING,
    KnowledgeChunk,
)
from app.models.knowledge_document import (
    CHUNK_STATUS_COMPLETED,
    CHUNK_STATUS_FAILED,
    METADATA_STATUS_LOADED,
    METADATA_STATUS_PDF_UPLOADED,
    VECTOR_STATUS_FAILED,
    VECTOR_STATUS_PENDING,
    VECTOR_STATUS_PROCESSING,
    VECTOR_STATUS_SUCCESS,
    KnowledgeDocument,
)
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model

logger = setup_logger(__name__)

EMBEDDING_BATCH_SIZE = 25  # DashScope 单次批量 Embedding 最大条数


# 定义可重试的异常类型（网络错误、SSL错误）
RETRYABLE_EXCEPTIONS = (
    ConnectionError,
    OSError,
    Exception,  # 包含SSLError等网络异常
)


"""辅助函数"""


@retry(
    stop=stop_after_attempt(3),  # 最多重试3次
    wait=wait_exponential(multiplier=1, min=2, max=10),  # 指数退避：2s, 4s, 8s
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    reraise=True,
)
def _embed_with_retry(embedding_model, text: str):
    """带重试机制的embedding调用"""
    return embedding_model.embed_query(text)


# ========== 公共入口函数 ==========


def get_vector_version():
    return (
        f"{settings.EMBEDDING_MODEL}:"
        f"{settings.EMBEDDING_DIM}:"
        f"{settings.CHUNK_SIZE}:"
        f"{settings.CHUNK_OVERLAP}"
    )


def vectorize_chunks(
    db: Session,
    chunk_ids: list[int] | None = None,
    batch_size: int = 10,
):
    """批量向量化切块：分批 Embedding + 批量 Milvus 写入"""
    logger.info(
        "[vectorize_chunks] 开始向量化: chunk_ids=%s, batch_size=%d",
        chunk_ids,
        batch_size,
    )

    # 查询待处理或正在处理的切块（支持PROCESSING状态，因为submit_batch_vectorize会先标记为PROCESSING）
    query = db.query(KnowledgeChunk).filter(
        KnowledgeChunk.vector_status.in_([
            CHUNK_VECTOR_STATUS_PENDING,
            CHUNK_VECTOR_STATUS_PROCESSING,
        ])
    )
    if chunk_ids:
        query = query.filter(KnowledgeChunk.id.in_(chunk_ids))
    else:
        query = query.limit(batch_size)

    chunks = query.all()
    if not chunks:
        logger.warning("[vectorize_chunks] 没有待向量化的切块")
        return {"total": 0, "success": 0, "failed": 0, "errors": []}

    logger.info("[vectorize_chunks] 找到 %d 个待处理切块", len(chunks))

    results = {"total": len(chunks), "success": 0, "failed": 0, "errors": []}

    logger.info("[vectorize_chunks] 正在获取 Milvus Collection...")
    collection = get_kb_collection()
    vector_version = get_vector_version()
    logger.info(
        "[vectorize_chunks] Milvus Collection 获取成功: name=%s, vector_version=%s",
        collection.name,
        vector_version,
    )

    logger.info("[vectorize_chunks] 正在获取 Embedding 模型...")
    embedding_model = get_model.embedding_model
    logger.info("[vectorize_chunks] Embedding 模型获取成功")

    # 标记所有切块为 PROCESSING
    for chunk in chunks:
        chunk.vector_status = CHUNK_VECTOR_STATUS_PROCESSING
    db.commit()

    # 预加载文档信息（减少逐条查询）
    doc_ids = list(set(c.document_id for c in chunks))
    doc_map = {}
    for doc in db.query(KnowledgeDocument).filter(KnowledgeDocument.id.in_(doc_ids)).all():
        doc_map[doc.id] = doc

    # 删除 Milvus 中旧向量
    chunk_id_list = [c.id for c in chunks]
    try:
        delete_expr = f'chunk_id in {chunk_id_list}'
        collection.delete(expr=delete_expr)
        logger.info("[vectorize_chunks] 批量删除旧向量: count=%d", len(chunk_id_list))
    except Exception as del_err:
        logger.warning("[vectorize_chunks] 批量删除旧向量失败（非阻塞）: %s", str(del_err))

    # 分批调用 Embedding API
    succeeded: list[tuple[KnowledgeChunk, list[float]]] = []
    for batch_start in range(0, len(chunks), EMBEDDING_BATCH_SIZE):
        batch = chunks[batch_start:batch_start + EMBEDDING_BATCH_SIZE]
        texts = [c.chunk_text for c in batch]
        logger.info(
            "[vectorize_chunks] Embedding 批次 [%d-%d/%d]",
            batch_start + 1,
            min(batch_start + EMBEDDING_BATCH_SIZE, len(chunks)),
            len(chunks),
        )

        try:
            embeddings = embedding_model.embed_documents(texts)
            logger.info("[vectorize_chunks] Embedding 批次完成: 生成 %d 个向量", len(embeddings))

            for chunk, embedding in zip(batch, embeddings):
                if not embedding or len(embedding) != settings.EMBEDDING_DIM:
                    raise ValueError(
                        f"Embedding 维度不正确: expected={settings.EMBEDDING_DIM}, "
                        f"got={len(embedding) if embedding else 0}"
                    )
                succeeded.append((chunk, embedding))

        except Exception as e:
            # 整批失败时逐条重试，区分成功和失败
            logger.warning(
                "[vectorize_chunks] 批量 Embedding 失败，逐条重试: %s", str(e)
            )
            for chunk in batch:
                try:
                    embedding = _embed_with_retry(embedding_model, chunk.chunk_text)
                    if not embedding or len(embedding) != settings.EMBEDDING_DIM:
                        raise ValueError(
                            f"Embedding 维度不正确: expected={settings.EMBEDDING_DIM}, "
                            f"got={len(embedding) if embedding else 0}"
                        )
                    succeeded.append((chunk, embedding))
                except Exception as single_err:
                    chunk.vector_status = CHUNK_VECTOR_STATUS_FAILED
                    chunk.vector_error_message = str(single_err)[:2000]
                    db.commit()
                    results["failed"] += 1
                    results["errors"].append(
                        {"chunk_id": chunk.id, "error": str(single_err)[:500]}
                    )
                    logger.error(
                        "切块向量化失败: chunk_id=%d, error=%s",
                        chunk.id,
                        str(single_err),
                        exc_info=True,
                    )

    # 批量写入 Milvus
    if succeeded:
        all_insert_data = []
        for chunk, embedding in succeeded:
            doc = doc_map.get(chunk.document_id)
            doc_type = doc.doc_type if doc else ""
            stock_code = doc.stock_code if doc else None
            all_insert_data.append({
                "chunk_id": chunk.id,
                "document_id": chunk.document_id,
                "doc_type": doc_type,
                "stock_code": stock_code or "",
                "vector_version": vector_version,
                "embedding": embedding,
            })

        try:
            collection.insert(all_insert_data)
            collection.flush()
            logger.info(
                "[vectorize_chunks] 批量写入 Milvus: collection=%s, count=%d",
                collection.name,
                len(all_insert_data),
            )
        except Exception as milvus_err:
            logger.error("[vectorize_chunks] Milvus 批量写入失败: %s", str(milvus_err), exc_info=True)
            # Milvus 写入失败，全部标记 FAILED
            for chunk, _ in succeeded:
                chunk.vector_status = CHUNK_VECTOR_STATUS_FAILED
                chunk.vector_error_message = f"Milvus写入失败: {str(milvus_err)[:1500]}"
                results["failed"] += 1
                results["errors"].append(
                    {"chunk_id": chunk.id, "error": f"Milvus写入失败: {str(milvus_err)[:500]}"}
                )
            db.commit()
            _update_document_vector_status(db, [c.document_id for c in chunks])
            return results

        # 通过 chunk_id 查询 Milvus 获取真正的 auto_id（避免依赖 insert 返回的 primary_keys）
        inserted_chunk_ids = [c.id for c, _ in succeeded]
        try:
            id_lookup = collection.query(
                expr=f"chunk_id in {inserted_chunk_ids}",
                output_fields=["id", "chunk_id"],
                limit=len(inserted_chunk_ids),
            )
            # chunk_id → auto_id
            chunk_id_to_milvus_id = {r["chunk_id"]: r["id"] for r in id_lookup}
        except Exception as query_err:
            logger.error("[vectorize_chunks] 查询 Milvus auto_id 失败: %s", str(query_err), exc_info=True)
            # 查询失败，全部标记 FAILED
            for chunk, _ in succeeded:
                chunk.vector_status = CHUNK_VECTOR_STATUS_FAILED
                chunk.vector_error_message = f"查询Milvus auto_id失败: {str(query_err)[:1500]}"
                results["failed"] += 1
                results["errors"].append(
                    {"chunk_id": chunk.id, "error": f"查询Milvus auto_id失败: {str(query_err)[:500]}"}
                )
            db.commit()
            _update_document_vector_status(db, [c.document_id for c in chunks])
            return results

        if len(chunk_id_to_milvus_id) != len(succeeded):
            logger.warning(
                "[vectorize_chunks] Milvus 返回 ID 数量异常: expected=%d, actual=%d",
                len(succeeded),
                len(chunk_id_to_milvus_id),
            )

        # 更新成功的切块状态
        for chunk, _ in succeeded:
            milvus_id = chunk_id_to_milvus_id.get(chunk.id)
            if milvus_id is None:
                # ID 缺失 → 标记为 FAILED（不能设为 COMPLETED + None，会导致假完成）
                chunk.vector_status = CHUNK_VECTOR_STATUS_FAILED
                chunk.vector_error_message = "Milvus写入成功但无法获取auto_id"
                results["failed"] += 1
                results["errors"].append(
                    {"chunk_id": chunk.id, "error": "无法获取Milvus auto_id"}
                )
                logger.error(
                    "[vectorize_chunks] 无法获取 Milvus auto_id: chunk_id=%d", chunk.id
                )
            else:
                chunk.vector_status = CHUNK_VECTOR_STATUS_COMPLETED
                chunk.milvus_id = milvus_id
                results["success"] += 1
        db.commit()

    _update_document_vector_status(db, [c.document_id for c in chunks])

    logger.info(
        "批量向量化完成: total=%d, success=%d, failed=%d",
        results["total"],
        results["success"],
        results["failed"],
    )
    return results


def vectorize_document(db: Session, document_id: int):
    logger.info("[vectorize_document] 开始向量化文档: document_id=%d", document_id)
    chunks = (
        db.query(KnowledgeChunk)
        .filter(
            KnowledgeChunk.document_id == document_id,
            KnowledgeChunk.vector_status.in_([
                CHUNK_VECTOR_STATUS_PENDING,
                CHUNK_VECTOR_STATUS_PROCESSING,
            ]),
        )
        .all()
    )
    if not chunks:
        logger.warning("[vectorize_document] 文档没有待向量化的切块: document_id=%d", document_id)
        return {"total": 0, "success": 0, "failed": 0, "errors": []}

    logger.info("[vectorize_document] 文档待处理切块数: %d", len(chunks))

    return vectorize_chunks(db, chunk_ids=[c.id for c in chunks])


"""辅助函数"""


def _reset_processing_chunks_to_pending(
    db: Session,
    *,
    chunk_ids: list[int] | None = None,
    document_id: int | None = None,
):
    """将遗留的 PROCESSING 切块重置为 PENDING，并同步刷新文档状态。"""
    query = db.query(KnowledgeChunk).filter(
        KnowledgeChunk.vector_status == CHUNK_VECTOR_STATUS_PROCESSING
    )

    if chunk_ids is not None:
        if not chunk_ids:
            return {"reset_count": 0, "document_ids": []}
        query = query.filter(KnowledgeChunk.id.in_(chunk_ids))

    if document_id is not None:
        query = query.filter(KnowledgeChunk.document_id == document_id)

    chunks = query.all()
    if not chunks:
        return {"reset_count": 0, "document_ids": []}

    target_doc_ids = sorted(set(chunk.document_id for chunk in chunks))
    target_chunk_ids = [chunk.id for chunk in chunks]

    (
        db.query(KnowledgeChunk)
        .filter(KnowledgeChunk.id.in_(target_chunk_ids))
        .update(
            {
                "vector_status": CHUNK_VECTOR_STATUS_PENDING,
                "updated_at": datetime.now(),
            },
            synchronize_session=False,
        )
    )
    db.commit()

    _update_document_vector_status(db, target_doc_ids)

    logger.info(
        "[_reset_processing_chunks_to_pending] 已重置遗留状态: chunk_count=%d, document_count=%d",
        len(target_chunk_ids),
        len(target_doc_ids),
    )
    return {
        "reset_count": len(target_chunk_ids),
        "document_ids": target_doc_ids,
    }


def _update_document_vector_status(db: Session, document_ids: list[int]):
    """更新文档的向量状态"""
    for doc_id in set(document_ids):
        doc = db.query(KnowledgeDocument).filter(KnowledgeDocument.id == doc_id).first()
        if not doc:
            continue

        total_chunks = (
            db.query(KnowledgeChunk)
            .filter(KnowledgeChunk.document_id == doc_id)
            .count()
        )
        if total_chunks == 0:
            continue

        # 统计各状态切块数量
        completed_chunks = (
            db.query(KnowledgeChunk)
            .filter(
                KnowledgeChunk.document_id == doc_id,
                KnowledgeChunk.vector_status == CHUNK_VECTOR_STATUS_COMPLETED,
            )
            .count()
        )
        failed_chunks = (
            db.query(KnowledgeChunk)
            .filter(
                KnowledgeChunk.document_id == doc_id,
                KnowledgeChunk.vector_status == CHUNK_VECTOR_STATUS_FAILED,
            )
            .count()
        )
        processing_chunks = (
            db.query(KnowledgeChunk)
            .filter(
                KnowledgeChunk.document_id == doc_id,
                KnowledgeChunk.vector_status == CHUNK_VECTOR_STATUS_PROCESSING,
            )
            .count()
        )
        pending_chunks = (
            db.query(KnowledgeChunk)
            .filter(
                KnowledgeChunk.document_id == doc_id,
                KnowledgeChunk.vector_status == CHUNK_VECTOR_STATUS_PENDING,
            )
            .count()
        )

        if processing_chunks > 0:
            doc.vector_status = VECTOR_STATUS_PROCESSING
            doc.vectorized_at = None
            logger.debug(
                "[_update_document_vector_status] 文档状态更新: doc_id=%d, status=PROCESSING (processing=%d)",
                doc_id,
                processing_chunks,
            )
        elif failed_chunks > 0:
            doc.vector_status = VECTOR_STATUS_FAILED
            doc.vectorized_at = None
            logger.info(
                "[_update_document_vector_status] 文档状态更新: doc_id=%d, status=FAILED (failed=%d, completed=%d)",
                doc_id,
                failed_chunks,
                completed_chunks,
            )
        elif completed_chunks == total_chunks:
            doc.vector_status = VECTOR_STATUS_SUCCESS
            doc.vectorized_at = datetime.now()
            logger.info(
                "[_update_document_vector_status] 文档状态更新: doc_id=%d, status=SUCCESS (completed=%d)",
                doc_id,
                completed_chunks,
            )
        elif pending_chunks > 0:
            doc.vector_status = VECTOR_STATUS_PENDING
            doc.vectorized_at = None
            logger.debug(
                "[_update_document_vector_status] 文档状态更新: doc_id=%d, status=PENDING (pending=%d, completed=%d)",
                doc_id,
                pending_chunks,
                completed_chunks,
            )
        else:
            logger.warning(
                "[_update_document_vector_status] 文档状态未更新: doc_id=%d, total=%d, completed=%d, failed=%d, processing=%d, pending=%d",
                doc_id,
                total_chunks,
                completed_chunks,
                failed_chunks,
                processing_chunks,
                pending_chunks,
            )

        doc.vector_model = settings.EMBEDDING_MODEL
        doc.vector_dim = settings.EMBEDDING_DIM
        doc.vector_version = get_vector_version()
        db.commit()


def get_processing_progress(db: Session):
    total_documents = db.query(KnowledgeDocument).count()

    metadata_loaded = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.metadata_status >= METADATA_STATUS_LOADED
    ).count()

    pdf_uploaded = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.metadata_status == METADATA_STATUS_PDF_UPLOADED
    ).count()

    chunked = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.chunk_status == CHUNK_STATUS_COMPLETED
    ).count()

    vectorized = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.vector_status == VECTOR_STATUS_SUCCESS
    ).count()

    pending_pdf_upload = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.metadata_status == METADATA_STATUS_LOADED
    ).count()

    pending_vectorize = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.chunk_status == CHUNK_STATUS_COMPLETED,
        KnowledgeDocument.vector_status == VECTOR_STATUS_PENDING,
    ).count()

    failed_chunk = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.chunk_status == CHUNK_STATUS_FAILED
    ).count()

    failed_vectorize = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.vector_status == VECTOR_STATUS_FAILED
    ).count()

    progress_percentage = round(pdf_uploaded / total_documents * 100, 2) if total_documents > 0 else 0.0

    recent_processed = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.metadata_status == METADATA_STATUS_PDF_UPLOADED
    ).order_by(KnowledgeDocument.updated_at.desc()).limit(10).all()

    return {
        "total_documents": total_documents,
        "metadata_loaded": metadata_loaded,
        "pdf_uploaded": pdf_uploaded,
        "chunked": chunked,
        "vectorized": vectorized,
        "pending_pdf_upload": pending_pdf_upload,
        "pending_chunk": 0,
        "pending_vectorize": pending_vectorize,
        "failed_chunk": failed_chunk,
        "failed_vectorize": failed_vectorize,
        "progress_percentage": progress_percentage,
        "recent_processed": [
            {
                "id": doc.id,
                "title": doc.title,
                "doc_type": doc.doc_type,
                "status": "chunked" if doc.chunk_status == CHUNK_STATUS_COMPLETED else "failed",
                "updated_at": doc.updated_at.strftime("%Y-%m-%d %H:%M:%S") if doc.updated_at else "",
            }
            for doc in recent_processed
        ],
    }


class VectorizeSubmitResult:
    def __init__(self, document_id: int, status: str, message: str, total_chunks: int = 0):
        self.document_id = document_id
        self.status = status
        self.message = message
        self.total_chunks = total_chunks

    def to_dict(self):
        return {
            "document_id": self.document_id,
            "status": self.status,
            "message": self.message,
            "total_chunks": self.total_chunks,
        }


class BatchVectorizeSubmitResult:
    def __init__(self):
        self.submitted_count: int = 0
        self.chunk_ids: list[int] = []

    def to_dict(self):
        return {
            "submitted": self.submitted_count,
            "message": f"已提交{self.submitted_count}个切块的向量化任务",
        }


def submit_vectorize_task(
    db: Session,
    document_id: int,
    force: bool = False,
) -> VectorizeSubmitResult:
    """提交单个文档的向量化任务"""
    doc = db.query(KnowledgeDocument).filter(KnowledgeDocument.id == document_id).first()
    if doc is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"文档不存在: id={document_id}")

    if doc.chunk_status != CHUNK_STATUS_COMPLETED:
        raise ServiceException(ErrorCode.PARAM_ERROR, "文档尚未完成切块，无法向量化")

    if force:
        reset_count = (
            db.query(KnowledgeChunk)
            .filter(
                KnowledgeChunk.document_id == document_id,
                KnowledgeChunk.vector_status.in_([
                    CHUNK_VECTOR_STATUS_FAILED,
                    CHUNK_VECTOR_STATUS_COMPLETED,
                ]),
            )
            .update({"vector_status": CHUNK_VECTOR_STATUS_PENDING}, synchronize_session=False)
        )
        doc.vector_status = VECTOR_STATUS_PENDING
        db.commit()
        logger.info(
            "[submit_vectorize_task] 强制重置切块状态: document_id=%d, reset_count=%d",
            document_id,
            reset_count,
        )

    chunk_count = db.query(KnowledgeChunk).filter(
        KnowledgeChunk.document_id == document_id,
        KnowledgeChunk.vector_status == CHUNK_VECTOR_STATUS_PENDING,
    ).count()

    if chunk_count == 0:
        return VectorizeSubmitResult(
            document_id=document_id,
            status="skipped",
            message="没有待向量化的切块",
            total_chunks=0,
        )

    doc.vector_status = VECTOR_STATUS_PROCESSING
    db.commit()

    return VectorizeSubmitResult(
        document_id=document_id,
        status="processing",
        message="向量化任务已提交",
        total_chunks=chunk_count,
    )


def submit_batch_vectorize(
    db: Session,
    batch_size: int = 20,
    force: bool = False,
) -> BatchVectorizeSubmitResult:
    """提交批量向量化任务"""
    if force:
        reset_count = (
            db.query(KnowledgeChunk)
            .filter(
                KnowledgeChunk.vector_status.in_([
                    CHUNK_VECTOR_STATUS_FAILED,
                    CHUNK_VECTOR_STATUS_COMPLETED,
                ]),
            )
            .update({"vector_status": CHUNK_VECTOR_STATUS_PENDING}, synchronize_session=False)
        )
        db.commit()
        logger.info(
            "[submit_batch_vectorize] 强制重置所有失败/完成切块: reset_count=%d",
            reset_count,
        )

    # 获取待处理的切块
    pending_chunks = db.query(KnowledgeChunk).filter(
        KnowledgeChunk.vector_status == CHUNK_VECTOR_STATUS_PENDING
    ).limit(batch_size * 10).all()

    if not pending_chunks:
        result = BatchVectorizeSubmitResult()
        result.submitted_count = 0
        return result

    # 立即将切块状态标记为 PROCESSING
    chunk_ids = [c.id for c in pending_chunks]
    db.query(KnowledgeChunk).filter(
        KnowledgeChunk.id.in_(chunk_ids)
    ).update({"vector_status": CHUNK_VECTOR_STATUS_PROCESSING}, synchronize_session=False)

    # 立即将相关文档状态标记为 PROCESSING
    document_ids = list(set([c.document_id for c in pending_chunks]))
    db.query(KnowledgeDocument).filter(
        KnowledgeDocument.id.in_(document_ids)
    ).update({"vector_status": VECTOR_STATUS_PROCESSING}, synchronize_session=False)

    db.commit()

    logger.info(
        "[submit_batch_vectorize] 立即更新状态: chunk_count=%d, document_count=%d",
        len(chunk_ids),
        len(document_ids),
    )

    result = BatchVectorizeSubmitResult()
    result.submitted_count = len(chunk_ids)
    result.chunk_ids = chunk_ids

    return result


def run_vectorize_in_background(document_id: int, batch_size: int = 20):
    """后台执行向量化任务"""
    db = get_background_db_session()
    try:
        vectorize_document(db, document_id)
        logger.info(f"后台向量化完成: document_id={document_id}")
    except Exception as e:
        logger.error(f"后台向量化失败: document_id={document_id}, error={e}")
        try:
            repair_result = _reset_processing_chunks_to_pending(
                db,
                document_id=document_id,
            )
            logger.info(
                "[run_vectorize_in_background] 已回滚遗留状态: document_id=%d, reset_count=%d",
                document_id,
                repair_result["reset_count"],
            )
        except Exception as repair_err:
            logger.error(
                "[run_vectorize_in_background] 回滚遗留状态失败: document_id=%d, error=%s",
                document_id,
                repair_err,
            )
    finally:
        db.close()


def run_vectorize_batch_in_background(batch_size: int = 20, chunk_ids: list[int] | None = None):
    """后台批量执行向量化任务"""
    db = get_background_db_session()
    try:
        vectorize_chunks(db, chunk_ids=chunk_ids, batch_size=batch_size)
        logger.info(f"后台批量向量化完成")
    except Exception as e:
        logger.error(f"后台批量向量化失败: error={e}")
        try:
            repair_result = _reset_processing_chunks_to_pending(
                db,
                chunk_ids=chunk_ids,
            )
            logger.info(
                "[run_vectorize_batch_in_background] 已回滚遗留状态: reset_count=%d, document_count=%d",
                repair_result["reset_count"],
                len(repair_result["document_ids"]),
            )
        except Exception as repair_err:
            logger.error(
                "[run_vectorize_batch_in_background] 回滚遗留状态失败: error=%s",
                repair_err,
            )
    finally:
        db.close()
