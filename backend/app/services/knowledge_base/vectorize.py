"""知识库向量化处理、向量化任务提交与进度查询服务"""
from datetime import datetime

from sqlalchemy import func, select, update
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.database import commit_or_rollback, get_background_db_session
from app.db.milvus import get_kb_collection
from app.models import knowledge_chunk as models_knowledge_chunk
from app.models import knowledge_document as models_knowledge_document
from app.schemas import knowledge_base as schemas_knowledge_base
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model
from app.constants import knowledge_base as constants_kb

logger = setup_logger(__name__)


# ========== 公共入口函数 ==========


def get_vector_version():
    """获取当前向量化版本标识"""
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
        f"[vectorize_chunks] 开始向量化: chunk_ids={chunk_ids}, batch_size={batch_size}"
    )

    stmt = select(models_knowledge_chunk.KnowledgeChunk).where(
        models_knowledge_chunk.KnowledgeChunk.vector_status.in_([
            models_knowledge_chunk.CHUNK_VECTOR_STATUS_PENDING,
            models_knowledge_chunk.CHUNK_VECTOR_STATUS_PROCESSING,
        ])
    )
    if chunk_ids:
        stmt = stmt.where(models_knowledge_chunk.KnowledgeChunk.id.in_(chunk_ids))
    else:
        stmt = stmt.limit(batch_size)

    chunks = db.execute(stmt).scalars().all()
    if not chunks:
        logger.warning("[vectorize_chunks] 没有待向量化的切块")
        return schemas_knowledge_base.VectorizeResultResponse(total=0, success=0, failed=0, errors=[])

    logger.info(f"[vectorize_chunks] 找到 {len(chunks)} 个待处理切块")

    results = {"total": len(chunks), "success": 0, "failed": 0, "errors": []}

    logger.info("[vectorize_chunks] 正在获取 Milvus Collection...")
    collection = get_kb_collection()
    vector_version = get_vector_version()
    logger.info(
        f"[vectorize_chunks] Milvus Collection 获取成功: name={collection.name}, vector_version={vector_version}"
    )

    logger.info("[vectorize_chunks] 正在获取 Embedding 模型...")
    embedding_model = get_model.embedding_model
    logger.info("[vectorize_chunks] Embedding 模型获取成功")

    for chunk in chunks:
        chunk.vector_status = models_knowledge_chunk.CHUNK_VECTOR_STATUS_PROCESSING
    commit_or_rollback(db)

    doc_ids = list(set(c.document_id for c in chunks))
    doc_map = {}
    docs = db.execute(
        select(models_knowledge_document.KnowledgeDocument).where(
            models_knowledge_document.KnowledgeDocument.id.in_(doc_ids)
        )
    ).scalars().all()
    for doc in docs:
        doc_map[doc.id] = doc

    chunk_id_list = [c.id for c in chunks]
    try:
        delete_expr = f'chunk_id in {chunk_id_list}'
        collection.delete(expr=delete_expr)
        logger.info(f"[vectorize_chunks] 批量删除旧向量: count={len(chunk_id_list)}")
    except Exception as del_err:
        logger.warning(f"[vectorize_chunks] 批量删除旧向量失败（非阻塞）: {str(del_err)}")

    succeeded: list[tuple[models_knowledge_chunk.KnowledgeChunk, list[float]]] = []
    for batch_start in range(0, len(chunks), constants_kb.EMBEDDING_BATCH_SIZE):
        batch = chunks[batch_start:batch_start + constants_kb.EMBEDDING_BATCH_SIZE]
        texts = [c.chunk_text for c in batch]
        logger.info(
            f"[vectorize_chunks] Embedding 批次 [{batch_start + 1}-{min(batch_start + constants_kb.EMBEDDING_BATCH_SIZE, len(chunks))}/{len(chunks)}]"
        )

        try:
            embeddings = embedding_model.embed_documents(texts)
            logger.info(f"[vectorize_chunks] Embedding 批次完成: 生成 {len(embeddings)} 个向量")

            for chunk, embedding in zip(batch, embeddings):
                if not embedding or len(embedding) != settings.EMBEDDING_DIM:
                    raise ValueError(
                        f"Embedding 维度不正确: expected={settings.EMBEDDING_DIM}, "
                        f"got={len(embedding) if embedding else 0}"
                    )
                succeeded.append((chunk, embedding))

        except Exception as e:
            logger.warning(
                f"[vectorize_chunks] 批量 Embedding 失败，逐条重试: {str(e)}"
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
                    chunk.vector_status = models_knowledge_chunk.CHUNK_VECTOR_STATUS_FAILED
                    chunk.vector_error_message = str(single_err)[:2000]
                    commit_or_rollback(db)
                    results["failed"] += 1
                    results["errors"].append(
                        {"chunk_id": chunk.id, "error": str(single_err)[:500]}
                    )
                    logger.error(
                        f"切块向量化失败: chunk_id={chunk.id}, error={str(single_err)}",
                        exc_info=True,
                    )

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
                f"[vectorize_chunks] 批量写入 Milvus: collection={collection.name}, count={len(all_insert_data)}"
            )
        except Exception as milvus_err:
            logger.error(f"[vectorize_chunks] Milvus 批量写入失败: {str(milvus_err)}", exc_info=True)
            for chunk, _ in succeeded:
                chunk.vector_status = models_knowledge_chunk.CHUNK_VECTOR_STATUS_FAILED
                chunk.vector_error_message = f"Milvus写入失败: {str(milvus_err)[:1500]}"
                results["failed"] += 1
                results["errors"].append(
                    {"chunk_id": chunk.id, "error": f"Milvus写入失败: {str(milvus_err)[:500]}"}
                )
            commit_or_rollback(db)
            _update_document_vector_status(db, [c.document_id for c in chunks])
            return schemas_knowledge_base.VectorizeResultResponse(**results)

        inserted_chunk_ids = [c.id for c, _ in succeeded]
        try:
            id_lookup = collection.query(
                expr=f"chunk_id in {inserted_chunk_ids}",
                output_fields=["id", "chunk_id"],
                limit=len(inserted_chunk_ids),
            )
            chunk_id_to_milvus_id = {r["chunk_id"]: r["id"] for r in id_lookup}
        except Exception as query_err:
            logger.error(f"[vectorize_chunks] 查询 Milvus auto_id 失败: {str(query_err)}", exc_info=True)
            for chunk, _ in succeeded:
                chunk.vector_status = models_knowledge_chunk.CHUNK_VECTOR_STATUS_FAILED
                chunk.vector_error_message = f"查询Milvus auto_id失败: {str(query_err)[:1500]}"
                results["failed"] += 1
                results["errors"].append(
                    {"chunk_id": chunk.id, "error": f"查询Milvus auto_id失败: {str(query_err)[:500]}"}
                )
            commit_or_rollback(db)
            _update_document_vector_status(db, [c.document_id for c in chunks])
            return schemas_knowledge_base.VectorizeResultResponse(**results)

        if len(chunk_id_to_milvus_id) != len(succeeded):
            logger.warning(
                f"[vectorize_chunks] Milvus 返回 ID 数量异常: expected={len(succeeded)}, actual={len(chunk_id_to_milvus_id)}"
            )

        for chunk, _ in succeeded:
            milvus_id = chunk_id_to_milvus_id.get(chunk.id)
            if milvus_id is None:
                chunk.vector_status = models_knowledge_chunk.CHUNK_VECTOR_STATUS_FAILED
                chunk.vector_error_message = "Milvus写入成功但无法获取auto_id"
                results["failed"] += 1
                results["errors"].append(
                    {"chunk_id": chunk.id, "error": "无法获取Milvus auto_id"}
                )
                logger.error(
                    f"[vectorize_chunks] 无法获取 Milvus auto_id: chunk_id={chunk.id}"
                )
            else:
                chunk.vector_status = models_knowledge_chunk.CHUNK_VECTOR_STATUS_COMPLETED
                chunk.milvus_id = milvus_id
                results["success"] += 1
        commit_or_rollback(db)

    _update_document_vector_status(db, [c.document_id for c in chunks])

    logger.info(
        f"批量向量化完成: total={results['total']}, success={results['success']}, failed={results['failed']}"
    )
    return schemas_knowledge_base.VectorizeResultResponse(**results)


def vectorize_document(db: Session, document_id: int):
    """向量化单个文档的所有待处理切块"""
    logger.info(f"[vectorize_document] 开始向量化文档: document_id={document_id}")
    chunks = db.execute(
        select(models_knowledge_chunk.KnowledgeChunk).where(
            models_knowledge_chunk.KnowledgeChunk.document_id == document_id,
            models_knowledge_chunk.KnowledgeChunk.vector_status.in_([
                models_knowledge_chunk.CHUNK_VECTOR_STATUS_PENDING,
                models_knowledge_chunk.CHUNK_VECTOR_STATUS_PROCESSING,
            ]),
        )
    ).scalars().all()
    if not chunks:
        logger.warning(f"[vectorize_document] 文档没有待向量化的切块: document_id={document_id}")
        return schemas_knowledge_base.VectorizeResultResponse(total=0, success=0, failed=0, errors=[])

    logger.info(f"[vectorize_document] 文档待处理切块数: {len(chunks)}")

    return vectorize_chunks(db, chunk_ids=[c.id for c in chunks])


def get_processing_progress(db: Session):
    """获取向量化处理进度统计"""
    DocModel = models_knowledge_document.KnowledgeDocument

    total_documents = db.scalar(select(func.count(DocModel.id)))

    metadata_loaded = db.scalar(
        select(func.count(DocModel.id)).where(DocModel.metadata_status >= models_knowledge_document.METADATA_STATUS_LOADED)
    )

    pdf_uploaded = db.scalar(
        select(func.count(DocModel.id)).where(DocModel.metadata_status == models_knowledge_document.METADATA_STATUS_PDF_UPLOADED)
    )

    chunked = db.scalar(
        select(func.count(DocModel.id)).where(DocModel.chunk_status == models_knowledge_document.CHUNK_STATUS_COMPLETED)
    )

    vectorized = db.scalar(
        select(func.count(DocModel.id)).where(DocModel.vector_status == models_knowledge_document.VECTOR_STATUS_SUCCESS)
    )

    pending_pdf_upload = db.scalar(
        select(func.count(DocModel.id)).where(DocModel.metadata_status == models_knowledge_document.METADATA_STATUS_LOADED)
    )

    pending_vectorize = db.scalar(
        select(func.count(DocModel.id)).where(
            DocModel.chunk_status == models_knowledge_document.CHUNK_STATUS_COMPLETED,
            DocModel.vector_status == models_knowledge_document.VECTOR_STATUS_PENDING,
        )
    )

    failed_chunk = db.scalar(
        select(func.count(DocModel.id)).where(DocModel.chunk_status == models_knowledge_document.CHUNK_STATUS_FAILED)
    )

    failed_vectorize = db.scalar(
        select(func.count(DocModel.id)).where(DocModel.vector_status == models_knowledge_document.VECTOR_STATUS_FAILED)
    )

    progress_percentage = round(pdf_uploaded / total_documents * 100, 2) if total_documents > 0 else 0.0

    recent_processed = db.execute(
        select(DocModel).where(DocModel.metadata_status == models_knowledge_document.METADATA_STATUS_PDF_UPLOADED)
        .order_by(DocModel.updated_at.desc()).limit(10)
    ).scalars().all()

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
                "status": "chunked" if doc.chunk_status == models_knowledge_document.CHUNK_STATUS_COMPLETED else "failed",
                "updated_at": doc.updated_at.strftime("%Y-%m-%d %H:%M:%S") if doc.updated_at else "",
            }
            for doc in recent_processed
        ],
    }


def submit_and_run_vectorize_task(
    db: Session, document_id: int, force: bool, batch_size: int, background_tasks
):
    """提交单个文档向量化任务并注册后台执行"""
    result = submit_vectorize_task(db, document_id, force)
    if result.status == "processing":
        background_tasks.add_task(
            run_vectorize_in_background, document_id, batch_size
        )
    return result


def submit_and_run_batch_vectorize(
    db: Session, batch_size: int, force: bool, background_tasks
):
    """提交批量向量化任务并注册后台执行"""
    result, chunk_ids = submit_batch_vectorize(db, batch_size, force)
    if result.submitted > 0:
        background_tasks.add_task(
            run_vectorize_batch_in_background, batch_size, chunk_ids
        )
    return result


def submit_vectorize_task(
    db: Session,
    document_id: int,
    force: bool = False,
):
    """提交单个文档的向量化任务"""
    doc = db.scalar(select(models_knowledge_document.KnowledgeDocument).where(models_knowledge_document.KnowledgeDocument.id == document_id))
    if doc is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"文档不存在: id={document_id}")

    if doc.chunk_status != models_knowledge_document.CHUNK_STATUS_COMPLETED:
        raise ServiceException(ErrorCode.PARAM_ERROR, "文档尚未完成切块，无法向量化")

    if force:
        reset_count = (
            db.execute(
                update(models_knowledge_chunk.KnowledgeChunk)
                .where(
                    models_knowledge_chunk.KnowledgeChunk.document_id == document_id,
                    models_knowledge_chunk.KnowledgeChunk.vector_status.in_([
                        models_knowledge_chunk.CHUNK_VECTOR_STATUS_FAILED,
                        models_knowledge_chunk.CHUNK_VECTOR_STATUS_COMPLETED,
                    ]),
                )
                .values(vector_status=models_knowledge_chunk.CHUNK_VECTOR_STATUS_PENDING)
            ).rowcount
        )
        doc.vector_status = models_knowledge_document.VECTOR_STATUS_PENDING
        commit_or_rollback(db)
        logger.info(
            f"[submit_vectorize_task] 强制重置切块状态: document_id={document_id}, reset_count={reset_count}"
        )

    chunk_count = db.scalar(
        select(func.count()).select_from(models_knowledge_chunk.KnowledgeChunk).where(
            models_knowledge_chunk.KnowledgeChunk.document_id == document_id,
            models_knowledge_chunk.KnowledgeChunk.vector_status == models_knowledge_chunk.CHUNK_VECTOR_STATUS_PENDING,
        )
    )

    if chunk_count == 0:
        return schemas_knowledge_base.VectorizeSubmitResponse(
            document_id=document_id,
            status="skipped",
            message="没有待向量化的切块",
            total_chunks=0,
        )

    doc.vector_status = models_knowledge_document.VECTOR_STATUS_PROCESSING
    commit_or_rollback(db)

    return schemas_knowledge_base.VectorizeSubmitResponse(
        document_id=document_id,
        status="processing",
        message="向量化任务已提交",
        total_chunks=chunk_count,
    )


def submit_batch_vectorize(
    db: Session,
    batch_size: int = 20,
    force: bool = False,
):
    """提交批量向量化任务"""
    if force:
        reset_count = (
            db.execute(
                update(models_knowledge_chunk.KnowledgeChunk)
                .where(
                    models_knowledge_chunk.KnowledgeChunk.vector_status.in_([
                        models_knowledge_chunk.CHUNK_VECTOR_STATUS_FAILED,
                        models_knowledge_chunk.CHUNK_VECTOR_STATUS_COMPLETED,
                    ]),
                )
                .values(vector_status=models_knowledge_chunk.CHUNK_VECTOR_STATUS_PENDING)
            ).rowcount
        )
        commit_or_rollback(db)
        logger.info(
            f"[submit_batch_vectorize] 强制重置所有失败/完成切块: reset_count={reset_count}"
        )

    pending_chunks = db.execute(
        select(models_knowledge_chunk.KnowledgeChunk).where(
            models_knowledge_chunk.KnowledgeChunk.vector_status == models_knowledge_chunk.CHUNK_VECTOR_STATUS_PENDING
        ).limit(batch_size * 10)
    ).scalars().all()

    if not pending_chunks:
        return schemas_knowledge_base.BatchVectorizeSubmitResponse(submitted=0, message="没有待处理的切块")

    chunk_ids = [c.id for c in pending_chunks]
    db.execute(
        update(models_knowledge_chunk.KnowledgeChunk).where(
            models_knowledge_chunk.KnowledgeChunk.id.in_(chunk_ids)
        ).values(vector_status=models_knowledge_chunk.CHUNK_VECTOR_STATUS_PROCESSING)
    )

    document_ids = list(set([c.document_id for c in pending_chunks]))
    db.execute(
        update(models_knowledge_document.KnowledgeDocument).where(
            models_knowledge_document.KnowledgeDocument.id.in_(document_ids)
        ).values(vector_status=models_knowledge_document.VECTOR_STATUS_PROCESSING)
    )

    commit_or_rollback(db)

    logger.info(
        f"[submit_batch_vectorize] 立即更新状态: chunk_count={len(chunk_ids)}, document_count={len(document_ids)}"
    )

    return schemas_knowledge_base.BatchVectorizeSubmitResponse(
        submitted=len(chunk_ids),
        message=f"已提交{len(chunk_ids)}个切块的向量化任务",
    ), chunk_ids


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
                f"[run_vectorize_in_background] 已回滚遗留状态: document_id={document_id}, reset_count={repair_result['reset_count']}"
            )
        except Exception as repair_err:
            logger.error(
                f"[run_vectorize_in_background] 回滚遗留状态失败: document_id={document_id}, error={repair_err}"
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
                f"[run_vectorize_batch_in_background] 已回滚遗留状态: reset_count={repair_result['reset_count']}, document_count={len(repair_result['document_ids'])}"
            )
        except Exception as repair_err:
            logger.error(
                f"[run_vectorize_batch_in_background] 回滚遗留状态失败: error={repair_err}"
            )
    finally:
        db.close()


"""辅助函数"""


def _reset_processing_chunks_to_pending(
    db: Session,
    *,
    chunk_ids: list[int] | None = None,
    document_id: int | None = None,
):
    """将遗留的 PROCESSING 切块重置为 PENDING，并同步刷新文档状态。"""
    stmt = select(models_knowledge_chunk.KnowledgeChunk).where(
        models_knowledge_chunk.KnowledgeChunk.vector_status == models_knowledge_chunk.CHUNK_VECTOR_STATUS_PROCESSING
    )

    if chunk_ids is not None:
        if not chunk_ids:
            return {"reset_count": 0, "document_ids": []}
        stmt = stmt.where(models_knowledge_chunk.KnowledgeChunk.id.in_(chunk_ids))

    if document_id is not None:
        stmt = stmt.where(models_knowledge_chunk.KnowledgeChunk.document_id == document_id)

    chunks = db.execute(stmt).scalars().all()
    if not chunks:
        return {"reset_count": 0, "document_ids": []}

    target_doc_ids = sorted(set(chunk.document_id for chunk in chunks))
    target_chunk_ids = [chunk.id for chunk in chunks]

    db.execute(
        update(models_knowledge_chunk.KnowledgeChunk)
        .where(models_knowledge_chunk.KnowledgeChunk.id.in_(target_chunk_ids))
        .values(
            vector_status=models_knowledge_chunk.CHUNK_VECTOR_STATUS_PENDING,
            updated_at=datetime.now(),
        )
    )
    commit_or_rollback(db)

    _update_document_vector_status(db, target_doc_ids)

    logger.info(
        f"[_reset_processing_chunks_to_pending] 已重置遗留状态: chunk_count={len(target_chunk_ids)}, document_count={len(target_doc_ids)}"
    )
    return {
        "reset_count": len(target_chunk_ids),
        "document_ids": target_doc_ids,
    }


def _update_document_vector_status(db: Session, document_ids: list[int]):
    """更新文档的向量状态"""
    for doc_id in set(document_ids):
        doc = db.scalar(select(models_knowledge_document.KnowledgeDocument).where(models_knowledge_document.KnowledgeDocument.id == doc_id))
        if not doc:
            continue

        ChunkModel = models_knowledge_chunk.KnowledgeChunk

        total_chunks = db.scalar(
            select(func.count(ChunkModel.id)).where(ChunkModel.document_id == doc_id)
        )
        if total_chunks == 0:
            continue

        completed_chunks = db.scalar(
            select(func.count(ChunkModel.id)).where(
                ChunkModel.document_id == doc_id,
                ChunkModel.vector_status == models_knowledge_chunk.CHUNK_VECTOR_STATUS_COMPLETED,
            )
        )
        failed_chunks = db.scalar(
            select(func.count(ChunkModel.id)).where(
                ChunkModel.document_id == doc_id,
                ChunkModel.vector_status == models_knowledge_chunk.CHUNK_VECTOR_STATUS_FAILED,
            )
        )
        processing_chunks = db.scalar(
            select(func.count(ChunkModel.id)).where(
                ChunkModel.document_id == doc_id,
                ChunkModel.vector_status == models_knowledge_chunk.CHUNK_VECTOR_STATUS_PROCESSING,
            )
        )
        pending_chunks = db.scalar(
            select(func.count(ChunkModel.id)).where(
                ChunkModel.document_id == doc_id,
                ChunkModel.vector_status == models_knowledge_chunk.CHUNK_VECTOR_STATUS_PENDING,
            )
        )

        if processing_chunks > 0:
            doc.vector_status = models_knowledge_document.VECTOR_STATUS_PROCESSING
            doc.vectorized_at = None
            logger.debug(
                f"[_update_document_vector_status] 文档状态更新: doc_id={doc_id}, status=PROCESSING (processing={processing_chunks})"
            )
        elif failed_chunks > 0:
            doc.vector_status = models_knowledge_document.VECTOR_STATUS_FAILED
            doc.vectorized_at = None
            logger.info(
                f"[_update_document_vector_status] 文档状态更新: doc_id={doc_id}, status=FAILED (failed={failed_chunks}, completed={completed_chunks})"
            )
        elif completed_chunks == total_chunks:
            doc.vector_status = models_knowledge_document.VECTOR_STATUS_SUCCESS
            doc.vectorized_at = datetime.now()
            logger.info(
                f"[_update_document_vector_status] 文档状态更新: doc_id={doc_id}, status=SUCCESS (completed={completed_chunks})"
            )
        elif pending_chunks > 0:
            doc.vector_status = models_knowledge_document.VECTOR_STATUS_PENDING
            doc.vectorized_at = None
            logger.debug(
                f"[_update_document_vector_status] 文档状态更新: doc_id={doc_id}, status=PENDING (pending={pending_chunks}, completed={completed_chunks})"
            )
        else:
            logger.warning(
                f"[_update_document_vector_status] 文档状态未更新: doc_id={doc_id}, total={total_chunks}, completed={completed_chunks}, failed={failed_chunks}, processing={processing_chunks}, pending={pending_chunks}"
            )

        doc.vector_model = settings.EMBEDDING_MODEL
        doc.vector_dim = settings.EMBEDDING_DIM
        doc.vector_version = get_vector_version()
        commit_or_rollback(db)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(constants_kb.RETRYABLE_EXCEPTIONS),
    reraise=True,
)
def _embed_with_retry(embedding_model, text: str):
    """带重试机制的embedding调用"""
    return embedding_model.embed_query(text)
