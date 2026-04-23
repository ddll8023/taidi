import re
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from pypdf import PdfReader
from sqlalchemy import func as sa_func
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
    compute_chunk_hash,
)
from app.models.knowledge_document import (
    CHUNK_STATUS_COMPLETED,
    CHUNK_STATUS_FAILED,
    CHUNK_STATUS_PENDING,
    CHUNK_STATUS_PROCESSING,
    METADATA_STATUS_LOADED,
    METADATA_STATUS_NOT_LOADED,
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

VALID_KB_DOC_TYPES = {"RESEARCH_REPORT", "INDUSTRY_REPORT"}


# 定义可重试的异常类型（网络错误、SSL错误）
RETRYABLE_EXCEPTIONS = (
    ConnectionError,
    OSError,
    Exception,  # 包含SSLError等网络异常
)


@retry(
    stop=stop_after_attempt(3),  # 最多重试3次
    wait=wait_exponential(multiplier=1, min=2, max=10),  # 指数退避：2s, 4s, 8s
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    reraise=True,
)
def _embed_with_retry(embedding_model, text: str) -> list[float]:
    """带重试机制的embedding调用"""
    return embedding_model.embed_query(text)


def extract_pdf_full_text(file_path: str) -> tuple[str, int]:
    reader = PdfReader(file_path)
    page_count = len(reader.pages)
    texts: list[str] = []
    for page in reader.pages:
        page_text = page.extract_text() or ""
        cleaned = _clean_pdf_text(page_text)
        if cleaned:
            texts.append(cleaned)
    full_text = "\n\n".join(texts)
    return full_text, page_count


def extract_pdf_pages(file_path: str) -> list[dict]:
    reader = PdfReader(file_path)
    pages: list[dict] = []
    for i, page in enumerate(reader.pages):
        page_text = page.extract_text() or ""
        cleaned = _clean_pdf_text(page_text)
        if cleaned:
            pages.append(
                {
                    "page_no": i + 1,
                    "text": cleaned,
                }
            )
    return pages


def _clean_pdf_text(raw: str) -> str:
    text = raw.strip()
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def chunk_text(
    text: str,
    chunk_size: int | None = None,
    chunk_overlap: int | None = None,
) -> list[str]:
    size = chunk_size or settings.CHUNK_SIZE
    overlap = chunk_overlap or settings.CHUNK_OVERLAP
    if overlap >= size:
        overlap = size // 10

    if not text.strip():
        return []

    chunks: list[str] = []
    start = 0
    text_len = len(text)

    while start < text_len:
        end = start + size
        chunk = text[start:end]

        if end < text_len and overlap > 0:
            boundary = _find_split_boundary(chunk)
            if boundary is not None and boundary > size * 0.3:
                chunk = text[start : start + boundary]
                end = start + boundary

        chunk = chunk.strip()
        if chunk:
            chunks.append(chunk)

        if end >= text_len:
            break
        start = end - overlap

    return chunks


def chunk_pages(
    pages: list[dict],
    chunk_size: int | None = None,
    chunk_overlap: int | None = None,
) -> list[dict]:
    size = chunk_size or settings.CHUNK_SIZE
    overlap = chunk_overlap or settings.CHUNK_OVERLAP

    chunks: list[dict] = []
    current_text = ""
    current_page_no = None

    for page_info in pages:
        page_no = page_info["page_no"]
        page_text = page_info["text"]

        if current_page_no is None:
            current_page_no = page_no

        if current_text:
            current_text += "\n\n"
        current_text += page_text

        while len(current_text) >= size:
            split_pos = size
            boundary = _find_split_boundary(current_text[:size])
            if boundary is not None and boundary > size * 0.3:
                split_pos = boundary

            chunk_content = current_text[:split_pos].strip()
            if chunk_content:
                chunks.append(
                    {
                        "page_no": current_page_no,
                        "text": chunk_content,
                    }
                )

            remaining = current_text[split_pos:]
            if overlap > 0 and len(remaining) > overlap:
                current_text = remaining[-overlap:]
            else:
                current_text = remaining
            current_page_no = page_no

    if current_text.strip():
        chunks.append(
            {
                "page_no": current_page_no,
                "text": current_text.strip(),
            }
        )

    return chunks


def _find_split_boundary(text: str) -> int | None:
    for sep in ["\n\n", "。", "！", "？", ".", "!", "?", "；", ";", "\n", "，", ","]:
        pos = text.rfind(sep)
        if pos > 0:
            return pos + len(sep)
    return None


def register_document(
    db: Session,
    *,
    doc_type: str,
    title: str,
    source_path: str,
    stock_code: str | None = None,
    stock_abbr: str | None = None,
    publish_date: str | None = None,
    org_name: str | None = None,
    industry_name: str | None = None,
    financial_report_id: int | None = None,
    page_count: int | None = None,
) -> KnowledgeDocument:
    existing = (
        db.query(KnowledgeDocument)
        .filter(
            KnowledgeDocument.source_path == source_path,
        )
        .first()
    )
    if existing:
        logger.info(
            "文档已存在，跳过注册: source_path=%s, id=%d", source_path, existing.id
        )
        return existing

    doc = KnowledgeDocument(
        doc_type=doc_type,
        title=title,
        source_path=source_path,
        stock_code=stock_code,
        stock_abbr=stock_abbr,
        publish_date=publish_date,
        org_name=org_name,
        industry_name=industry_name,
        financial_report_id=financial_report_id,
        page_count=page_count,
        chunk_count=0,
        chunk_status=CHUNK_STATUS_PENDING,
        vector_status=VECTOR_STATUS_PENDING,
    )
    db.add(doc)
    db.commit()
    db.refresh(doc)
    logger.info("文档注册成功: id=%d, title=%s, doc_type=%s", doc.id, title, doc_type)
    return doc


def chunk_document(db: Session, document_id: int) -> list[KnowledgeChunk]:
    doc = (
        db.query(KnowledgeDocument).filter(KnowledgeDocument.id == document_id).first()
    )
    if doc is None:
        raise ServiceException(
            ErrorCode.DATA_NOT_FOUND, f"文档不存在: id={document_id}"
        )

    if doc.chunk_status == CHUNK_STATUS_COMPLETED:
        existing_chunks = (
            db.query(KnowledgeChunk)
            .filter(KnowledgeChunk.document_id == document_id)
            .order_by(KnowledgeChunk.chunk_index)
            .all()
        )
        logger.info(
            "文档已完成切块，跳过: id=%d, chunks=%d", document_id, len(existing_chunks)
        )
        return list(existing_chunks)

    doc.chunk_status = CHUNK_STATUS_PROCESSING
    db.commit()

    try:
        reader = PdfReader(doc.source_path)
        total_pages = len(reader.pages)
        if doc.page_count is None:
            doc.page_count = total_pages

        pages: list[dict] = []
        for i, page in enumerate(reader.pages):
            page_text = page.extract_text() or ""
            cleaned = _clean_pdf_text(page_text)
            if cleaned:
                pages.append({"page_no": i + 1, "text": cleaned})

        if not pages:
            raise ServiceException(
                ErrorCode.AI_SERVICE_ERROR,
                f"PDF 未提取到有效文本: source_path={doc.source_path}",
            )

        page_chunks = chunk_pages(pages)

        existing_count = (
            db.query(KnowledgeChunk)
            .filter(KnowledgeChunk.document_id == document_id)
            .count()
        )
        if existing_count > 0:
            db.query(KnowledgeChunk).filter(
                KnowledgeChunk.document_id == document_id
            ).delete()
            db.flush()

        chunk_records: list[KnowledgeChunk] = []
        for idx, chunk_info in enumerate(page_chunks):
            chunk_text_content = chunk_info["text"]
            chunk_hash = compute_chunk_hash(chunk_text_content)
            record = KnowledgeChunk(
                document_id=document_id,
                page_no=chunk_info["page_no"],
                chunk_index=idx,
                chunk_text=chunk_text_content,
                chunk_hash=chunk_hash,
                char_count=len(chunk_text_content),
                vector_status=CHUNK_VECTOR_STATUS_PENDING,
            )
            db.add(record)
            chunk_records.append(record)

        doc.chunk_count = len(page_chunks)
        doc.chunk_status = CHUNK_STATUS_COMPLETED
        db.commit()

        for record in chunk_records:
            db.refresh(record)

        logger.info(
            "文档切块完成: id=%d, chunks=%d, pages=%d",
            document_id,
            len(page_chunks),
            len(pages),
        )
        return chunk_records

    except Exception as e:
        doc.chunk_status = CHUNK_STATUS_FAILED
        doc.chunk_error_message = str(e)[:2000]
        db.commit()
        logger.error("文档切块失败: id=%d, error=%s", document_id, str(e))
        raise


def register_and_chunk_document(
    db: Session,
    *,
    doc_type: str,
    title: str,
    source_path: str,
    stock_code: str | None = None,
    stock_abbr: str | None = None,
    publish_date: str | None = None,
    org_name: str | None = None,
    industry_name: str | None = None,
    financial_report_id: int | None = None,
) -> tuple[KnowledgeDocument, list[KnowledgeChunk]]:
    doc = register_document(
        db,
        doc_type=doc_type,
        title=title,
        source_path=source_path,
        stock_code=stock_code,
        stock_abbr=stock_abbr,
        publish_date=publish_date,
        org_name=org_name,
        industry_name=industry_name,
        financial_report_id=financial_report_id,
    )

    chunks = chunk_document(db, doc.id)
    return doc, chunks


def get_document_list(
    db: Session,
    *,
    doc_type: str | None = None,
    stock_code: str | None = None,
    stock_abbr: str | None = None,
    metadata_status: int | None = None,
    chunk_status: int | None = None,
    vector_status: int | None = None,
    page: int = 1,
    page_size: int = 20,
) -> tuple[list[KnowledgeDocument], int]:
    query = db.query(KnowledgeDocument)

    if doc_type:
        query = query.filter(KnowledgeDocument.doc_type == doc_type)
    if stock_code:
        query = query.filter(KnowledgeDocument.stock_code == stock_code)
    if stock_abbr:
        query = query.filter(KnowledgeDocument.stock_abbr.like(f"%{stock_abbr}%"))
    if metadata_status is not None:
        query = query.filter(KnowledgeDocument.metadata_status == metadata_status)
    if chunk_status is not None:
        query = query.filter(KnowledgeDocument.chunk_status == chunk_status)
    if vector_status is not None:
        query = query.filter(KnowledgeDocument.vector_status == vector_status)

    total = query.count()
    items = (
        query.order_by(KnowledgeDocument.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    return items, total


def get_chunk_list(
    db: Session,
    *,
    document_id: int | None = None,
    vector_status: int | None = None,
    page: int = 1,
    page_size: int = 20,
) -> tuple[list[KnowledgeChunk], int]:
    query = db.query(KnowledgeChunk)

    if document_id:
        query = query.filter(KnowledgeChunk.document_id == document_id)
    if vector_status is not None:
        query = query.filter(KnowledgeChunk.vector_status == vector_status)

    total = query.count()
    items = (
        query.order_by(KnowledgeChunk.document_id, KnowledgeChunk.chunk_index)
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    return items, total


def get_vector_version() -> str:
    return (
        f"{settings.EMBEDDING_MODEL}:"
        f"{settings.EMBEDDING_DIM}:"
        f"{settings.CHUNK_SIZE}:"
        f"{settings.CHUNK_OVERLAP}"
    )


EMBEDDING_BATCH_SIZE = 25  # DashScope 单次批量 Embedding 最大条数


def vectorize_chunks(
    db: Session,
    chunk_ids: list[int] | None = None,
    batch_size: int = 10,
) -> dict:
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


# ─────────────────────────────────────────────────────────────────────────────
# 增量处理模式：系统初始化 + 增量上传PDF
# ─────────────────────────────────────────────────────────────────────────────
async def init_knowledge_base_metadata(
    db: Session,
    stock_excel_content: bytes,
    industry_excel_content: bytes,
    force_reload: bool = False,
) -> dict:
    from app.services.fujian5_data_processor import (
        parse_stock_research_excel_from_upload,
        parse_industry_research_excel_from_upload,
    )

    existing_count = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.metadata_status >= METADATA_STATUS_LOADED
    ).count()

    if existing_count > 0 and not force_reload:
        return {
            "success": False,
            "message": f"系统已初始化（{existing_count}条元数据），如需重新加载请使用force_reload=true",
            "stock_metadata_count": 0,
            "industry_metadata_count": 0,
            "total_count": existing_count,
            "duplicates": 0,
            "errors": [],
        }

    try:
        stock_records, _ = parse_stock_research_excel_from_upload(stock_excel_content)
        industry_records, _ = parse_industry_research_excel_from_upload(industry_excel_content)
    except Exception as e:
        logger.error(f"解析 Excel 元数据失败: {e}")
        return {
            "success": False,
            "message": f"解析Excel失败: {str(e)[:200]}",
            "stock_metadata_count": 0,
            "industry_metadata_count": 0,
            "total_count": 0,
            "duplicates": 0,
            "errors": [{"phase": "excel_parsing", "error": str(e)[:500]}],
        }

    if force_reload:
        db.query(KnowledgeChunk).filter(
            KnowledgeChunk.document_id.in_(
                db.query(KnowledgeDocument.id).filter(
                    KnowledgeDocument.metadata_status >= METADATA_STATUS_LOADED
                )
            )
        ).delete(synchronize_session=False)
        db.query(KnowledgeDocument).filter(
            KnowledgeDocument.metadata_status >= METADATA_STATUS_LOADED
        ).delete(synchronize_session=False)
        db.commit()

    documents: list[KnowledgeDocument] = []
    duplicates = 0
    seen_keys: set[tuple[str, str]] = set()

    for record in stock_records:
        title = record.get("title", "").strip()
        if not title:
            continue

        key = ("RESEARCH_REPORT", title)
        if key in seen_keys:
            duplicates += 1
            continue

        existing = db.query(KnowledgeDocument).filter(
            KnowledgeDocument.title == title,
            KnowledgeDocument.doc_type == "RESEARCH_REPORT",
        ).first()

        if existing:
            duplicates += 1
            continue

        doc = KnowledgeDocument(
            doc_type="RESEARCH_REPORT",
            title=title,
            source_path="pending_upload",
            stock_code=record.get("stockCode"),
            stock_abbr=record.get("stockName"),
            org_name=record.get("orgName"),
            publish_date=record.get("publishDate"),
            researcher=record.get("researcher"),
            em_rating_name=record.get("emRatingName"),
            predict_this_year_eps=record.get("predictThisYearEps"),
            predict_this_year_pe=record.get("predictThisYearPe"),
            metadata_status=METADATA_STATUS_LOADED,
            chunk_status=CHUNK_STATUS_PENDING,
            vector_status=VECTOR_STATUS_PENDING,
            chunk_count=0,
        )
        documents.append(doc)
        seen_keys.add(key)

    for record in industry_records:
        title = record.get("title", "").strip()
        if not title:
            continue

        key = ("INDUSTRY_REPORT", title)
        if key in seen_keys:
            duplicates += 1
            continue

        existing = db.query(KnowledgeDocument).filter(
            KnowledgeDocument.title == title,
            KnowledgeDocument.doc_type == "INDUSTRY_REPORT",
        ).first()

        if existing:
            duplicates += 1
            continue

        doc = KnowledgeDocument(
            doc_type="INDUSTRY_REPORT",
            title=title,
            source_path="pending_upload",
            org_name=record.get("orgName"),
            publish_date=record.get("publishDate"),
            industry_name=record.get("industryName"),
            researcher=record.get("researcher"),
            metadata_status=METADATA_STATUS_LOADED,
            chunk_status=CHUNK_STATUS_PENDING,
            vector_status=VECTOR_STATUS_PENDING,
            chunk_count=0,
        )
        documents.append(doc)
        seen_keys.add(key)

    if documents:
        db.bulk_save_objects(documents)
        db.commit()

    stock_count = len([d for d in documents if d.doc_type == "RESEARCH_REPORT"])
    industry_count = len([d for d in documents if d.doc_type == "INDUSTRY_REPORT"])

    logger.info(
        f"系统初始化完成: 个股研报 {stock_count} 条, 行业研报 {industry_count} 条, "
        f"重复 {duplicates} 条"
    )

    return {
        "success": True,
        "message": "系统初始化成功",
        "stock_metadata_count": stock_count,
        "industry_metadata_count": industry_count,
        "total_count": len(documents),
        "duplicates": duplicates,
        "errors": [],
    }


async def upload_pdf_incremental(
    db: Session,
    pdfs: list,
    doc_type: str,
    manual_match: dict | None = None,
) -> dict:
    import os
    import uuid

    processed_count = 0
    failed_documents: list[dict] = []
    errors: list[dict] = []

    for pdf_file in pdfs:
        filename = getattr(pdf_file, "filename", None)
        if not filename:
            failed_documents.append({
                "pdf_name": "unknown",
                "reason": "PDF文件名为空",
                "suggestion": "请检查上传的文件",
            })
            continue

        pdf_name = os.path.splitext(filename)[0]

        doc = None

        if manual_match and pdf_name in manual_match:
            matched_title = manual_match[pdf_name]
            doc = db.query(KnowledgeDocument).filter(
                KnowledgeDocument.title == matched_title,
                KnowledgeDocument.doc_type == doc_type,
                KnowledgeDocument.metadata_status == METADATA_STATUS_LOADED,
            ).first()

        if not doc:
            doc = db.query(KnowledgeDocument).filter(
                KnowledgeDocument.title == pdf_name,
                KnowledgeDocument.doc_type == doc_type,
                KnowledgeDocument.metadata_status == METADATA_STATUS_LOADED,
            ).first()

        if not doc:
            failed_documents.append({
                "pdf_name": pdf_name,
                "reason": "未找到匹配的Excel元数据",
                "suggestion": "请检查文件名是否与Excel中的title字段一致",
            })
            continue

        try:
            upload_dir = os.path.join(settings.fujian5_UPLOAD_DIR, doc_type)
            os.makedirs(upload_dir, exist_ok=True)
            unique_filename = f"{uuid.uuid4().hex[:8]}_{filename}"
            file_path = os.path.join(upload_dir, unique_filename)

            content = await pdf_file.read()
            with open(file_path, "wb") as f:
                f.write(content)

            reader = PdfReader(file_path)
            total_pages = len(reader.pages)

            pages: list[dict] = []
            for i, page in enumerate(reader.pages):
                page_text = page.extract_text() or ""
                cleaned = _clean_pdf_text(page_text)
                if cleaned:
                    pages.append({"page_no": i + 1, "text": cleaned})

            if not pages:
                doc.error_message = "PDF未提取到有效文本"
                db.commit()
                failed_documents.append({
                    "pdf_name": pdf_name,
                    "reason": "PDF未提取到有效文本",
                    "suggestion": "请检查PDF文件完整性",
                })
                continue

            page_chunks = chunk_pages(pages)

            existing_count = db.query(KnowledgeChunk).filter(
                KnowledgeChunk.document_id == doc.id
            ).count()
            if existing_count > 0:
                db.query(KnowledgeChunk).filter(
                    KnowledgeChunk.document_id == doc.id
                ).delete()
                db.flush()

            chunk_records: list[KnowledgeChunk] = []
            for idx, chunk_info in enumerate(page_chunks):
                chunk_text_content = chunk_info["text"]
                chunk_hash = compute_chunk_hash(chunk_text_content)
                record = KnowledgeChunk(
                    document_id=doc.id,
                    page_no=chunk_info["page_no"],
                    chunk_index=idx,
                    chunk_text=chunk_text_content,
                    chunk_hash=chunk_hash,
                    char_count=len(chunk_text_content),
                    vector_status=CHUNK_VECTOR_STATUS_PENDING,
                )
                db.add(record)
                chunk_records.append(record)

            doc.source_path = file_path
            doc.metadata_status = METADATA_STATUS_PDF_UPLOADED
            doc.chunk_status = CHUNK_STATUS_COMPLETED
            doc.chunk_count = len(page_chunks)
            doc.page_count = total_pages
            doc.error_message = None
            db.commit()

            processed_count += 1

            logger.info(
                f"增量上传PDF处理完成: title={pdf_name}, chunks={len(page_chunks)}"
            )

        except Exception as e:
            doc.error_message = str(e)[:2000]
            db.commit()
            failed_documents.append({
                "pdf_name": pdf_name,
                "reason": str(e)[:200],
                "suggestion": "处理失败，请检查PDF文件完整性",
            })
            errors.append({
                "file": filename,
                "error": str(e)[:500],
            })
            logger.error(f"增量上传PDF处理失败: {filename}, error={str(e)}")

    total_processed = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.metadata_status == METADATA_STATUS_PDF_UPLOADED
    ).count()

    total_pending = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.metadata_status == METADATA_STATUS_LOADED
    ).count()

    return {
        "success": True,
        "message": f"成功处理{processed_count}个文档"
        + (f"，{len(failed_documents)}个失败" if failed_documents else ""),
        "processed_count": processed_count,
        "failed_count": len(failed_documents),
        "total_processed": total_processed,
        "total_pending": total_pending,
        "failed_documents": failed_documents,
        "errors": errors,
    }


def get_processing_progress(db: Session) -> dict:
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


def get_init_status(db: Session) -> dict:
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


async def upload_single_pdf_for_document(
    db: Session,
    document_id: int,
    pdf_file,
) -> dict:
    import os
    import uuid

    doc = db.query(KnowledgeDocument).filter(KnowledgeDocument.id == document_id).first()
    if not doc:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "文档不存在")

    if doc.metadata_status != METADATA_STATUS_LOADED:
        raise ServiceException(ErrorCode.PARAM_ERROR, "文档元数据未加载或PDF已上传")

    filename = getattr(pdf_file, "filename", "")
    pdf_name = os.path.splitext(filename)[0] if filename else ""

    upload_dir = os.path.join(settings.fujian5_UPLOAD_DIR, doc.doc_type)
    os.makedirs(upload_dir, exist_ok=True)
    unique_filename = f"{uuid.uuid4().hex[:8]}_{filename}"
    file_path = os.path.join(upload_dir, unique_filename)

    content = await pdf_file.read()
    with open(file_path, "wb") as f:
        f.write(content)

    reader = PdfReader(file_path)
    total_pages = len(reader.pages)

    pages: list[dict] = []
    for i, page in enumerate(reader.pages):
        page_text = page.extract_text() or ""
        cleaned = _clean_pdf_text(page_text)
        if cleaned:
            pages.append({"page_no": i + 1, "text": cleaned})

    if not pages:
        doc.error_message = "PDF未提取到有效文本"
        db.commit()
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "PDF未提取到有效文本")

    page_chunks = chunk_pages(pages)

    existing_count = db.query(KnowledgeChunk).filter(
        KnowledgeChunk.document_id == doc.id
    ).count()
    if existing_count > 0:
        db.query(KnowledgeChunk).filter(
            KnowledgeChunk.document_id == doc.id
        ).delete()
        db.flush()

    for idx, chunk_info in enumerate(page_chunks):
        chunk_text_content = chunk_info["text"]
        chunk_hash = compute_chunk_hash(chunk_text_content)
        record = KnowledgeChunk(
            document_id=doc.id,
            page_no=chunk_info["page_no"],
            chunk_index=idx,
            chunk_text=chunk_text_content,
            chunk_hash=chunk_hash,
            char_count=len(chunk_text_content),
            vector_status=CHUNK_VECTOR_STATUS_PENDING,
        )
        db.add(record)

    doc.source_path = file_path
    doc.metadata_status = METADATA_STATUS_PDF_UPLOADED
    doc.chunk_status = CHUNK_STATUS_COMPLETED
    doc.chunk_count = len(page_chunks)
    doc.page_count = total_pages
    doc.error_message = None
    db.commit()

    logger.info(
        f"单文档PDF上传处理完成: document_id={document_id}, title={doc.title}, chunks={len(page_chunks)}"
    )

    return {
        "success": True,
        "message": "文档PDF上传成功",
        "document_id": document_id,
        "chunk_count": len(page_chunks),
    }


async def retry_failed_document(
    db: Session,
    document_id: int,
    pdf_file,
) -> dict:
    doc = db.query(KnowledgeDocument).filter(KnowledgeDocument.id == document_id).first()
    if not doc:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "文档不存在")

    if doc.chunk_status != CHUNK_STATUS_FAILED and doc.metadata_status != METADATA_STATUS_PDF_UPLOADED:
        if doc.metadata_status == METADATA_STATUS_LOADED:
            return await upload_single_pdf_for_document(db, document_id, pdf_file)
        raise ServiceException(ErrorCode.PARAM_ERROR, "文档未标记为失败，无需重试")

    doc.chunk_status = CHUNK_STATUS_PENDING
    doc.error_message = None
    if doc.metadata_status == METADATA_STATUS_LOADED:
        return await upload_single_pdf_for_document(db, document_id, pdf_file)

    import os
    import uuid

    filename = getattr(pdf_file, "filename", "")
    upload_dir = os.path.join(settings.fujian5_UPLOAD_DIR, doc.doc_type)
    os.makedirs(upload_dir, exist_ok=True)
    unique_filename = f"{uuid.uuid4().hex[:8]}_{filename}"
    file_path = os.path.join(upload_dir, unique_filename)

    content = await pdf_file.read()
    with open(file_path, "wb") as f:
        f.write(content)

    try:
        reader = PdfReader(file_path)
        total_pages = len(reader.pages)

        pages: list[dict] = []
        for i, page in enumerate(reader.pages):
            page_text = page.extract_text() or ""
            cleaned = _clean_pdf_text(page_text)
            if cleaned:
                pages.append({"page_no": i + 1, "text": cleaned})

        if not pages:
            doc.error_message = "PDF未提取到有效文本"
            db.commit()
            raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "PDF未提取到有效文本")

        page_chunks = chunk_pages(pages)

        existing_count = db.query(KnowledgeChunk).filter(
            KnowledgeChunk.document_id == doc.id
        ).count()
        if existing_count > 0:
            db.query(KnowledgeChunk).filter(
                KnowledgeChunk.document_id == doc.id
            ).delete()
            db.flush()

        for idx, chunk_info in enumerate(page_chunks):
            chunk_text_content = chunk_info["text"]
            chunk_hash = compute_chunk_hash(chunk_text_content)
            record = KnowledgeChunk(
                document_id=doc.id,
                page_no=chunk_info["page_no"],
                chunk_index=idx,
                chunk_text=chunk_text_content,
                chunk_hash=chunk_hash,
                char_count=len(chunk_text_content),
                vector_status=CHUNK_VECTOR_STATUS_PENDING,
            )
            db.add(record)

        doc.source_path = file_path
        doc.chunk_status = CHUNK_STATUS_COMPLETED
        doc.chunk_count = len(page_chunks)
        doc.page_count = total_pages
        doc.error_message = None
        db.commit()

        logger.info(
            f"重试文档处理完成: document_id={document_id}, title={doc.title}, chunks={len(page_chunks)}"
        )

        return {
            "success": True,
            "message": "文档重试成功",
            "document_id": document_id,
            "chunk_count": len(page_chunks),
        }

    except ServiceException:
        raise
    except Exception as e:
        doc.chunk_status = CHUNK_STATUS_FAILED
        doc.error_message = str(e)[:2000]
        db.commit()
        raise


def vectorize_document(db: Session, document_id: int) -> dict:
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


def _reset_processing_chunks_to_pending(
    db: Session,
    *,
    chunk_ids: list[int] | None = None,
    document_id: int | None = None,
) -> dict:
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


def _update_document_vector_status(db: Session, document_ids: list[int]) -> None:
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


def _normalize_doc_types(doc_type) -> list[str]:
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
        if value in VALID_KB_DOC_TYPES and value not in normalized:
            normalized.append(value)
    return normalized


def _build_kb_filter_expr(stock_code: str | None, doc_types: list[str]) -> str | None:
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


def _has_search_hits(results) -> bool:
    return bool(results and results[0])


def _count_search_hits(results) -> int:
    if not results or not results[0]:
        return 0
    return len(results[0])


def _summarize_search_results(search_results: list[dict], limit: int = 3) -> list[dict]:
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


def search_knowledge(
    query_text: str,
    *,
    stock_code: str | None = None,
    doc_type=None,
    top_k: int = 5,
) -> list[dict]:
    logger.info(
        "[search_knowledge] 开始检索: query长度=%d, stock_code=%s, doc_type=%s, top_k=%d",
        len(query_text),
        stock_code,
        doc_type,
        top_k,
    )

    embedding_model = get_model.embedding_model
    logger.debug("[search_knowledge] 调用 embed_query...")
    query_embedding = embedding_model.embed_query(query_text)
    logger.debug(
        "[search_knowledge] embed_query 返回: embedding长度=%s",
        len(query_embedding) if query_embedding else "None",
    )

    if not query_embedding or len(query_embedding) != settings.EMBEDDING_DIM:
        logger.error(
            "[search_knowledge] Embedding 维度异常: expected=%d, got=%s",
            settings.EMBEDDING_DIM,
            len(query_embedding) if query_embedding else "None",
        )
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR,
            f"Query embedding 维度不正确: expected={settings.EMBEDDING_DIM}",
        )

    logger.debug("[search_knowledge] 正在获取 Milvus Collection...")
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
        "[search_knowledge] Milvus检索参数: filter_expr=%s, normalized_doc_types=%s, top_k=%d",
        filter_expr or "<none>",
        doc_types or [],
        top_k,
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
        "[search_knowledge] Milvus初次检索完成: hit_count=%d, filter_expr=%s",
        _count_search_hits(results),
        filter_expr or "<none>",
    )

    if not _has_search_hits(results) and stock_code:
        fallback_expr = _build_kb_filter_expr(None, doc_types)
        logger.info(
            "[search_knowledge] 股票代码过滤无命中，放宽stock_code重试: original=%s, fallback=%s",
            filter_expr,
            fallback_expr,
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
            "[search_knowledge] 放宽stock_code后检索完成: hit_count=%d, filter_expr=%s",
            _count_search_hits(results),
            fallback_expr or "<none>",
        )

    if not _has_search_hits(results) and doc_types:
        fallback_expr = _build_kb_filter_expr(stock_code, [])
        logger.info(
            "[search_knowledge] 文档类型过滤无命中，放宽doc_type重试: original=%s, fallback=%s",
            filter_expr,
            fallback_expr,
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
            "[search_knowledge] 放宽doc_type后检索完成: hit_count=%d, filter_expr=%s",
            _count_search_hits(results),
            fallback_expr or "<none>",
        )

    if not results or not results[0]:
        logger.info("[search_knowledge] 检索无命中: query=%s", query_text[:120])
        return []

    hits = results[0]
    chunk_ids = [
        hit.entity.get("chunk_id") for hit in hits if hit.entity.get("chunk_id")
    ]

    db = get_background_db_session()
    try:
        chunk_records = (
            db.query(KnowledgeChunk).filter(KnowledgeChunk.id.in_(chunk_ids)).all()
        )
        chunk_map = {c.id: c for c in chunk_records}

        doc_ids = list(set(c.document_id for c in chunk_records))
        doc_records = (
            db.query(KnowledgeDocument).filter(KnowledgeDocument.id.in_(doc_ids)).all()
        )
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
            "[search_knowledge] 回表完成: result_count=%d, effective_filter_expr=%s, top_hits=%s",
            len(search_results),
            effective_filter_expr or "<none>",
            _summarize_search_results(search_results),
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
) -> list[dict]:
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
        "[search_and_format_evidence] 证据格式化完成: evidence_count=%d, query=%s",
        len(evidence_list),
        query_text[:120],
    )
    return evidence_list


# ─────────────────────────────────────────────────────────────────────────────
# 元数据缓存
# ─────────────────────────────────────────────────────────────────────────────
_metadata_cache: dict | None = None
_metadata_cache_loaded: bool = False


def _get_metadata_map() -> dict:
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


def reload_metadata_cache() -> dict:
    """重新加载元数据缓存"""
    global _metadata_cache_loaded
    _metadata_cache_loaded = False
    return _get_metadata_map()


# ─────────────────────────────────────────────────────────────────────────────
# 增量上传函数
# ─────────────────────────────────────────────────────────────────────────────
async def upload_single_document(
    db: Session,
    file,
    doc_type: str | None = None,
) -> dict:
    """上传单个文档（阶段一：仅建档）"""
    import os
    import uuid

    if not file.filename:
        raise ServiceException(ErrorCode.PARAM_ERROR, "文件名不能为空")

    filename = file.filename
    title = os.path.splitext(filename)[0]

    upload_dir = os.path.join(settings.fujian5_UPLOAD_DIR, doc_type or "RESEARCH_REPORT")
    os.makedirs(upload_dir, exist_ok=True)

    unique_filename = f"{uuid.uuid4().hex[:8]}_{filename}"
    file_path = os.path.join(upload_dir, unique_filename)

    content = await file.read()
    with open(file_path, "wb") as f:
        f.write(content)

    metadata_map = _get_metadata_map()
    metadata = metadata_map.get(title)

    metadata_matched = metadata is not None

    if metadata:
        final_doc_type = metadata.get("doc_type", doc_type or "RESEARCH_REPORT")
        stock_code = metadata.get("stock_code")
        stock_abbr = metadata.get("stock_abbr")
        org_name = metadata.get("org_name")
        publish_date = metadata.get("publish_date")
        industry_name = metadata.get("industry_name")
    else:
        final_doc_type = doc_type or "RESEARCH_REPORT"
        stock_code = None
        stock_abbr = None
        org_name = None
        publish_date = None
        industry_name = None

    doc = register_document(
        db,
        doc_type=final_doc_type,
        title=title,
        source_path=file_path,
        stock_code=stock_code,
        stock_abbr=stock_abbr,
        publish_date=publish_date,
        org_name=org_name,
        industry_name=industry_name,
    )

    return {
        "id": doc.id,
        "title": doc.title,
        "doc_type": doc.doc_type,
        "stock_code": doc.stock_code,
        "stock_abbr": doc.stock_abbr,
        "source_path": doc.source_path,
        "chunk_status": doc.chunk_status,
        "vector_status": doc.vector_status,
        "metadata_matched": metadata_matched,
        "created_at": doc.created_at.isoformat() if doc.created_at else None,
    }


async def upload_documents_batch(
    db: Session,
    files: list,
    doc_type: str | None = None,
) -> dict:
    """批量上传文档（阶段一：仅建档）"""
    results = {
        "total": len(files),
        "success": 0,
        "failed": 0,
        "documents": [],
        "errors": [],
    }

    for file in files:
        try:
            doc_result = await upload_single_document(db, file, doc_type)
            results["success"] += 1
            results["documents"].append(doc_result)
        except Exception as e:
            results["failed"] += 1
            results["errors"].append({
                "file": file.filename if file.filename else "unknown",
                "error": str(e)[:500],
            })

    return results


# ─────────────────────────────────────────────────────────────────────────────
# 切块任务提交函数
# ─────────────────────────────────────────────────────────────────────────────
class ChunkSubmitResult:
    def __init__(self, document_id: int, status: str, message: str):
        self.document_id = document_id
        self.status = status
        self.message = message


class BatchChunkSubmitResult:
    def __init__(self):
        self.submitted: int = 0
        self.skipped: int = 0
        self.submitted_ids: list[int] = []


def submit_chunk_task(
    db: Session,
    document_id: int,
    force: bool = False,
) -> ChunkSubmitResult:
    """提交单个文档的切块任务"""
    doc = db.query(KnowledgeDocument).filter(KnowledgeDocument.id == document_id).first()
    if doc is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"文档不存在: id={document_id}")

    if not force and doc.chunk_status == CHUNK_STATUS_COMPLETED:
        return ChunkSubmitResult(
            document_id=document_id,
            status="skipped",
            message="文档已完成切块，跳过",
        )

    if doc.chunk_status == CHUNK_STATUS_PROCESSING:
        return ChunkSubmitResult(
            document_id=document_id,
            status="processing",
            message="文档正在切块中",
        )

    doc.chunk_status = CHUNK_STATUS_PROCESSING
    db.commit()

    return ChunkSubmitResult(
        document_id=document_id,
        status="processing",
        message="切块任务已提交",
    )


def submit_batch_chunk(
    db: Session,
    document_ids: list[int],
) -> BatchChunkSubmitResult:
    """提交批量切块任务"""
    result = BatchChunkSubmitResult()

    for doc_id in document_ids:
        try:
            submit_result = submit_chunk_task(db, doc_id, force=False)
            if submit_result.status == "processing":
                result.submitted += 1
                result.submitted_ids.append(doc_id)
            else:
                result.skipped += 1
        except Exception as e:
            logger.error(f"提交切块任务失败: doc_id={doc_id}, error={e}")
            result.skipped += 1

    return result


def submit_all_pending_chunk(
    db: Session,
    limit: int = 100,
    doc_type: str | None = None,
) -> BatchChunkSubmitResult:
    """提交所有待处理文档的切块任务"""
    query = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.chunk_status == CHUNK_STATUS_PENDING
    )

    if doc_type:
        query = query.filter(KnowledgeDocument.doc_type == doc_type)

    docs = query.limit(limit).all()

    result = BatchChunkSubmitResult()
    for doc in docs:
        try:
            submit_result = submit_chunk_task(db, doc.id, force=False)
            if submit_result.status == "processing":
                result.submitted += 1
                result.submitted_ids.append(doc.id)
        except Exception as e:
            logger.error(f"提交切块任务失败: doc_id={doc.id}, error={e}")

    return result


def run_chunk_in_background(document_id: int):
    """后台执行切块任务"""
    db = get_background_db_session()
    try:
        chunk_document(db, document_id)
        logger.info(f"后台切块完成: document_id={document_id}")
    except Exception as e:
        logger.error(f"后台切块失败: document_id={document_id}, error={e}")
    finally:
        db.close()


def run_chunk_batch_in_background(document_ids: list[int]):
    """后台批量执行切块任务"""
    db = get_background_db_session()
    try:
        for doc_id in document_ids:
            try:
                chunk_document(db, doc_id)
            except Exception as e:
                logger.error(f"切块失败: document_id={doc_id}, error={e}")
        logger.info(f"后台批量切块完成: {len(document_ids)} 个文档")
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# 向量化任务提交函数
# ─────────────────────────────────────────────────────────────────────────────
class VectorizeSubmitResult:
    def __init__(self, document_id: int, status: str, message: str, total_chunks: int = 0):
        self.document_id = document_id
        self.status = status
        self.message = message
        self.total_chunks = total_chunks


class BatchVectorizeSubmitResult:
    def __init__(self):
        self.submitted_count: int = 0
        self.chunk_ids: list[int] = []


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


def reset_vector_status(
    db: Session,
    document_id: int,
    target_status: int = CHUNK_VECTOR_STATUS_PENDING,
) -> dict:
    """重置文档的向量状态（用于取消处理中任务或重置已完成任务）"""
    from app.models.knowledge_document import (
        VECTOR_STATUS_PENDING,
        VECTOR_STATUS_PROCESSING,
        VECTOR_STATUS_SUCCESS,
        VECTOR_STATUS_FAILED,
    )

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
