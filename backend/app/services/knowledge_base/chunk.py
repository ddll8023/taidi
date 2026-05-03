"""知识库文档分块与分块任务提交服务"""

from pypdf import PdfReader
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.database import commit_or_rollback, get_background_db_session
from app.models import knowledge_chunk as models_knowledge_chunk
from app.models import knowledge_document as models_knowledge_document
from app.schemas import knowledge_base as schemas_knowledge_base
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.services.knowledge_base.helpers import clean_pdf_text

logger = setup_logger(__name__)


# ========== 公共入口函数 ==========


def chunk_text(
    text: str,
    chunk_size: int | None = None,
    chunk_overlap: int | None = None,
):
    """将纯文本按指定大小和重叠度切块"""
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
):
    """将页面列表按指定大小和重叠度切块"""
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


def chunk_document(db: Session, document_id: int):
    """对指定文档执行PDF提取和切块处理"""
    stmt = select(models_knowledge_document.KnowledgeDocument).where(models_knowledge_document.KnowledgeDocument.id == document_id)
    doc = db.scalar(stmt)
    if doc is None:
        raise ServiceException(
            ErrorCode.DATA_NOT_FOUND, f"文档不存在: id={document_id}"
        )

    if doc.chunk_status == models_knowledge_document.CHUNK_STATUS_COMPLETED:
        existing_chunks = db.scalars(
            select(models_knowledge_chunk.KnowledgeChunk)
            .where(models_knowledge_chunk.KnowledgeChunk.document_id == document_id)
            .order_by(models_knowledge_chunk.KnowledgeChunk.chunk_index)
        ).all()
        logger.info(
            f"文档已完成切块，跳过: id={document_id}, chunks={len(existing_chunks)}"
        )
        return [
            schemas_knowledge_base.KnowledgeChunkItem.model_validate(c)
            for c in existing_chunks
        ]

    doc.chunk_status = models_knowledge_document.CHUNK_STATUS_PROCESSING
    commit_or_rollback(db)

    try:
        reader = PdfReader(doc.source_path)
        total_pages = len(reader.pages)
        if doc.page_count is None:
            doc.page_count = total_pages

        pages: list[dict] = []
        for i, page in enumerate(reader.pages):
            page_text = page.extract_text() or ""
            cleaned = clean_pdf_text(page_text)
            if cleaned:
                pages.append({"page_no": i + 1, "text": cleaned})

        if not pages:
            raise ServiceException(
                ErrorCode.AI_SERVICE_ERROR,
                f"PDF 未提取到有效文本: source_path={doc.source_path}",
            )

        page_chunks = chunk_pages(pages)

        existing_count = db.scalar(
            select(func.count())
            .select_from(models_knowledge_chunk.KnowledgeChunk)
            .where(models_knowledge_chunk.KnowledgeChunk.document_id == document_id)
        )
        if existing_count > 0:
            db.execute(
                delete(models_knowledge_chunk.KnowledgeChunk).where(models_knowledge_chunk.KnowledgeChunk.document_id == document_id)
            )
            db.flush()

        chunk_records: list[models_knowledge_chunk.KnowledgeChunk] = []
        for idx, chunk_info in enumerate(page_chunks):
            chunk_text_content = chunk_info["text"]
            chunk_hash = models_knowledge_chunk.compute_chunk_hash(chunk_text_content)
            record = models_knowledge_chunk.KnowledgeChunk(
                document_id=document_id,
                page_no=chunk_info["page_no"],
                chunk_index=idx,
                chunk_text=chunk_text_content,
                chunk_hash=chunk_hash,
                char_count=len(chunk_text_content),
                vector_status=models_knowledge_chunk.CHUNK_VECTOR_STATUS_PENDING,
            )
            db.add(record)
            chunk_records.append(record)

        doc.chunk_count = len(page_chunks)
        doc.chunk_status = models_knowledge_document.CHUNK_STATUS_COMPLETED
        commit_or_rollback(db)

        for record in chunk_records:
            db.refresh(record)

        logger.info(
            f"文档切块完成: id={document_id}, chunks={len(page_chunks)}, pages={len(pages)}"
        )
        return [
            schemas_knowledge_base.KnowledgeChunkItem.model_validate(r)
            for r in chunk_records
        ]

    except ServiceException:
        doc.chunk_status = models_knowledge_document.CHUNK_STATUS_FAILED
        doc.chunk_error_message = "操作失败"
        commit_or_rollback(db)
        raise
    except Exception as e:
        logger.error(f"文档切块失败: id={document_id}, error={e}", exc_info=True)
        doc.chunk_status = models_knowledge_document.CHUNK_STATUS_FAILED
        doc.chunk_error_message = "系统内部错误"
        commit_or_rollback(db)
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "操作失败") from e


def submit_and_run_chunk_task(
    db: Session, document_id: int, force: bool, background_tasks
):
    """提交单个文档切块任务并注册后台执行"""
    result = submit_chunk_task(db, document_id, force)
    if result.status == "processing":
        background_tasks.add_task(run_chunk_in_background, document_id)
    return result


def submit_and_run_batch_chunk(db: Session, document_ids: list[int], background_tasks):
    """提交批量切块任务并注册后台执行"""
    result = submit_batch_chunk(db, document_ids)
    if result.submitted_ids:
        background_tasks.add_task(run_chunk_batch_in_background, result.submitted_ids)
    return result


def submit_and_run_all_pending_chunk(
    db: Session, limit: int, doc_type: str | None, background_tasks
):
    """提交所有待处理文档切块任务并注册后台执行"""
    result = submit_all_pending_chunk(db, limit, doc_type)
    if result.submitted_ids:
        background_tasks.add_task(run_chunk_batch_in_background, result.submitted_ids)
    return result


def submit_chunk_task(
    db: Session,
    document_id: int,
    force: bool = False,
):
    """提交单个文档的切块任务"""
    stmt = select(models_knowledge_document.KnowledgeDocument).where(models_knowledge_document.KnowledgeDocument.id == document_id)
    doc = db.scalar(stmt)
    if doc is None:
        raise ServiceException(
            ErrorCode.DATA_NOT_FOUND, f"文档不存在: id={document_id}"
        )

    if not force and doc.chunk_status == models_knowledge_document.CHUNK_STATUS_COMPLETED:
        return schemas_knowledge_base.ChunkSubmitResponse(
            document_id=document_id,
            status="skipped",
            message="文档已完成切块，跳过",
        )

    if doc.chunk_status == models_knowledge_document.CHUNK_STATUS_PROCESSING:
        return schemas_knowledge_base.ChunkSubmitResponse(
            document_id=document_id,
            status="processing",
            message="文档正在切块中",
        )

    doc.chunk_status = models_knowledge_document.CHUNK_STATUS_PROCESSING
    commit_or_rollback(db)

    return schemas_knowledge_base.ChunkSubmitResponse(
        document_id=document_id,
        status="processing",
        message="切块任务已提交",
    )


def submit_batch_chunk(
    db: Session,
    document_ids: list[int],
):
    """提交批量切块任务"""
    submitted = 0
    skipped = 0
    submitted_ids: list[int] = []

    for doc_id in document_ids:
        try:
            submit_result = submit_chunk_task(db, doc_id, force=False)
            if submit_result.status == "processing":
                submitted += 1
                submitted_ids.append(doc_id)
            else:
                skipped += 1
        except Exception as e:
            logger.error(f"提交切块任务失败: doc_id={doc_id}, error={e}")
            skipped += 1

    return schemas_knowledge_base.BatchChunkSubmitResponse(
        submitted=submitted,
        skipped=skipped,
        submitted_ids=submitted_ids,
        message=f"已提交{submitted}个文档的切块任务",
    )


def submit_all_pending_chunk(
    db: Session,
    limit: int = 100,
    doc_type: str | None = None,
):
    """提交所有待处理文档的切块任务"""
    stmt = select(models_knowledge_document.KnowledgeDocument).where(
        models_knowledge_document.KnowledgeDocument.chunk_status == models_knowledge_document.CHUNK_STATUS_PENDING
    )

    if doc_type:
        stmt = stmt.where(models_knowledge_document.KnowledgeDocument.doc_type == doc_type)

    docs = db.scalars(stmt.limit(limit)).all()

    submitted = 0
    skipped = 0
    submitted_ids: list[int] = []
    for doc in docs:
        try:
            submit_result = submit_chunk_task(db, doc.id, force=False)
            if submit_result.status == "processing":
                submitted += 1
                submitted_ids.append(doc.id)
        except Exception as e:
            logger.error(f"提交切块任务失败: doc_id={doc.id}, error={e}")

    return schemas_knowledge_base.BatchChunkSubmitResponse(
        submitted=submitted,
        skipped=skipped,
        submitted_ids=submitted_ids,
        message=f"已提交{submitted}个文档的切块任务",
    )


def run_chunk_in_background(document_id: int):
    """后台执行切块任务"""
    db = get_background_db_session()
    try:
        chunk_document(db, document_id)
        logger.info(f"后台切块完成: document_id={document_id}")
    except ServiceException as e:
        logger.error(f"后台切块业务异常: document_id={document_id} error={e.message}")
    except Exception as e:
        logger.error(
            f"后台切块系统异常: document_id={document_id} error={e}", exc_info=True
        )
    finally:
        db.close()


def run_chunk_batch_in_background(document_ids: list[int]):
    """后台批量执行切块任务"""
    db = get_background_db_session()
    try:
        for doc_id in document_ids:
            try:
                chunk_document(db, doc_id)
            except ServiceException as e:
                logger.error(f"切块业务异常: document_id={doc_id} error={e.message}")
            except Exception as e:
                logger.error(
                    f"切块系统异常: document_id={doc_id} error={e}", exc_info=True
                )
        logger.info(f"后台批量切块完成: {len(document_ids)} 个文档")
    finally:
        db.close()


"""辅助函数"""


def _find_split_boundary(text: str):
    """在文本中从末尾查找最佳分割位置"""
    for sep in ["\n\n", "。", "！", "？", ".", "!", "?", "；", ";", "\n", "，", ","]:
        pos = text.rfind(sep)
        if pos > 0:
            return pos + len(sep)
    return None
