"""知识库文档分块与分块任务提交服务"""
import re

from pypdf import PdfReader
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.knowledge_chunk import (
    CHUNK_VECTOR_STATUS_PENDING,
    KnowledgeChunk,
    compute_chunk_hash,
)
from app.models.knowledge_document import (
    CHUNK_STATUS_COMPLETED,
    CHUNK_STATUS_FAILED,
    CHUNK_STATUS_PENDING,
    CHUNK_STATUS_PROCESSING,
    METADATA_STATUS_PDF_UPLOADED,
    KnowledgeDocument,
)
from app.schemas.knowledge_base import KnowledgeChunkItem
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.db.database import get_background_db_session

logger = setup_logger(__name__)


"""辅助函数"""


def _clean_pdf_text(raw: str):
    text = raw.strip()
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ========== 公共入口函数 ==========


def chunk_text(
    text: str,
    chunk_size: int | None = None,
    chunk_overlap: int | None = None,
):
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


"""辅助函数"""


def _find_split_boundary(text: str):
    for sep in ["\n\n", "。", "！", "？", ".", "!", "?", "；", ";", "\n", "，", ","]:
        pos = text.rfind(sep)
        if pos > 0:
            return pos + len(sep)
    return None


def chunk_document(db: Session, document_id: int):
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
        return [KnowledgeChunkItem.model_validate(c) for c in existing_chunks]

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
        return [KnowledgeChunkItem.model_validate(r) for r in chunk_records]

    except Exception as e:
        doc.chunk_status = CHUNK_STATUS_FAILED
        doc.chunk_error_message = str(e)[:2000]
        db.commit()
        logger.error("文档切块失败: id=%d, error=%s", document_id, str(e))
        raise


class ChunkSubmitResult:
    def __init__(self, document_id: int, status: str, message: str):
        self.document_id = document_id
        self.status = status
        self.message = message

    def to_dict(self):
        return {
            "document_id": self.document_id,
            "status": self.status,
            "message": self.message,
        }


class BatchChunkSubmitResult:
    def __init__(self):
        self.submitted: int = 0
        self.skipped: int = 0
        self.submitted_ids: list[int] = []

    def to_dict(self):
        return {
            "submitted": self.submitted,
            "skipped": self.skipped,
            "submitted_ids": self.submitted_ids,
            "message": f"已提交{self.submitted}个文档的切块任务",
        }


def submit_and_run_chunk_task(
    db: Session, document_id: int, force: bool, background_tasks
):
    """提交单个文档切块任务并注册后台执行"""
    result = submit_chunk_task(db, document_id, force)
    if result.status == "processing":
        background_tasks.add_task(run_chunk_in_background, document_id)
    return result


def submit_and_run_batch_chunk(
    db: Session, document_ids: list[int], background_tasks
):
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
