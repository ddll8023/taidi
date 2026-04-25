"""知识库文档注册、PDF 提取与文档列表查询服务"""
import os
import re
import uuid

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
    METADATA_STATUS_LOADED,
    METADATA_STATUS_NOT_LOADED,
    METADATA_STATUS_PDF_UPLOADED,
    VECTOR_STATUS_PENDING,
    KnowledgeDocument,
)
from app.schemas.knowledge_base import (
    KnowledgeChunkItem,
    KnowledgeDocumentItem,
)
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

from app.services.knowledge_base.chunk import chunk_document, chunk_pages, _clean_pdf_text
from app.services.knowledge_base.metadata import _get_metadata_map

logger = setup_logger(__name__)

FILENAME_UNSAFE_PATTERN = re.compile(r'[\\/:*?"<>|／∕⁄]+')
MATCH_UNDERSCORE_PATTERN = re.compile(r"\s*_\s*")
MATCH_WHITESPACE_PATTERN = re.compile(r"\s+")


# ========== 公共入口函数 ==========


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
):
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
        return KnowledgeDocumentItem.model_validate(existing)

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
    return KnowledgeDocumentItem.model_validate(doc)


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
):
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
):
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
    return [KnowledgeDocumentItem.model_validate(item) for item in items], total


def get_chunk_list(
    db: Session,
    *,
    document_id: int | None = None,
    vector_status: int | None = None,
    page: int = 1,
    page_size: int = 20,
):
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
    return [KnowledgeChunkItem.model_validate(item) for item in items], total


def extract_pdf_full_text(file_path: str):
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


def extract_pdf_pages(file_path: str):
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


async def init_knowledge_base_metadata(
    db: Session,
    stock_excel_content: bytes,
    industry_excel_content: bytes,
    force_reload: bool = False,
):
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
):
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
            doc = _find_pending_metadata_document(db, pdf_name, doc_type)

        if not doc:
            failed_documents.append({
                "pdf_name": pdf_name,
                "reason": "未找到匹配的Excel元数据",
                "suggestion": "请检查文件名是否与Excel中的title字段一致；标题中的/等文件名非法字符会按_匹配",
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


async def upload_single_pdf_for_document(
    db: Session,
    document_id: int,
    pdf_file,
):
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
):
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


async def upload_single_document(
    db: Session,
    file,
    doc_type: str | None = None,
) -> dict:
    """上传单个文档（阶段一：仅建档）"""
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
        "created_at": doc.created_at,
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


"""辅助函数"""


def _find_pending_metadata_document(
    db: Session,
    pdf_name: str,
    doc_type: str,
) -> KnowledgeDocument | None:
    """按文件名规范化结果匹配待上传的 Excel 元数据"""
    normalized_pdf_name = _normalize_metadata_match_text(pdf_name)
    candidates = db.query(KnowledgeDocument).filter(
        KnowledgeDocument.doc_type == doc_type,
        KnowledgeDocument.metadata_status == METADATA_STATUS_LOADED,
    ).all()

    matched_documents = [
        doc
        for doc in candidates
        if _normalize_metadata_match_text(doc.title) == normalized_pdf_name
    ]

    if len(matched_documents) > 1:
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            f"PDF文件名匹配到多条Excel元数据: {pdf_name}",
        )

    return matched_documents[0] if matched_documents else None


def _normalize_metadata_match_text(value: str) -> str:
    """将 Excel 标题转换为可与 PDF 文件名比较的规范化文本"""
    normalized = str(value).strip()
    normalized = FILENAME_UNSAFE_PATTERN.sub("_", normalized)
    normalized = MATCH_UNDERSCORE_PATTERN.sub("_", normalized)
    normalized = MATCH_WHITESPACE_PATTERN.sub(" ", normalized)
    return normalized.strip().casefold()
