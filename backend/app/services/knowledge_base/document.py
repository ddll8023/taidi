"""知识库文档注册、PDF 提取与文档列表查询服务"""

import math
import os
import uuid

from fastapi import UploadFile
from pypdf import PdfReader
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models import knowledge_chunk as models_knowledge_chunk
from app.models import knowledge_document as models_knowledge_document
from app.schemas import knowledge_base as schemas_knowledge_base
from app.schemas.common import ErrorCode, PaginatedResponse, PaginationInfo
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.db.database import commit_or_rollback
from app.constants import knowledge_base as constants_kb
from app.services.knowledge_base.chunk import chunk_document, chunk_pages
from app.services.knowledge_base.helpers import clean_pdf_text, get_metadata_map

logger = setup_logger(__name__)


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
    """注册文档到知识库"""
    existing = db.scalar(
        select(models_knowledge_document.KnowledgeDocument).where(
            models_knowledge_document.KnowledgeDocument.source_path == source_path
        )
    )
    if existing:
        logger.info(
            f"文档已存在，跳过注册: source_path={source_path}, id={existing.id}"
        )
        return schemas_knowledge_base.KnowledgeDocumentItem.model_validate(existing)

    doc = models_knowledge_document.KnowledgeDocument(
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
        chunk_status=models_knowledge_document.CHUNK_STATUS_PENDING,
        vector_status=models_knowledge_document.VECTOR_STATUS_PENDING,
    )
    db.add(doc)
    commit_or_rollback(db)
    db.refresh(doc)
    logger.info(f"文档注册成功: id={doc.id}, title={title}, doc_type={doc_type}")
    return schemas_knowledge_base.KnowledgeDocumentItem.model_validate(doc)


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
    """注册文档并立即执行切块"""
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
    """分页查询知识库文档列表"""
    stmt = select(models_knowledge_document.KnowledgeDocument)

    if doc_type:
        stmt = stmt.where(
            models_knowledge_document.KnowledgeDocument.doc_type == doc_type
        )
    if stock_code:
        stmt = stmt.where(
            models_knowledge_document.KnowledgeDocument.stock_code == stock_code
        )
    if stock_abbr:
        stmt = stmt.where(
            models_knowledge_document.KnowledgeDocument.stock_abbr.like(
                f"%{stock_abbr}%"
            )
        )
    if metadata_status is not None:
        stmt = stmt.where(
            models_knowledge_document.KnowledgeDocument.metadata_status
            == metadata_status
        )
    if chunk_status is not None:
        stmt = stmt.where(
            models_knowledge_document.KnowledgeDocument.chunk_status == chunk_status
        )
    if vector_status is not None:
        stmt = stmt.where(
            models_knowledge_document.KnowledgeDocument.vector_status == vector_status
        )

    total = db.scalar(select(func.count()).select_from(stmt.subquery()))
    items = db.scalars(
        stmt.order_by(models_knowledge_document.KnowledgeDocument.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    ).all()
    lists = [
        schemas_knowledge_base.KnowledgeDocumentItem.model_validate(item)
        for item in items
    ]
    total_pages = math.ceil(total / page_size) if total else 0
    return PaginatedResponse(
        lists=lists,
        pagination=PaginationInfo(
            page=page,
            page_size=page_size,
            total=total,
            total_pages=total_pages,
        ),
    )


def get_chunk_list(
    db: Session,
    *,
    document_id: int | None = None,
    vector_status: int | None = None,
    page: int = 1,
    page_size: int = 20,
):
    """分页查询知识库切块列表"""
    stmt = select(models_knowledge_chunk.KnowledgeChunk)

    if document_id:
        stmt = stmt.where(
            models_knowledge_chunk.KnowledgeChunk.document_id == document_id
        )
    if vector_status is not None:
        stmt = stmt.where(
            models_knowledge_chunk.KnowledgeChunk.vector_status == vector_status
        )

    total = db.scalar(select(func.count()).select_from(stmt.subquery()))
    items = db.scalars(
        stmt.order_by(models_knowledge_chunk.KnowledgeChunk.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    ).all()
    lists = [
        schemas_knowledge_base.KnowledgeChunkItem.model_validate(item) for item in items
    ]
    total_pages = math.ceil(total / page_size) if total else 0
    return PaginatedResponse(
        lists=lists,
        pagination=PaginationInfo(
            page=page,
            page_size=page_size,
            total=total,
            total_pages=total_pages,
        ),
    )


def extract_pdf_full_text(file_path: str):
    """提取PDF全文"""
    reader = PdfReader(file_path)
    page_count = len(reader.pages)
    texts: list[str] = []
    for page in reader.pages:
        page_text = page.extract_text() or ""
        cleaned = clean_pdf_text(page_text)
        if cleaned:
            texts.append(cleaned)
    full_text = "\n\n".join(texts)
    return full_text, page_count


def extract_pdf_pages(file_path: str):
    """按页提取PDF文本"""
    reader = PdfReader(file_path)
    pages: list[dict] = []
    for i, page in enumerate(reader.pages):
        page_text = page.extract_text() or ""
        cleaned = clean_pdf_text(page_text)
        if cleaned:
            pages.append(
                {
                    "page_no": i + 1,
                    "text": cleaned,
                }
            )
    return pages


async def init_system_from_upload(
    db: Session,
    stock_excel: UploadFile,
    industry_excel: UploadFile,
    force_reload: bool = False,
):
    """系统初始化：校验并加载Excel元数据"""
    for label, f in [("个股研报", stock_excel), ("行业研报", industry_excel)]:
        if not f.filename or not f.filename.lower().endswith((".xlsx", ".xls")):
            raise ServiceException(
                ErrorCode.PARAM_ERROR, f"{label}文件仅支持 Excel 格式（.xlsx/.xls）"
            )

    stock_content = await stock_excel.read()
    industry_content = await industry_excel.read()
    return await init_knowledge_base_metadata(
        db=db,
        stock_excel_content=stock_content,
        industry_excel_content=industry_content,
        force_reload=force_reload,
    )


async def init_knowledge_base_metadata(
    db: Session,
    stock_excel_content: bytes,
    industry_excel_content: bytes,
    force_reload: bool = False,
):
    """加载Excel元数据到knowledge_document表"""
    from app.services.fujian5_data_processor import (
        parse_stock_research_excel_from_upload,
        parse_industry_research_excel_from_upload,
    )

    existing_count = db.scalar(
        select(func.count()).select_from(
            select(models_knowledge_document.KnowledgeDocument)
            .where(
                models_knowledge_document.KnowledgeDocument.metadata_status
                >= models_knowledge_document.METADATA_STATUS_LOADED
            )
            .subquery()
        )
    )

    if existing_count > 0 and not force_reload:
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            f"系统已初始化（{existing_count}条元数据），如需重新加载请使用force_reload=true",
        )

    try:
        stock_records, _ = parse_stock_research_excel_from_upload(stock_excel_content)
        industry_records, _ = parse_industry_research_excel_from_upload(
            industry_excel_content
        )
    except ServiceException:
        raise
    except Exception as e:
        logger.error(f"解析 Excel 元数据失败: {e}")
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "解析Excel失败") from e

    if force_reload:
        loaded_doc_ids = select(models_knowledge_document.KnowledgeDocument.id).where(
            models_knowledge_document.KnowledgeDocument.metadata_status
            >= models_knowledge_document.METADATA_STATUS_LOADED
        )
        db.execute(
            delete(models_knowledge_chunk.KnowledgeChunk).where(
                models_knowledge_chunk.KnowledgeChunk.document_id.in_(loaded_doc_ids)
            )
        )
        db.execute(
            delete(models_knowledge_document.KnowledgeDocument).where(
                models_knowledge_document.KnowledgeDocument.metadata_status
                >= models_knowledge_document.METADATA_STATUS_LOADED
            )
        )
        commit_or_rollback(db)

    documents: list[models_knowledge_document.KnowledgeDocument] = []
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

        existing = db.scalar(
            select(models_knowledge_document.KnowledgeDocument).where(
                models_knowledge_document.KnowledgeDocument.title == title,
                models_knowledge_document.KnowledgeDocument.doc_type
                == "RESEARCH_REPORT",
            )
        )

        if existing:
            duplicates += 1
            continue

        doc = models_knowledge_document.KnowledgeDocument(
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
            metadata_status=models_knowledge_document.METADATA_STATUS_LOADED,
            chunk_status=models_knowledge_document.CHUNK_STATUS_PENDING,
            vector_status=models_knowledge_document.VECTOR_STATUS_PENDING,
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

        existing = db.scalar(
            select(models_knowledge_document.KnowledgeDocument).where(
                models_knowledge_document.KnowledgeDocument.title == title,
                models_knowledge_document.KnowledgeDocument.doc_type
                == "INDUSTRY_REPORT",
            )
        )

        if existing:
            duplicates += 1
            continue

        doc = models_knowledge_document.KnowledgeDocument(
            doc_type="INDUSTRY_REPORT",
            title=title,
            source_path="pending_upload",
            org_name=record.get("orgName"),
            publish_date=record.get("publishDate"),
            industry_name=record.get("industryName"),
            researcher=record.get("researcher"),
            metadata_status=models_knowledge_document.METADATA_STATUS_LOADED,
            chunk_status=models_knowledge_document.CHUNK_STATUS_PENDING,
            vector_status=models_knowledge_document.VECTOR_STATUS_PENDING,
            chunk_count=0,
        )
        documents.append(doc)
        seen_keys.add(key)

    if documents:
        db.bulk_save_objects(documents)
        commit_or_rollback(db)

    stock_count = len([d for d in documents if d.doc_type == "RESEARCH_REPORT"])
    industry_count = len([d for d in documents if d.doc_type == "INDUSTRY_REPORT"])

    logger.info(
        f"系统初始化完成: 个股研报 {stock_count} 条, 行业研报 {industry_count} 条, "
        f"重复 {duplicates} 条"
    )

    return schemas_knowledge_base.InitResponse(
        success=True,
        message="系统初始化成功",
        stock_metadata_count=stock_count,
        industry_metadata_count=industry_count,
        total_count=len(documents),
        duplicates=duplicates,
        errors=[],
    )


async def upload_pdf_incremental(
    db: Session,
    pdfs: list,
    doc_type: str,
    manual_match: dict | None = None,
):
    """增量上传PDF文件，匹配元数据并切块"""
    if not pdfs:
        raise ServiceException(ErrorCode.PARAM_ERROR, "请选择至少一个PDF文件")

    for pdf_file in pdfs:
        filename = getattr(pdf_file, "filename", None)
        if not filename or not filename.lower().endswith(".pdf"):
            raise ServiceException(ErrorCode.PARAM_ERROR, "仅支持PDF文件")

    processed_count = 0
    failed_documents: list[dict] = []
    errors: list[dict] = []

    for pdf_file in pdfs:
        filename = getattr(pdf_file, "filename", None)
        if not filename:
            failed_documents.append(
                {
                    "pdf_name": "unknown",
                    "reason": "PDF文件名为空",
                    "suggestion": "请检查上传的文件",
                }
            )
            continue

        pdf_name = os.path.splitext(filename)[0]

        doc = None

        if manual_match and pdf_name in manual_match:
            matched_title = manual_match[pdf_name]
            doc = db.scalar(
                select(models_knowledge_document.KnowledgeDocument).where(
                    models_knowledge_document.KnowledgeDocument.title == matched_title,
                    models_knowledge_document.KnowledgeDocument.doc_type == doc_type,
                    models_knowledge_document.KnowledgeDocument.metadata_status
                    == models_knowledge_document.METADATA_STATUS_LOADED,
                )
            )

        if not doc:
            doc = _find_pending_metadata_document(db, pdf_name, doc_type)

        if not doc:
            failed_documents.append(
                {
                    "pdf_name": pdf_name,
                    "reason": "未找到匹配的Excel元数据",
                    "suggestion": "请检查文件名是否与Excel中的title字段一致；标题中的/等文件名非法字符会按_匹配",
                }
            )
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
                cleaned = clean_pdf_text(page_text)
                if cleaned:
                    pages.append({"page_no": i + 1, "text": cleaned})

            if not pages:
                doc.error_message = "PDF未提取到有效文本"
                commit_or_rollback(db)
                failed_documents.append(
                    {
                        "pdf_name": pdf_name,
                        "reason": "PDF未提取到有效文本",
                        "suggestion": "请检查PDF文件完整性",
                    }
                )
                continue

            page_chunks = chunk_pages(pages)

            existing_count = db.scalar(
                select(func.count()).select_from(
                    select(models_knowledge_chunk.KnowledgeChunk)
                    .where(models_knowledge_chunk.KnowledgeChunk.document_id == doc.id)
                    .subquery()
                )
            )
            if existing_count > 0:
                db.execute(
                    delete(models_knowledge_chunk.KnowledgeChunk).where(
                        models_knowledge_chunk.KnowledgeChunk.document_id == doc.id
                    )
                )
                db.flush()

            for idx, chunk_info in enumerate(page_chunks):
                chunk_text_content = chunk_info["text"]
                chunk_hash = models_knowledge_chunk.compute_chunk_hash(
                    chunk_text_content
                )
                record = models_knowledge_chunk.KnowledgeChunk(
                    document_id=doc.id,
                    page_no=chunk_info["page_no"],
                    chunk_index=idx,
                    chunk_text=chunk_text_content,
                    chunk_hash=chunk_hash,
                    char_count=len(chunk_text_content),
                    vector_status=models_knowledge_chunk.CHUNK_VECTOR_STATUS_PENDING,
                )
                db.add(record)

            doc.source_path = file_path
            doc.metadata_status = models_knowledge_document.METADATA_STATUS_PDF_UPLOADED
            doc.chunk_status = models_knowledge_document.CHUNK_STATUS_COMPLETED
            doc.chunk_count = len(page_chunks)
            doc.page_count = total_pages
            doc.error_message = None
            commit_or_rollback(db)

            processed_count += 1

            logger.info(
                f"增量上传PDF处理完成: title={pdf_name}, chunks={len(page_chunks)}"
            )

        except ServiceException:
            raise
        except Exception as e:
            doc.error_message = "PDF处理失败"
            commit_or_rollback(db)
            failed_documents.append(
                {
                    "pdf_name": pdf_name,
                    "reason": "PDF处理失败",
                    "suggestion": "请检查PDF文件完整性",
                }
            )
            errors.append(
                {
                    "file": filename,
                    "error": "PDF处理失败",
                }
            )
            logger.error(f"增量上传PDF处理失败: {filename}, error={e}")

    total_processed = db.scalar(
        select(func.count()).select_from(
            select(models_knowledge_document.KnowledgeDocument)
            .where(
                models_knowledge_document.KnowledgeDocument.metadata_status
                == models_knowledge_document.METADATA_STATUS_PDF_UPLOADED
            )
            .subquery()
        )
    )

    total_pending = db.scalar(
        select(func.count()).select_from(
            select(models_knowledge_document.KnowledgeDocument)
            .where(
                models_knowledge_document.KnowledgeDocument.metadata_status
                == models_knowledge_document.METADATA_STATUS_LOADED
            )
            .subquery()
        )
    )

    return schemas_knowledge_base.UploadPdfResponse(
        success=True,
        message=f"成功处理{processed_count}个文档"
        + (f"，{len(failed_documents)}个失败" if failed_documents else ""),
        processed_count=processed_count,
        failed_count=len(failed_documents),
        total_processed=total_processed,
        total_pending=total_pending,
        failed_documents=failed_documents,
        errors=errors,
    )


async def upload_single_pdf_for_document(
    db: Session,
    document_id: int,
    pdf_file,
):
    """上传单个文档的PDF"""
    filename = getattr(pdf_file, "filename", None)
    if not filename or not filename.lower().endswith(".pdf"):
        raise ServiceException(ErrorCode.PARAM_ERROR, "仅支持PDF文件")

    doc = db.scalar(
        select(models_knowledge_document.KnowledgeDocument).where(
            models_knowledge_document.KnowledgeDocument.id == document_id
        )
    )
    if not doc:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "文档不存在")

    if doc.metadata_status != models_knowledge_document.METADATA_STATUS_LOADED:
        raise ServiceException(ErrorCode.PARAM_ERROR, "文档元数据未加载或PDF已上传")

    pdf_name = os.path.splitext(filename)[0]

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
        cleaned = clean_pdf_text(page_text)
        if cleaned:
            pages.append({"page_no": i + 1, "text": cleaned})

    if not pages:
        doc.error_message = "PDF未提取到有效文本"
        commit_or_rollback(db)
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "PDF未提取到有效文本")

    page_chunks = chunk_pages(pages)

    existing_count = db.scalar(
        select(func.count()).select_from(
            select(models_knowledge_chunk.KnowledgeChunk)
            .where(models_knowledge_chunk.KnowledgeChunk.document_id == doc.id)
            .subquery()
        )
    )
    if existing_count > 0:
        db.execute(
            delete(models_knowledge_chunk.KnowledgeChunk).where(
                models_knowledge_chunk.KnowledgeChunk.document_id == doc.id
            )
        )
        db.flush()

    for idx, chunk_info in enumerate(page_chunks):
        chunk_text_content = chunk_info["text"]
        chunk_hash = models_knowledge_chunk.compute_chunk_hash(chunk_text_content)
        record = models_knowledge_chunk.KnowledgeChunk(
            document_id=doc.id,
            page_no=chunk_info["page_no"],
            chunk_index=idx,
            chunk_text=chunk_text_content,
            chunk_hash=chunk_hash,
            char_count=len(chunk_text_content),
            vector_status=models_knowledge_chunk.CHUNK_VECTOR_STATUS_PENDING,
        )
        db.add(record)

    doc.source_path = file_path
    doc.metadata_status = models_knowledge_document.METADATA_STATUS_PDF_UPLOADED
    doc.chunk_status = models_knowledge_document.CHUNK_STATUS_COMPLETED
    doc.chunk_count = len(page_chunks)
    doc.page_count = total_pages
    doc.error_message = None
    commit_or_rollback(db)

    logger.info(
        f"单文档PDF上传处理完成: document_id={document_id}, title={doc.title}, chunks={len(page_chunks)}"
    )

    return schemas_knowledge_base.UploadSinglePdfResponse(
        success=True,
        message="文档PDF上传成功",
        document_id=document_id,
        chunk_count=len(page_chunks),
    )


async def retry_failed_document(
    db: Session,
    document_id: int,
    pdf_file,
):
    """重试失败的文档"""
    filename = getattr(pdf_file, "filename", None)
    if not filename or not filename.lower().endswith(".pdf"):
        raise ServiceException(ErrorCode.PARAM_ERROR, "仅支持PDF文件")

    doc = db.scalar(
        select(models_knowledge_document.KnowledgeDocument).where(
            models_knowledge_document.KnowledgeDocument.id == document_id
        )
    )
    if not doc:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "文档不存在")

    if (
        doc.chunk_status != models_knowledge_document.CHUNK_STATUS_FAILED
        and doc.metadata_status
        != models_knowledge_document.METADATA_STATUS_PDF_UPLOADED
    ):
        if doc.metadata_status == models_knowledge_document.METADATA_STATUS_LOADED:
            return await upload_single_pdf_for_document(db, document_id, pdf_file)
        raise ServiceException(ErrorCode.PARAM_ERROR, "文档未标记为失败，无需重试")

    doc.chunk_status = models_knowledge_document.CHUNK_STATUS_PENDING
    doc.error_message = None
    if doc.metadata_status == models_knowledge_document.METADATA_STATUS_LOADED:
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
            cleaned = clean_pdf_text(page_text)
            if cleaned:
                pages.append({"page_no": i + 1, "text": cleaned})

        if not pages:
            doc.error_message = "PDF未提取到有效文本"
            commit_or_rollback(db)
            raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "PDF未提取到有效文本")

        page_chunks = chunk_pages(pages)

        existing_count = db.scalar(
            select(func.count()).select_from(
                select(models_knowledge_chunk.KnowledgeChunk)
                .where(models_knowledge_chunk.KnowledgeChunk.document_id == doc.id)
                .subquery()
            )
        )
        if existing_count > 0:
            db.execute(
                delete(models_knowledge_chunk.KnowledgeChunk).where(
                    models_knowledge_chunk.KnowledgeChunk.document_id == doc.id
                )
            )
            db.flush()

        for idx, chunk_info in enumerate(page_chunks):
            chunk_text_content = chunk_info["text"]
            chunk_hash = models_knowledge_chunk.compute_chunk_hash(chunk_text_content)
            record = models_knowledge_chunk.KnowledgeChunk(
                document_id=doc.id,
                page_no=chunk_info["page_no"],
                chunk_index=idx,
                chunk_text=chunk_text_content,
                chunk_hash=chunk_hash,
                char_count=len(chunk_text_content),
                vector_status=models_knowledge_chunk.CHUNK_VECTOR_STATUS_PENDING,
            )
            db.add(record)

        doc.source_path = file_path
        doc.chunk_status = models_knowledge_document.CHUNK_STATUS_COMPLETED
        doc.chunk_count = len(page_chunks)
        doc.page_count = total_pages
        doc.error_message = None
        commit_or_rollback(db)

        logger.info(
            f"重试文档处理完成: document_id={document_id}, title={doc.title}, chunks={len(page_chunks)}"
        )

        return schemas_knowledge_base.RetryDocumentResponse(
            success=True,
            message="文档重试成功",
            document_id=document_id,
            chunk_count=len(page_chunks),
        )

    except ServiceException:
        raise
    except Exception as e:
        doc.chunk_status = models_knowledge_document.CHUNK_STATUS_FAILED
        doc.error_message = "PDF处理失败"
        commit_or_rollback(db)
        logger.error(f"重试文档处理失败: document_id={document_id}, error={e}")
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "操作失败") from e


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

    upload_dir = os.path.join(
        settings.fujian5_UPLOAD_DIR, doc_type or "RESEARCH_REPORT"
    )
    os.makedirs(upload_dir, exist_ok=True)

    unique_filename = f"{uuid.uuid4().hex[:8]}_{filename}"
    file_path = os.path.join(upload_dir, unique_filename)

    content = await file.read()
    with open(file_path, "wb") as f:
        f.write(content)

    metadata_map = get_metadata_map()
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

    return schemas_knowledge_base.DocumentUploadResponse(
        id=doc.id,
        title=doc.title,
        doc_type=doc.doc_type,
        stock_code=doc.stock_code,
        stock_abbr=doc.stock_abbr,
        source_path=doc.source_path,
        chunk_status=doc.chunk_status,
        vector_status=doc.vector_status,
        metadata_matched=metadata_matched,
        created_at=doc.created_at,
    )


async def upload_documents_batch(
    db: Session,
    files: list,
    doc_type: str | None = None,
):
    """批量上传文档（阶段一：仅建档）"""
    total = len(files)
    success_count = 0
    failed_count = 0
    documents: list[schemas_knowledge_base.DocumentUploadResponse] = []
    errors: list[dict] = []

    for file in files:
        try:
            doc_result = await upload_single_document(db, file, doc_type)
            success_count += 1
            documents.append(doc_result)
        except Exception as e:
            failed_count += 1
            errors.append(
                {
                    "file": file.filename if file.filename else "unknown",
                    "error": "文档处理失败",
                }
            )
            logger.error(f"批量上传单文档失败: file={file.filename}, error={e}")

    return schemas_knowledge_base.BatchUploadResponse(
        total=total,
        success=success_count,
        failed=failed_count,
        documents=documents,
        errors=errors,
    )


"""辅助函数"""


def _find_pending_metadata_document(
    db: Session,
    pdf_name: str,
    doc_type: str,
) -> models_knowledge_document.KnowledgeDocument | None:
    """按文件名规范化结果匹配待上传的 Excel 元数据"""
    normalized_pdf_name = _normalize_metadata_match_text(pdf_name)
    candidates = db.scalars(
        select(models_knowledge_document.KnowledgeDocument).where(
            models_knowledge_document.KnowledgeDocument.doc_type == doc_type,
            models_knowledge_document.KnowledgeDocument.metadata_status
            == models_knowledge_document.METADATA_STATUS_LOADED,
        )
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
    normalized = constants_kb.FILENAME_UNSAFE_PATTERN.sub("_", normalized)
    normalized = constants_kb.MATCH_UNDERSCORE_PATTERN.sub("_", normalized)
    normalized = constants_kb.MATCH_WHITESPACE_PATTERN.sub(" ", normalized)
    return normalized.strip().casefold()
