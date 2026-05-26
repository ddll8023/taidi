"""知识库管理服务"""

import hashlib
import math
import os
import re
import tempfile

import pandas as pd
from fastapi import UploadFile
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pypdf import PdfReader
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.constants import knowledge_base as constants_knowledge_base
from app.core.config import settings
from app.db.database import commit_or_rollback
from app.models import knowledge_document as models_knowledge_document
from app.models import knowledge_chunk as models_knowledge_chunk
from app.schemas import knowledge_base as schemas_knowledge_base
from app.schemas.common import ErrorCode, PaginatedResponse, PaginationInfo
from app.utils.exception import ServiceException
from app.utils.file import save_file
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


def init_knowledge_base(
    db: Session,
    file: UploadFile,
    doc_type: str,
):
    """加载研报元数据到知识库"""
    logger.info(f"收到初始化请求: file={file.filename} doc_type={doc_type}")
    if doc_type not in (
        constants_knowledge_base.DOC_TYPE_RESEARCH_REPORT,
        constants_knowledge_base.DOC_TYPE_INDUSTRY_REPORT,
    ):
        raise ServiceException(
            ErrorCode.PARAM_ERROR,
            f"不支持的文档类型: {doc_type}",
        )

    column_map = (
        constants_knowledge_base.STOCK_RESEARCH_REPORT_COLUMN_MAP
        if doc_type == constants_knowledge_base.DOC_TYPE_RESEARCH_REPORT
        else constants_knowledge_base.INDUSTRY_REPORT_COLUMN_MAP
    )

    try:
        success_count, errors, duplicate_count = _import_excel(
            db, file, doc_type, column_map
        )
    except Exception as e:
        logger.error(
            f"导入Excel异常: file={file.filename} doc_type={doc_type} error={e}",
            exc_info=True,
        )
        raise ServiceException from e

    # 构造消息
    message_parts = []
    if success_count > 0:
        message_parts.append(f"成功导入{success_count}条")
    if duplicate_count > 0:
        message_parts.append(f"跳过{duplicate_count}条重复记录")
    if errors:
        message_parts.append(f"{len(errors)}条记录失败")
    message = "，".join(message_parts) if message_parts else "导入完成"

    logger.info(
        f"初始化请求处理完成: file={file.filename} doc_type={doc_type} success={success_count} duplicate={duplicate_count} errors={len(errors)}"
    )
    return schemas_knowledge_base.InitKnowledgeBaseResponse(
        success=True,
        message=message,
        total_count=success_count,
        duplicate_count=duplicate_count,
    )


def get_init_status(db: Session):
    """查询系统初始化状态"""
    logger.info("查询初始化状态")
    stmt = select(
        models_knowledge_document.KnowledgeDocument.doc_type,
        func.count(models_knowledge_document.KnowledgeDocument.id),
    ).group_by(models_knowledge_document.KnowledgeDocument.doc_type)

    results = db.execute(stmt).all()
    count_map = {row[0]: row[1] for row in results}

    stock_count = count_map.get(constants_knowledge_base.DOC_TYPE_RESEARCH_REPORT, 0)
    industry_count = count_map.get(constants_knowledge_base.DOC_TYPE_INDUSTRY_REPORT, 0)
    total = stock_count + industry_count

    logger.info(
        f"初始化状态查询完成: total={total} stock={stock_count} industry={industry_count}"
    )
    return schemas_knowledge_base.GetInitStatusResponse(
        initialized=total > 0,
        stock_metadata_count=stock_count,
        industry_metadata_count=industry_count,
        total_metadata_count=total,
    )


def get_knowledge_base_stats(db: Session):
    """获取知识库整体统计信息"""
    logger.info("查询知识库统计信息")

    doc_total = (
        db.scalar(select(func.count(models_knowledge_document.KnowledgeDocument.id)))
        or 0
    )

    # 查询文档切块状态统计
    doc_chunk_status_rows = db.execute(
        select(
            models_knowledge_document.KnowledgeDocument.chunk_status,
            func.count(models_knowledge_document.KnowledgeDocument.id),
        ).group_by(models_knowledge_document.KnowledgeDocument.chunk_status)
    ).all()
    doc_by_chunk_status = {str(row[0]): row[1] for row in doc_chunk_status_rows}

    # 查询文档向量状态统计
    doc_vector_status_rows = db.execute(
        select(
            models_knowledge_document.KnowledgeDocument.vector_status,
            func.count(models_knowledge_document.KnowledgeDocument.id),
        ).group_by(models_knowledge_document.KnowledgeDocument.vector_status)
    ).all()
    doc_by_vector_status = {str(row[0]): row[1] for row in doc_vector_status_rows}

    # 查询文档类型统计
    doc_type_rows = db.execute(
        select(
            models_knowledge_document.KnowledgeDocument.doc_type,
            func.count(models_knowledge_document.KnowledgeDocument.id),
        ).group_by(models_knowledge_document.KnowledgeDocument.doc_type)
    ).all()
    doc_by_type = {row[0]: row[1] for row in doc_type_rows}

    chunk_total = (
        db.scalar(select(func.count(models_knowledge_chunk.KnowledgeChunk.id))) or 0
    )

    # 查询切块向量状态统计
    chunk_vector_status_rows = db.execute(
        select(
            models_knowledge_chunk.KnowledgeChunk.vector_status,
            func.count(models_knowledge_chunk.KnowledgeChunk.id),
        ).group_by(models_knowledge_chunk.KnowledgeChunk.vector_status)
    ).all()
    chunk_by_vector_status = {str(row[0]): row[1] for row in chunk_vector_status_rows}

    logger.info(f"知识库统计查询完成: doc_total={doc_total} chunk_total={chunk_total}")
    return schemas_knowledge_base.GetKnowledgeBaseStatsResponse(
        documents=schemas_knowledge_base.DocumentStatsItem(
            total=doc_total,
            by_chunk_status=doc_by_chunk_status,
            by_vector_status=doc_by_vector_status,
            by_doc_type=doc_by_type,
        ),
        chunks=schemas_knowledge_base.ChunkStatsItem(
            total=chunk_total,
            by_vector_status=chunk_by_vector_status,
        ),
    )


def get_knowledge_document_list(
    db: Session,
    get_knowledge_document_list_request: schemas_knowledge_base.GetKnowledgeDocumentListRequest,
):
    """查询知识库文档列表"""
    logger.info(
        f"查询知识库文档列表: page={get_knowledge_document_list_request.page} page_size={get_knowledge_document_list_request.page_size}"
    )
    base_stmt = select(models_knowledge_document.KnowledgeDocument)

    if get_knowledge_document_list_request.keyword:
        base_stmt = base_stmt.where(
            models_knowledge_document.KnowledgeDocument.title.like(
                f"%{get_knowledge_document_list_request.keyword}%"
            )
        )

    if get_knowledge_document_list_request.doc_type is not None:
        base_stmt = base_stmt.where(
            models_knowledge_document.KnowledgeDocument.doc_type
            == get_knowledge_document_list_request.doc_type
        )

    if get_knowledge_document_list_request.stock_code is not None:
        base_stmt = base_stmt.where(
            models_knowledge_document.KnowledgeDocument.stock_code
            == get_knowledge_document_list_request.stock_code
        )

    if get_knowledge_document_list_request.chunk_status is not None:
        base_stmt = base_stmt.where(
            models_knowledge_document.KnowledgeDocument.chunk_status
            == get_knowledge_document_list_request.chunk_status
        )

    if get_knowledge_document_list_request.vector_status is not None:
        base_stmt = base_stmt.where(
            models_knowledge_document.KnowledgeDocument.vector_status
            == get_knowledge_document_list_request.vector_status
        )

    total = db.scalar(select(func.count()).select_from(base_stmt.subquery()))

    sort_column = (
        models_knowledge_document.KnowledgeDocument.updated_at
        if get_knowledge_document_list_request.sort_by == "updated_at"
        else models_knowledge_document.KnowledgeDocument.created_at
    )

    result = db.scalars(
        base_stmt.order_by(
            sort_column.desc()
            if get_knowledge_document_list_request.sort_order == "desc"
            else sort_column.asc()
        )
        .offset(
            (get_knowledge_document_list_request.page - 1)
            * get_knowledge_document_list_request.page_size
        )
        .limit(get_knowledge_document_list_request.page_size)
    )

    logger.info(
        f"知识库文档列表查询完成: total={total} page={get_knowledge_document_list_request.page}"
    )
    return PaginatedResponse[schemas_knowledge_base.GetKnowledgeDocumentListResponse](
        lists=[
            schemas_knowledge_base.GetKnowledgeDocumentListResponse.model_validate(item)
            for item in result
        ],
        pagination=PaginationInfo(
            page=get_knowledge_document_list_request.page,
            page_size=get_knowledge_document_list_request.page_size,
            total=total,
            total_pages=(
                math.ceil(total / get_knowledge_document_list_request.page_size)
                if total
                else 0
            ),
        ),
    )


def chunk_documents(
    db: Session,
    chunk_documents_request: schemas_knowledge_base.ChunkDocumentsRequest,
):
    """对知识库文档执行切块"""
    results: list[schemas_knowledge_base.ChunkDocumentItem] = []
    success_count = 0
    failed_count = 0

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=settings.CHUNK_SIZE,
        chunk_overlap=settings.CHUNK_OVERLAP,
        separators=settings.CHUNK_SEPARATORS,
        length_function=len,
    )

    for document_id in chunk_documents_request.document_ids:
        try:
            doc_entity = db.get(
                models_knowledge_document.KnowledgeDocument, document_id
            )
            if not doc_entity:
                logger.warning(f"文档不存在: document_id={document_id}")
                results.append(
                    schemas_knowledge_base.ChunkDocumentItem(
                        document_id=document_id,
                        title="",
                        chunk_count=0,
                        success=False,
                        error="文档不存在",
                    )
                )
                failed_count += 1
                continue

            if not doc_entity.source_path or not os.path.exists(doc_entity.source_path):
                logger.warning(f"PDF源文件不存在: document_id={document_id}")
                results.append(
                    schemas_knowledge_base.ChunkDocumentItem(
                        document_id=document_id,
                        title=doc_entity.title,
                        chunk_count=0,
                        success=False,
                        error="PDF源文件不存在",
                    )
                )
                failed_count += 1
                continue

            logger.info(f"开始切块: document_id={document_id} title={doc_entity.title}")

            doc_entity.chunk_status = 1
            commit_or_rollback(db)

            # 清理旧切块
            db.execute(
                delete(models_knowledge_chunk.KnowledgeChunk).where(
                    models_knowledge_chunk.KnowledgeChunk.document_id == document_id
                )
            )
            commit_or_rollback(db)
            logger.info(f"已清理旧切块: document_id={document_id}")

            pages = _read_pdf_pages(doc_entity.source_path)
            doc_entity.page_count = len(pages)
            logger.info(
                f"PDF读取完成: document_id={document_id} total_pages={len(pages)}"
            )

            chunk_entities = []
            chunk_counter = 0
            for pg in pages:
                chunks = splitter.split_text(pg["text"])
                for chunk_text in chunks:
                    chunk_hash = hashlib.sha256(f"{document_id}:{chunk_counter}:{chunk_text}".encode("utf-8")).hexdigest()
                    chunk_entities.append(
                        models_knowledge_chunk.KnowledgeChunk(
                            document_id=document_id,
                            doc_type=doc_entity.doc_type,
                            stock_code=doc_entity.stock_code,
                            page_no=pg["page_no"],
                            chunk_index=chunk_counter,
                            chunk_text=chunk_text,
                            chunk_hash=chunk_hash,
                            char_count=len(chunk_text),
                        )
                    )
                    chunk_counter += 1

            if chunk_entities:
                db.add_all(chunk_entities)

            doc_entity.chunk_count = len(chunk_entities)
            doc_entity.chunk_status = 2
            doc_entity.chunk_error_message = None
            commit_or_rollback(db)

            logger.info(
                f"切块完成: document_id={document_id} chunk_count={len(chunk_entities)}"
            )
            results.append(
                schemas_knowledge_base.ChunkDocumentItem(
                    document_id=document_id,
                    title=doc_entity.title,
                    chunk_count=len(chunk_entities),
                    success=True,
                    error=None,
                )
            )
            success_count += 1

        except Exception as e:
            logger.error(
                f"切块失败: document_id={document_id} error={e}", exc_info=True
            )
            try:
                if doc_entity:
                    doc_entity.chunk_status = 3
                    doc_entity.chunk_error_message = str(e)[:500]
                    commit_or_rollback(db)
            except Exception:
                db.rollback()

            results.append(
                schemas_knowledge_base.ChunkDocumentItem(
                    document_id=document_id,
                    title=doc_entity.title if doc_entity else "",
                    chunk_count=0,
                    success=False,
                    error=str(e)[:200],
                )
            )
            failed_count += 1

    logger.info(
        f"批量切块完成: total={chunk_documents_request.document_ids.__len__()} success={success_count} failed={failed_count}"
    )
    return schemas_knowledge_base.ChunkDocumentsResponse(
        total=len(chunk_documents_request.document_ids),
        success_count=success_count,
        failed_count=failed_count,
        results=results,
    )


def upload_knowledge_documents(db: Session, file_list: list[UploadFile]):
    """批量上传知识库文档PDF"""
    if file_list is None or len(file_list) == 0:
        logger.error("上传文件列表为空")
        raise ServiceException(ErrorCode.PARAM_ERROR, "上传文件列表为空")

    success_count = 0
    failed_count = 0
    success_documents: list[schemas_knowledge_base.UploadDocumentItem] = []
    failed_files: list[schemas_knowledge_base.UploadFailedFileItem] = []
    logger.info(f"收到知识库文档上传请求: 共 {len(file_list)} 个文件")

    for file in file_list:
        file_path = None
        try:
            logger.info(f"开始处理文件: {file.filename}")

            if not file.filename.endswith(".pdf"):
                logger.error(f"文件 {file.filename} 不是PDF文件")
                failed_files.append(
                    schemas_knowledge_base.UploadFailedFileItem(
                        file_name=file.filename,
                        error=f"文件 {file.filename} 不是PDF文件",
                    )
                )
                failed_count += 1
                continue

            match_name = file.filename.removesuffix(".pdf")

            doc_entity = db.scalar(
                select(models_knowledge_document.KnowledgeDocument).where(
                    models_knowledge_document.KnowledgeDocument.title == match_name,
                    models_knowledge_document.KnowledgeDocument.metadata_status == 1,
                )
            )

            if not doc_entity:
                # Windows 文件名不允许 /，下载到 Windows 时 / 会被自动替换为 _
                alt_name = match_name.replace("_", "/")
                if alt_name != match_name:
                    doc_entity = db.scalar(
                        select(models_knowledge_document.KnowledgeDocument).where(
                            models_knowledge_document.KnowledgeDocument.title == alt_name,
                            models_knowledge_document.KnowledgeDocument.metadata_status == 1,
                        )
                    )

            if not doc_entity:
                logger.warning(f"未匹配到元数据记录: {file.filename}")
                failed_files.append(
                    schemas_knowledge_base.UploadFailedFileItem(
                        file_name=file.filename,
                        error=f'未匹配到元数据记录（标题="{match_name}" 且状态为待上传）',
                    )
                )
                failed_count += 1
                continue

            if doc_entity.source_path and os.path.exists(doc_entity.source_path):
                logger.warning(f"文档已有PDF文件: document_id={doc_entity.id}")
                failed_files.append(
                    schemas_knowledge_base.UploadFailedFileItem(
                        file_name=file.filename,
                        error=f'文档 "{doc_entity.title}" 已上传过PDF',
                    )
                )
                failed_count += 1
                continue

            bytes_content = file.file.read()
            file_hash = hashlib.sha256(bytes_content).hexdigest()

            saved_name = f"{doc_entity.id}_{file_hash[:16]}.pdf"
            file_path = os.path.join(settings.RESEARCH_REPORT_UPLOAD_DIR, saved_name)

            save_file(bytes_content, file_path)
            logger.info(f"文件已保存: {file.filename} → {file_path}")

            doc_entity.source_path = file_path
            doc_entity.doc_hash = file_hash
            doc_entity.metadata_status = 2
            commit_or_rollback(db)

            logger.info(
                f"文档上传成功: document_id={doc_entity.id} title={doc_entity.title}"
            )
            success_documents.append(
                schemas_knowledge_base.UploadDocumentItem(
                    document_id=doc_entity.id,
                    title=doc_entity.title,
                    file_name=file.filename,
                )
            )
            success_count += 1

        except Exception as e:
            logger.error(f"处理文件 {file.filename} 时出错: {e}", exc_info=True)
            failed_files.append(
                schemas_knowledge_base.UploadFailedFileItem(
                    file_name=file.filename, error=str(e)[:200]
                )
            )
            failed_count += 1
            if file_path:
                try:
                    logger.info(f"删除临时文件: {file_path}")
                    os.remove(file_path)
                except FileNotFoundError:
                    pass

    logger.info(
        f"知识库文档上传处理完成: total={len(file_list)} success={success_count} failed={failed_count}"
    )
    return schemas_knowledge_base.UploadKnowledgeDocumentResponse(
        total=len(file_list),
        success_count=success_count,
        failed_count=failed_count,
        success_documents=success_documents,
        failed_files=failed_files,
    )


"""辅助函数"""


def _import_excel(
    db: Session,
    excel_file: UploadFile,
    doc_type: str,
    column_map: dict[str, str],
):
    """解析导入研报Excel文件"""
    success_count = 0
    errors: list[schemas_knowledge_base.InitErrorItem] = []

    filename = excel_file.filename
    logger.info(f"开始解析Excel: file={filename}")
    if not filename.endswith((".xlsx", ".xls")):
        raise ServiceException(
            ErrorCode.UNSUPPORTED_FILE_FORMAT,
            f"文件 {filename} 格式不支持，仅支持 xlsx 和 xls",
        )

    with tempfile.NamedTemporaryFile(
        delete=False, suffix=os.path.splitext(filename)[1]
    ) as f:
        f.write(excel_file.file.read())
        temp_path = f.name
    logger.info(f"临时文件已保存: path={temp_path} size={os.path.getsize(temp_path)}")

    try:
        df = pd.read_excel(temp_path, sheet_name=0)
        data = df.to_dict(orient="records")
    except Exception as exc:
        logger.error(f"Excel解析失败: {filename} error={exc}", exc_info=True)
        raise ServiceException(
            ErrorCode.PARAM_ERROR, f"文件 {filename} 解析失败，请检查内容"
        ) from exc
    finally:
        try:
            os.unlink(temp_path)
        except Exception as exc:
            logger.warning(f"临时文件删除失败: path={temp_path} error={exc}")

    logger.info(f"Excel解析完成: {filename} 总行数={len(data)}")
    if not data:
        logger.warning(f"Excel文件无有效数据: file={filename}")

    # 预先查询已存在的记录（以 doc_type + title 为去重键）
    existing_titles: set[str] = set()
    existing_stmt = select(models_knowledge_document.KnowledgeDocument.title).where(
        models_knowledge_document.KnowledgeDocument.doc_type == doc_type,
    )
    for title in db.scalars(existing_stmt):
        if title.strip():
            existing_titles.add(title.strip())

    entities_to_add: list[models_knowledge_document.KnowledgeDocument] = []
    duplicate_count = 0

    logger.info(f"开始逐行处理: file={filename} doc_type={doc_type}")
    for row_idx, row in enumerate(data):
        try:
            data = {}
            # 公共字段
            data["metadata_status"] = 1

            data["title"] = str(row.get(column_map.get("title"), "")).strip()
            if not data["title"]:
                logger.warning(f"行{row_idx + 2}标题为空，跳过")
                errors.append(
                    schemas_knowledge_base.InitErrorItem(
                        row=row_idx + 2, error="标题为空"
                    )
                )
                continue

            # 去重检查
            if data["title"] in existing_titles:
                duplicate_count += 1
                logger.info(f"行{row_idx + 2}文档已存在，跳过: title={data['title']}")
                continue
            data["org_code"] = str(row.get(column_map.get("org_code"), "")).strip()
            data["org_name"] = str(row.get(column_map.get("org_name"), "")).strip()
            data["publish_date"] = pd.to_datetime(
                row.get(column_map.get("publish_date"), "")
            ).date()
            data["researcher"] = str(row.get(column_map.get("researcher"), "")).strip()
            data["industry_name"] = str(
                row.get(column_map.get("industry_name"), "")
            ).strip()
            data["em_rating_name"] = str(
                row.get(column_map.get("em_rating_name"), "")
            ).strip()
            data["last_em_rating_name"] = str(
                row.get(column_map.get("last_em_rating_name"), "")
            ).strip()
            data["s_rating_name"] = str(
                row.get(column_map.get("s_rating_name"), "")
            ).strip()
            data["s_rating_code"] = str(
                row.get(column_map.get("s_rating_code"), "")
            ).strip()

            # 个股研报
            if doc_type == constants_knowledge_base.DOC_TYPE_RESEARCH_REPORT:
                data["stock_code"] = (
                    str(row.get(column_map.get("stock_code"), "")).strip().zfill(6)
                )
                data["stock_abbr"] = str(
                    row.get(column_map.get("stock_abbr"), "")
                ).strip()
                data["predict_next_two_year_eps"] = str(
                    row.get(column_map.get("predict_next_two_year_eps"), "")
                ).strip()
                data["predict_next_two_year_pe"] = str(
                    row.get(column_map.get("predict_next_two_year_pe"), "")
                ).strip()
                data["predict_next_year_eps"] = str(
                    row.get(column_map.get("predict_next_year_eps"), "")
                ).strip()
                data["predict_next_year_pe"] = str(
                    row.get(column_map.get("predict_next_year_pe"), "")
                ).strip()
                data["predict_this_year_eps"] = str(
                    row.get(column_map.get("predict_this_year_eps"), "")
                ).strip()
                data["predict_this_year_pe"] = str(
                    row.get(column_map.get("predict_this_year_pe"), "")
                ).strip()
                data["predict_last_year_eps"] = str(
                    row.get(column_map.get("predict_last_year_eps"), "")
                ).strip()
                data["predict_last_year_pe"] = str(
                    row.get(column_map.get("predict_last_year_pe"), "")
                ).strip()
                data["indv_is_new"] = str(
                    row.get(column_map.get("indv_is_new"), "")
                ).strip()
                data["new_listing_date"] = str(
                    row.get(column_map.get("new_listing_date"), "")
                ).strip()
                data["new_purchase_date"] = str(
                    row.get(column_map.get("new_purchase_date"), "")
                ).strip()
                data["new_issue_price"] = str(
                    row.get(column_map.get("new_issue_price"), "")
                ).strip()
                data["new_pe_issue_a"] = str(
                    row.get(column_map.get("new_pe_issue_a"), "")
                ).strip()
                data["indv_aim_price_t"] = str(
                    row.get(column_map.get("indv_aim_price_t"), "")
                ).strip()
                data["indv_aim_price_l"] = str(
                    row.get(column_map.get("indv_aim_price_l"), "")
                ).strip()
                data["market"] = str(row.get(column_map.get("market"), "")).strip()

                for k in list(data):
                    v = data[k]
                    if isinstance(v, str) and v.lower() in ("nan", "none", ""):
                        data[k] = None

                entity = models_knowledge_document.KnowledgeDocument(
                    **data, doc_type=doc_type
                )

            # 行业研报
            if doc_type == constants_knowledge_base.DOC_TYPE_INDUSTRY_REPORT:
                data["org_S_Name"] = str(
                    row.get(column_map.get("org_S_Name"), "")
                ).strip()

                entity = models_knowledge_document.KnowledgeDocument(
                    **data, doc_type=doc_type
                )

            entities_to_add.append(entity)
            # 写入已处理集合，防止同一文件内的重复标题
            existing_titles.add(data["title"])

        except Exception as exc:
            logger.error(f"行{row_idx + 2}处理失败: {exc}", exc_info=True)
            errors.append(
                schemas_knowledge_base.InitErrorItem(row=row_idx + 2, error=str(exc))
            )

    if entities_to_add:
        db.add_all(entities_to_add)
        commit_or_rollback(db)
        success_count = len(entities_to_add)
        logger.info(f"批量写入完成: doc_type={doc_type} 写入{success_count}条")
    else:
        logger.warning(f"无有效数据入库: file={filename} doc_type={doc_type}")

    logger.info(
        f"Excel整体处理完成: file={filename} success={success_count} duplicates={duplicate_count} failed={len(errors)}"
    )
    return success_count, errors, duplicate_count


def _read_pdf_pages(file_path: str):
    """读取PDF文件全部页面文本"""
    reader = PdfReader(file_path)
    pages: list[dict] = []
    for i, page in enumerate(reader.pages):
        text = page.extract_text()
        if text:
            text = text.replace("\r", "\n")
            text = re.sub(r"[ \t]+", " ", text)
            text = re.sub(r"\n+", "\n", text)
            text = text.strip()
        else:
            text = ""
        pages.append({"page_no": i + 1, "text": text})
    return pages
