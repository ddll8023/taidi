"""知识库管理服务"""

import hashlib
import json
import math
import os
import re
import tempfile
from pathlib import Path
from app.utils.mineru import run_mineru_parse

import pandas as pd
from fastapi import UploadFile
from langchain_text_splitters import RecursiveCharacterTextSplitter
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.constants import knowledge_base as constants_knowledge_base
from app.core.config import settings
from app.db.chroma import (
    add_texts_to_kb,
    delete_by_filter as chroma_delete_by_filter,
    search_kb as chroma_search_kb,
)
from app.db.database import commit_or_rollback
from app.models import knowledge_document as models_knowledge_document
from app.models import knowledge_chunk as models_knowledge_chunk
from app.schemas import knowledge_base as schemas_knowledge_base
from app.schemas.common import ErrorCode, PaginatedResponse, PaginationInfo
from app.utils.exception import ServiceException
from app.utils.file import save_file
from app.utils.logger_config import setup_logger
from app.models import company_basic_info as models_company_basic_info

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
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "操作失败") from e

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

    doc_chunk_status_rows = db.execute(
        select(
            models_knowledge_document.KnowledgeDocument.chunk_status,
            func.count(models_knowledge_document.KnowledgeDocument.id),
        ).group_by(models_knowledge_document.KnowledgeDocument.chunk_status)
    ).all()
    doc_by_chunk_status = {str(row[0]): row[1] for row in doc_chunk_status_rows}

    doc_vector_status_rows = db.execute(
        select(
            models_knowledge_document.KnowledgeDocument.vector_status,
            func.count(models_knowledge_document.KnowledgeDocument.id),
        ).group_by(models_knowledge_document.KnowledgeDocument.vector_status)
    ).all()
    doc_by_vector_status = {str(row[0]): row[1] for row in doc_vector_status_rows}

    doc_parse_status_rows = db.execute(
        select(
            models_knowledge_document.KnowledgeDocument.parse_status,
            func.count(models_knowledge_document.KnowledgeDocument.id),
        ).group_by(models_knowledge_document.KnowledgeDocument.parse_status)
    ).all()
    doc_by_parse_status = {str(row[0]): row[1] for row in doc_parse_status_rows}

    doc_type_rows = db.execute(
        select(
            models_knowledge_document.KnowledgeDocument.doc_type,
            func.count(models_knowledge_document.KnowledgeDocument.id),
        ).group_by(models_knowledge_document.KnowledgeDocument.doc_type)
    ).all()
    doc_by_type = {row[0]: row[1] for row in doc_type_rows}

    logger.info(f"知识库统计查询完成: doc_total={doc_total}")
    return schemas_knowledge_base.GetKnowledgeBaseStatsResponse(
        documents=schemas_knowledge_base.DocumentStatsItem(
            total=doc_total,
            by_chunk_status=doc_by_chunk_status,
            by_vector_status=doc_by_vector_status,
            by_doc_type=doc_by_type,
            by_parse_status=doc_by_parse_status,
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

    if get_knowledge_document_list_request.parse_status is not None:
        base_stmt = base_stmt.where(
            models_knowledge_document.KnowledgeDocument.parse_status
            == get_knowledge_document_list_request.parse_status
        )

    if get_knowledge_document_list_request.clean_status is not None:
        base_stmt = base_stmt.where(
            models_knowledge_document.KnowledgeDocument.clean_status
            == get_knowledge_document_list_request.clean_status
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
                alt_name = match_name.replace("_", "/")
                if alt_name != match_name:
                    doc_entity = db.scalar(
                        select(models_knowledge_document.KnowledgeDocument).where(
                            models_knowledge_document.KnowledgeDocument.title
                            == alt_name,
                            models_knowledge_document.KnowledgeDocument.metadata_status
                            == 1,
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


def parse_documents(
    db: Session,
    parse_documents_request: schemas_knowledge_base.ParseDocumentsRequest,
):
    """批量解析文档（MinerU结构化解析）"""
    logger.info(
        f"收到批量解析请求: document_ids={parse_documents_request.document_ids}"
    )
    results: list[schemas_knowledge_base.ParseDocumentsItem] = []
    success_count = 0
    failed_count = 0
    document_ids = parse_documents_request.document_ids

    # 批量查询所有文档
    docs = db.scalars(
        select(models_knowledge_document.KnowledgeDocument).where(
            models_knowledge_document.KnowledgeDocument.id.in_(document_ids)
        )
    ).all()

    commit_or_rollback(db)

    # 逐个文档执行解析
    for doc_entity in docs:
        document_id = doc_entity.id
        try:
            logger.info(f"开始转换: document_id={document_id} title={doc_entity.title}")

            doc_entity.parse_status = constants_knowledge_base.PARSE_STATUS_PARSING
            commit_or_rollback(db)

            output_dir = _get_parse_output_dir(document_id, doc_entity.title)
            os.makedirs(output_dir, exist_ok=True)

            # 幂等：已有 .md 产出则跳过
            md_files = list(Path(output_dir).rglob("*.md"))
            if md_files:
                logger.info(f"复用已有转换结果: document_id={document_id}")
            else:
                run_mineru_parse(doc_entity.source_path, output_dir)
                logger.info(f"MinerU转换完成: document_id={document_id}")

            doc_entity.parse_status = constants_knowledge_base.PARSE_STATUS_COMPLETED
            doc_entity.parse_error_message = None
            commit_or_rollback(db)
            logger.info(f"转换完成: document_id={document_id}")
            results.append(
                schemas_knowledge_base.ParseDocumentsItem(
                    document_id=document_id,
                    title=doc_entity.title,
                    success=True,
                    error=None,
                )
            )
            success_count += 1

        except Exception as e:
            logger.error(
                f"解析失败: document_id={document_id} error={e}", exc_info=True
            )
            try:
                doc_entity.parse_status = constants_knowledge_base.PARSE_STATUS_FAILED
                doc_entity.parse_error_message = str(e)[:500]
                commit_or_rollback(db)
            except Exception:
                db.rollback()

            results.append(
                schemas_knowledge_base.ParseDocumentsItem(
                    document_id=document_id,
                    title=doc_entity.title,
                    success=False,
                    error=str(e)[:200],
                )
            )
            failed_count += 1

    logger.info(
        f"批量解析完成: total={len(document_ids)} success={success_count} failed={failed_count}"
    )
    return schemas_knowledge_base.ParseDocumentsResponse(
        total=len(document_ids),
        success_count=success_count,
        failed_count=failed_count,
        results=results,
    )


def get_parse_result(db: Session, document_id: int):
    """获取单个文档的解析结果"""
    logger.info(f"查询解析结果: document_id={document_id}")
    doc_entity = db.get(models_knowledge_document.KnowledgeDocument, document_id)
    if not doc_entity:
        raise ServiceException(ErrorCode.PARAM_ERROR, "文档不存在")

    output_dir = _get_parse_output_dir(document_id, doc_entity.title)

    # 检查是否存在转换结果文件
    md_files = list(Path(output_dir).rglob("*.md"))
    if not md_files:
        raise ServiceException(ErrorCode.PARAM_ERROR, "该文档尚未转换")

    markdown_content = md_files[0].read_text(encoding="utf-8")

    logger.info(f"转换结果查询完成: document_id={document_id}")
    return schemas_knowledge_base.GetParseResultResponse(
        document_id=document_id,
        title=doc_entity.title,
        markdown_content=markdown_content,
    )


def save_parse_result(
    db: Session,
    request: schemas_knowledge_base.SaveParseResultRequest,
):
    """保存清洗后的Markdown到解析结果文件"""
    logger.info(f"保存清洗结果: document_id={request.document_id}")

    doc_entity = db.get(
        models_knowledge_document.KnowledgeDocument, request.document_id
    )
    if not doc_entity:
        raise ServiceException(ErrorCode.PARAM_ERROR, "文档不存在")

    output_dir = _get_parse_output_dir(request.document_id, doc_entity.title)
    md_files = list(Path(output_dir).rglob("*.md"))
    if not md_files:
        logger.error(f"未找到转换结果文件: document_id={request.document_id}")
        raise ServiceException(ErrorCode.PARAM_ERROR, "该文档尚未转换，无法保存")

    target_path = md_files[0]
    try:
        target_path.write_text(request.markdown_content, encoding="utf-8")
    except Exception as e:
        logger.error(
            f"写入Markdown文件失败: path={target_path} error={e}", exc_info=True
        )
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "保存清洗结果失败")

    commit_or_rollback(db)

    logger.info(f"清洗结果保存成功: document_id={request.document_id}")
    return schemas_knowledge_base.SaveParseResultResponse(
        document_id=request.document_id,
        title=doc_entity.title,
        saved=True,
        clean_status=doc_entity.clean_status,
    )


def toggle_clean_status(db: Session, document_id: int):
    """切换文档清洗标记"""
    logger.info(f"切换清洗标记: document_id={document_id}")
    doc_entity = db.get(models_knowledge_document.KnowledgeDocument, document_id)
    if not doc_entity:
        raise ServiceException(ErrorCode.PARAM_ERROR, "文档不存在")

    doc_entity.clean_status = (
        constants_knowledge_base.CLEAN_STATUS_PENDING
        if doc_entity.clean_status == constants_knowledge_base.CLEAN_STATUS_DONE
        else constants_knowledge_base.CLEAN_STATUS_DONE
    )
    commit_or_rollback(db)

    logger.info(
        f"清洗标记已切换: document_id={document_id} clean_status={doc_entity.clean_status}"
    )
    return schemas_knowledge_base.ToggleCleanStatusResponse(
        document_id=document_id,
        title=doc_entity.title,
        clean_status=doc_entity.clean_status,
    )


def chunk_documents(
    db: Session,
    request: schemas_knowledge_base.ChunkDocumentsRequest,
):
    """批量清洗后文档切块"""
    logger.info(f"收到批量切块请求: document_ids={request.document_ids}")
    results: list[schemas_knowledge_base.ChunkDocumentsItem] = []
    success_count = 0
    failed_count = 0
    document_ids = request.document_ids

    docs = db.scalars(
        select(models_knowledge_document.KnowledgeDocument).where(
            models_knowledge_document.KnowledgeDocument.id.in_(document_ids)
        )
    ).all()

    # 统一将所有文档标记为"向量化中"
    for doc in docs:
        doc.vector_status = constants_knowledge_base.VECTOR_STATUS_VECTORIZING
    # 统一更新 chunk 状态为向量化中
    db.execute(
        models_knowledge_chunk.KnowledgeChunk.__table__.update()
        .where(
            models_knowledge_chunk.KnowledgeChunk.document_id.in_(document_ids),
            models_knowledge_chunk.KnowledgeChunk.vector_status
            == constants_knowledge_base.VECTOR_STATUS_PENDING,
        )
        .values(vector_status=constants_knowledge_base.VECTOR_STATUS_VECTORIZING)
    )
    commit_or_rollback(db)

    for doc in docs:
        doc_entity = doc
        try:
            doc_entity.chunk_status = constants_knowledge_base.CHUNK_STATUS_CHUNKING
            commit_or_rollback(db)

            chunk_count = _chunk_single_document(db, doc_entity)

            doc_entity.chunk_status = constants_knowledge_base.CHUNK_STATUS_COMPLETED
            doc_entity.chunk_count = chunk_count
            doc_entity.chunk_error_message = None
            commit_or_rollback(db)

            logger.info(
                f"切块完成: document_id={doc_entity.id} chunk_count={chunk_count}"
            )
            results.append(
                schemas_knowledge_base.ChunkDocumentsItem(
                    document_id=doc_entity.id,
                    title=doc_entity.title,
                    success=True,
                    chunk_count=chunk_count,
                )
            )
            success_count += 1

        except Exception as e:
            logger.error(
                f"切块失败: document_id={doc_entity.id} error={e}", exc_info=True
            )
            try:
                doc_entity.chunk_status = constants_knowledge_base.CHUNK_STATUS_FAILED
                doc_entity.chunk_error_message = str(e)[:500]
                commit_or_rollback(db)
            except Exception:
                db.rollback()

            results.append(
                schemas_knowledge_base.ChunkDocumentsItem(
                    document_id=doc_entity.id,
                    title=doc_entity.title,
                    success=False,
                    error=str(e)[:200],
                )
            )
            failed_count += 1

    logger.info(
        f"批量切块完成: total={len(document_ids)} success={success_count} failed={failed_count}"
    )
    return schemas_knowledge_base.ChunkDocumentsResponse(
        total=len(document_ids),
        success_count=success_count,
        failed_count=failed_count,
        results=results,
    )


def vectorize_documents(
    db: Session,
    request: schemas_knowledge_base.VectorizeDocumentsRequest,
):
    """批量文档向量化"""
    logger.info(f"收到批量向量化请求: document_ids={request.document_ids}")
    results: list[schemas_knowledge_base.VectorizeDocumentsItem] = []
    success_count = 0
    failed_count = 0
    document_ids = request.document_ids

    docs = db.scalars(
        select(models_knowledge_document.KnowledgeDocument).where(
            models_knowledge_document.KnowledgeDocument.id.in_(document_ids)
        )
    ).all()

    # 统一将所有文档标记为"向量化中"
    for doc in docs:
        doc.vector_status = constants_knowledge_base.VECTOR_STATUS_VECTORIZING
    # 统一更新 chunk 状态为向量化中
    db.execute(
        models_knowledge_chunk.KnowledgeChunk.__table__.update()
        .where(
            models_knowledge_chunk.KnowledgeChunk.document_id.in_(document_ids),
            models_knowledge_chunk.KnowledgeChunk.vector_status
            == constants_knowledge_base.VECTOR_STATUS_PENDING,
        )
        .values(vector_status=constants_knowledge_base.VECTOR_STATUS_VECTORIZING)
    )
    commit_or_rollback(db)

    for doc in docs:
        try:
            chunk_count = _vectorize_single_document(db, doc)

            logger.info(f"向量化完成: document_id={doc.id} chunk_count={chunk_count}")
            results.append(
                schemas_knowledge_base.VectorizeDocumentsItem(
                    document_id=doc.id,
                    title=doc.title,
                    success=True,
                    chunk_count=chunk_count,
                )
            )
            success_count += 1

        except Exception as e:
            logger.error(f"向量化失败: document_id={doc.id} error={e}", exc_info=True)
            try:
                doc.vector_status = constants_knowledge_base.VECTOR_STATUS_FAILED
                commit_or_rollback(db)
            except Exception:
                db.rollback()

            results.append(
                schemas_knowledge_base.VectorizeDocumentsItem(
                    document_id=doc.id,
                    title=doc.title,
                    success=False,
                    error=str(e)[:200],
                )
            )
            failed_count += 1

    logger.info(
        f"批量向量化完成: total={len(document_ids)} success={success_count} failed={failed_count}"
    )
    return schemas_knowledge_base.VectorizeDocumentsResponse(
        total=len(document_ids),
        success_count=success_count,
        failed_count=failed_count,
        results=results,
    )


def search_knowledge(
    db: Session,
    request: schemas_knowledge_base.SearchKnowledgeRequest,
):
    """知识库语义检索（分路检索 + 合并重排序）"""
    logger.info(f"收到检索请求: query={request.query[:50]}")

    all_results = []

    # 路1：个股研报
    if request.stock_codes:
        for stock_code in request.stock_codes:
            chunk_results = chroma_search_kb(
                request.query,
                filter_dict={
                    "doc_type": constants_knowledge_base.DOC_TYPE_RESEARCH_REPORT,
                    "stock_code": stock_code,
                },
                k=request.top_k,
            )
            all_results.extend(_format_chroma_results(chunk_results))

        # 自动推定行业：公司同行业则补行业研报
        if not request.industry_names:
            _auto_append_industry_search(db, request, all_results)

    # 路2：行业研报（显式指定）
    if request.industry_names:
        for industry in request.industry_names:
            chunk_results = chroma_search_kb(
                request.query,
                filter_dict={
                    "doc_type": constants_knowledge_base.DOC_TYPE_INDUSTRY_REPORT,
                    "industry_name": industry,
                },
                k=request.top_k,
            )
            all_results.extend(_format_chroma_results(chunk_results))

    # 路3：全量搜索（无过滤条件）
    if not request.stock_codes and not request.industry_names:
        chunk_results = chroma_search_kb(request.query, k=request.top_k)
        all_results = _format_chroma_results(chunk_results)

    # 合并去重 + 按 score 降序
    seen = set()
    deduped = []
    for item in sorted(all_results, key=lambda x: x["score"], reverse=True):
        dedup_key = (item["document_id"], item["chunk_index"])
        if dedup_key not in seen:
            seen.add(dedup_key)
            deduped.append(item)

    top_results: list[dict] = deduped[: request.top_k]

    logger.info(f"检索完成: query={request.query[:50]} results={len(top_results)}")
    return schemas_knowledge_base.SearchKnowledgeResponse(
        query=request.query,
        total=len(top_results),
        results=[
            schemas_knowledge_base.SearchKnowledgeItem(**item) for item in top_results
        ],
    )


"""辅助函数"""


def _str_or_none(value):
    """将 pandas/native 值转为 str|None，清理 nan/none/空字符串"""
    if value is None:
        return None
    s = str(value).strip()
    if s.lower() in ("nan", "none", ""):
        return None
    return s


def _get_parse_output_dir(document_id: int, title: str):
    """获取文档解析输出目录（统一入口）"""
    return os.path.join(settings.KNOWLEDGE_PARSE_OUTPUT_DIR, f"{document_id}_{title}")


def _vectorize_single_document(db: Session, doc_entity):
    """对单个文档的所有chunk执行向量化"""
    chunk_entities = db.scalars(
        select(models_knowledge_chunk.KnowledgeChunk).where(
            models_knowledge_chunk.KnowledgeChunk.document_id == doc_entity.id,
            models_knowledge_chunk.KnowledgeChunk.vector_status
            == constants_knowledge_base.VECTOR_STATUS_VECTORIZING,
        )
    ).all()

    if not chunk_entities:
        logger.info(f"无需向量化: document_id={doc_entity.id}")
        return 0

    # 清除 Chroma 中该文档的旧向量（幂等）
    chroma_delete_by_filter({"document_id": doc_entity.id})

    texts = []
    metadatas = []
    ids = []
    industry_name = getattr(doc_entity, "industry_name", None) or ""

    for chunk in chunk_entities:
        texts.append(chunk.chunk_text)
        metadatas.append(
            {
                "document_id": doc_entity.id,
                "chunk_index": chunk.chunk_index,
                "doc_type": doc_entity.doc_type,
                "stock_code": chunk.stock_code or "",
                "stock_abbr": getattr(doc_entity, "stock_abbr", None) or "",
                "industry_name": industry_name,
                "content_type": chunk.content_type,
            }
        )
        ids.append(chunk.chunk_hash)

    # 分片批量写入 Chroma
    batch_size = constants_knowledge_base.EMBEDDING_BATCH_SIZE
    total_chunks = len(texts)
    for i in range(0, total_chunks, batch_size):
        batch_end = min(i + batch_size, total_chunks)
        add_texts_to_kb(
            texts=texts[i:batch_end],
            metadatas=metadatas[i:batch_end],
            ids=ids[i:batch_end],
        )

    # 更新每个 chunk 的向量化状态
    for chunk in chunk_entities:
        chunk.vector_status = constants_knowledge_base.VECTOR_STATUS_COMPLETED
        chunk.vector_model = settings.EMBEDDING_MODEL
        chunk.vector_dim = settings.EMBEDDING_DIM

    # 更新文档的向量化状态
    doc_entity.vector_status = constants_knowledge_base.VECTOR_STATUS_COMPLETED
    commit_or_rollback(db)

    logger.info(f"向量化完成: document_id={doc_entity.id} chunks={total_chunks}")
    return total_chunks


def _format_chroma_results(chunk_results: list) -> list[dict]:
    """将 Chroma 搜索结果格式化为统一 dict 列表"""
    formatted = []
    for doc, score in chunk_results:
        formatted.append(
            {
                "document_id": doc.metadata.get("document_id", 0),
                "chunk_index": doc.metadata.get("chunk_index", 0),
                "chunk_text": doc.page_content,
                "score": float(score),
                "doc_type": doc.metadata.get("doc_type", ""),
                "stock_code": doc.metadata.get("stock_code", None),
                "stock_abbr": doc.metadata.get("stock_abbr", None),
                "industry_name": doc.metadata.get("industry_name", None),
            }
        )
    return formatted


def _auto_append_industry_search(
    db: Session,
    request: schemas_knowledge_base.SearchKnowledgeRequest,
    current_results: list,
):
    """自动推定行业：如果所有公司属于同一行业，补行业研报检索"""
    if not request.stock_codes or request.industry_names:
        return

    company_rows = db.execute(
        select(
            models_company_basic_info.CompanyBasicInfo.stock_code,
            models_company_basic_info.CompanyBasicInfo.csrc_industry,
        ).where(
            models_company_basic_info.CompanyBasicInfo.stock_code.in_(
                request.stock_codes
            )
        )
    ).all()

    industries = {r.stock_code: r.csrc_industry for r in company_rows}
    unique_industries = set(industries.values())

    if len(unique_industries) != 1:
        return

    industry = unique_industries.pop()
    chunk_results = chroma_search_kb(
        request.query,
        filter_dict={
            "doc_type": constants_knowledge_base.DOC_TYPE_INDUSTRY_REPORT,
            "industry_name": industry,
        },
        k=request.top_k,
    )
    current_results.extend(_format_chroma_results(chunk_results))


def _chunk_single_document(db: Session, doc_entity):
    """对单个清洗后文档执行切块：提取表格块，剩余Markdown全文用RecursiveCharacterTextSplitter切分"""
    output_dir = _get_parse_output_dir(doc_entity.id, doc_entity.title)
    md_files = list(Path(output_dir).rglob("*.md"))
    if not md_files:
        logger.error(f"未找到转换结果文件，无法切块: document_id={doc_entity.id}")
        raise ServiceException(ErrorCode.PARAM_ERROR, "该文档尚未转换，无法切块")
    md = md_files[0].read_text(encoding="utf-8")

    # 删除已有切块（幂等）
    existing = select(models_knowledge_chunk.KnowledgeChunk).where(
        models_knowledge_chunk.KnowledgeChunk.document_id == doc_entity.id,
    )
    for old_chunk in db.scalars(existing):
        db.delete(old_chunk)
    commit_or_rollback(db)

    # 用正则一次性提取表格标注区域并移除（re.DOTALL 使 . 匹配换行）
    table_pattern = re.compile(
        r"<!--\s*table:\s*(.+?)\s*-->(.*?)<!--\s*endtable\s*-->",
        re.DOTALL,
    )

    chunks: list[models_knowledge_chunk.KnowledgeChunk] = []

    # 逐个提取表格块
    for match in table_pattern.finditer(md):
        table_desc = match.group(1).strip()
        table_raw = match.group(2).strip()
        chunk = _make_chunk(
            doc_entity,
            len(chunks),
            table_desc,
            content_type=constants_knowledge_base.CONTENT_TYPE_TABLE,
            section_type=constants_knowledge_base.SECTION_TYPE_TABLE_DESC,
            heading_text="",
            table_content=table_raw,
        )
        chunks.append(chunk)

    # 移除所有表格标注区域，剩余纯文本过分割器
    clean_text = table_pattern.sub("", md).strip()
    if clean_text:
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=settings.CHUNK_SIZE,
            chunk_overlap=settings.CHUNK_OVERLAP,
            separators=settings.CHUNK_SEPARATORS,
            add_start_index=False,
        )
        for segment in text_splitter.split_text(clean_text):
            segment = segment.strip()
            if not segment:
                continue
            # 丢弃过短的切块，避免检索噪音
            if len(segment) < settings.CHUNK_MIN_CHARS:
                continue
            chunk = _make_chunk(
                doc_entity,
                len(chunks),
                segment,
                content_type=constants_knowledge_base.CONTENT_TYPE_TEXT,
                section_type=constants_knowledge_base.SECTION_TYPE_PARAGRAPH,
                heading_text="",
            )
            chunks.append(chunk)

    # 批量写入
    if chunks:
        db.add_all(chunks)
        commit_or_rollback(db)

    return len(chunks)


def _make_chunk(
    doc_entity,
    chunk_index: int,
    text: str,
    content_type: str,
    section_type: str,
    heading_text: str,
    table_content: str | None = None,
):
    """构造单个KnowledgeChunk实体"""
    text_bytes = text.encode("utf-8")
    # 表格类型：哈希包含说明文字和表格内容，确保内容完整一致性
    if table_content:
        table_bytes = table_content.encode("utf-8")
        chunk_hash = hashlib.sha256(text_bytes + table_bytes).hexdigest()
    else:
        chunk_hash = hashlib.sha256(text_bytes).hexdigest()
    stock_code = getattr(doc_entity, "stock_code", None) or ""

    return models_knowledge_chunk.KnowledgeChunk(
        document_id=doc_entity.id,
        doc_type=doc_entity.doc_type,
        stock_code=stock_code,
        chunk_index=chunk_index,
        chunk_text=text,
        table_content=table_content,
        chunk_hash=chunk_hash,
        char_count=len(text),
        content_type=content_type,
        section_type=section_type,
        heading_text=heading_text,
    )


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
            import_data = schemas_knowledge_base.KnowledgeDocumentImportItem(
                metadata_status=1,
            )

            raw_title = str(row.get(column_map.get("title"), "")).strip()
            if not raw_title:
                logger.warning(f"行{row_idx + 2}标题为空，跳过")
                errors.append(
                    schemas_knowledge_base.InitErrorItem(
                        row=row_idx + 2, error="标题为空"
                    )
                )
                continue
            import_data.title = raw_title

            if import_data.title in existing_titles:
                duplicate_count += 1
                logger.info(
                    f"行{row_idx + 2}文档已存在，跳过: title={import_data.title}"
                )
                continue

            import_data.org_code = _str_or_none(row.get(column_map.get("org_code")))
            import_data.org_name = _str_or_none(row.get(column_map.get("org_name")))
            import_data.publish_date = pd.to_datetime(
                row.get(column_map.get("publish_date"), "")
            ).date()
            import_data.researcher = _str_or_none(row.get(column_map.get("researcher")))
            import_data.industry_name = _str_or_none(
                row.get(column_map.get("industry_name"))
            )
            import_data.em_rating_name = _str_or_none(
                row.get(column_map.get("em_rating_name"))
            )
            import_data.last_em_rating_name = _str_or_none(
                row.get(column_map.get("last_em_rating_name"))
            )
            import_data.s_rating_name = _str_or_none(
                row.get(column_map.get("s_rating_name"))
            )
            import_data.s_rating_code = _str_or_none(
                row.get(column_map.get("s_rating_code"))
            )

            if doc_type == constants_knowledge_base.DOC_TYPE_RESEARCH_REPORT:
                import_data.stock_code = (
                    _str_or_none(row.get(column_map.get("stock_code"))) or ""
                ).zfill(6)
                import_data.stock_abbr = _str_or_none(
                    row.get(column_map.get("stock_abbr"))
                )
                import_data.predict_next_two_year_eps = _str_or_none(
                    row.get(column_map.get("predict_next_two_year_eps"))
                )
                import_data.predict_next_two_year_pe = _str_or_none(
                    row.get(column_map.get("predict_next_two_year_pe"))
                )
                import_data.predict_next_year_eps = _str_or_none(
                    row.get(column_map.get("predict_next_year_eps"))
                )
                import_data.predict_next_year_pe = _str_or_none(
                    row.get(column_map.get("predict_next_year_pe"))
                )
                import_data.predict_this_year_eps = _str_or_none(
                    row.get(column_map.get("predict_this_year_eps"))
                )
                import_data.predict_this_year_pe = _str_or_none(
                    row.get(column_map.get("predict_this_year_pe"))
                )
                import_data.predict_last_year_eps = _str_or_none(
                    row.get(column_map.get("predict_last_year_eps"))
                )
                import_data.predict_last_year_pe = _str_or_none(
                    row.get(column_map.get("predict_last_year_pe"))
                )
                import_data.indv_is_new = _str_or_none(
                    row.get(column_map.get("indv_is_new"))
                )
                import_data.new_listing_date = _str_or_none(
                    row.get(column_map.get("new_listing_date"))
                )
                import_data.new_purchase_date = _str_or_none(
                    row.get(column_map.get("new_purchase_date"))
                )
                import_data.new_issue_price = _str_or_none(
                    row.get(column_map.get("new_issue_price"))
                )
                import_data.new_pe_issue_a = _str_or_none(
                    row.get(column_map.get("new_pe_issue_a"))
                )
                import_data.indv_aim_price_t = _str_or_none(
                    row.get(column_map.get("indv_aim_price_t"))
                )
                import_data.indv_aim_price_l = _str_or_none(
                    row.get(column_map.get("indv_aim_price_l"))
                )
                import_data.market = _str_or_none(row.get(column_map.get("market")))

                entity = models_knowledge_document.KnowledgeDocument(
                    **import_data.model_dump(), doc_type=doc_type
                )

            if doc_type == constants_knowledge_base.DOC_TYPE_INDUSTRY_REPORT:
                import_data.org_S_Name = _str_or_none(
                    row.get(column_map.get("org_S_Name"))
                )

                entity = models_knowledge_document.KnowledgeDocument(
                    **import_data.model_dump(), doc_type=doc_type
                )

            entities_to_add.append(entity)
            existing_titles.add(import_data.title)

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
