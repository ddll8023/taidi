"""知识库管理服务"""

import hashlib
import os
import tempfile

import pandas as pd
from fastapi import UploadFile
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.constants import knowledge_base as constants_knowledge_base
from app.db.database import commit_or_rollback
from app.models import knowledge_document as models_knowledge_document
from app.models import knowledge_chunk as models_knowledge_chunk
from app.schemas import knowledge_base as schemas_knowledge_base
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException
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
        success_count, errors = _import_excel(db, file, doc_type, column_map)
    except Exception as e:
        logger.error(
            f"导入Excel异常: file={file.filename} doc_type={doc_type} error={e}",
            exc_info=True,
        )
        raise ServiceException from e

    logger.info(
        f"初始化请求处理完成: file={file.filename} doc_type={doc_type} success={success_count} errors={len(errors)}"
    )
    return schemas_knowledge_base.InitKnowledgeBaseResponse(
        success=True,
        message=(
            "导入成功" if len(errors) == 0 else f"导入完成，{len(errors)}条记录失败"
        ),
        total_count=success_count,
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
    return schemas_knowledge_base.InitStatusResponse(
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
    return schemas_knowledge_base.KnowledgeBaseStatsResponse(
        documents=schemas_knowledge_base.DocumentStatsData(
            total=doc_total,
            by_chunk_status=doc_by_chunk_status,
            by_vector_status=doc_by_vector_status,
            by_doc_type=doc_by_type,
        ),
        chunks=schemas_knowledge_base.ChunkStatsData(
            total=chunk_total,
            by_vector_status=chunk_by_vector_status,
        ),
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

    entities_to_add: list[models_knowledge_document.KnowledgeDocument] = []

    logger.info(f"开始逐行处理: file={filename} doc_type={doc_type}")
    for row_idx, row in enumerate(data):
        try:
            data = {}
            # 公共字段
            data["title"] = str(row.get(column_map.get("title"), "")).strip()
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
        f"Excel整体处理完成: file={filename} success={success_count} failed={len(errors)}"
    )
    return success_count, errors
