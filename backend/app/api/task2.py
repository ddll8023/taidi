import os
import tempfile
from typing import Annotated

from fastapi import APIRouter, Depends, File, Query, UploadFile
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.schemas.common import ErrorCode
from app.schemas.response import error, success
from app.schemas.task2 import (
    Task2ImportResponse,
    Task2QuestionItemResponse,
    Task2QuestionListResponse,
    Task2WorkspaceResponse,
)
from app.services import task2_import
from app.services import task2_runner
from app.services import task2_export
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

router = APIRouter(prefix="/api/v1/task2", tags=["任务二工作台"])
logger = setup_logger(__name__)


@router.get("/workspace")
async def get_workspace(
    db: Session = Depends(get_db),
):
    """获取当前工作台概览"""
    try:
        workspace = task2_import.get_workspace_info(db)
        if workspace is None:
            return success(data=None, message="工作台尚未初始化")
        return success(Task2WorkspaceResponse.model_validate(workspace))
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error("获取工作台信息异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/workspace/import")
async def import_fujian4(
    file: Annotated[UploadFile, File(description="附件4文件")],
    db: Session = Depends(get_db),
):
    """上传并解析附件4"""
    try:
        if not file.filename or not file.filename.endswith(".xlsx"):
            return error(code=ErrorCode.PARAM_ERROR, message="请上传xlsx格式的附件4文件")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            content = await file.read()
            tmp_file.write(content)
            tmp_path = tmp_file.name

        try:
            result = task2_import.import_fujian4(
                file_path=tmp_path,
                original_filename=file.filename,
                db=db,
            )
            return success(Task2ImportResponse(**result))
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error("导入附件4异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"导入失败: {str(e)}")


@router.get("/questions")
async def get_questions(
    status: int | None = Query(None, description="状态筛选：0待处理 1回答中 2已完成 3失败"),
    db: Session = Depends(get_db),
):
    """获取题目列表"""
    try:
        workspace = task2_import.get_workspace_info(db)
        if workspace is None:
            return success(Task2QuestionListResponse())

        questions = task2_import.get_question_list(
            db=db,
            workspace_id=workspace.id,
            status=status,
        )

        stats = task2_import.get_question_stats(db, workspace.id)

        items = [Task2QuestionItemResponse.model_validate(q) for q in questions]

        return success(Task2QuestionListResponse(
            items=items,
            total=stats["total"],
            pending_count=stats["pending"],
            answered_count=stats["answered"],
            failed_count=stats["failed"],
        ))

    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error("获取题目列表异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.get("/questions/{question_id}")
async def get_question_detail(
    question_id: int,
    db: Session = Depends(get_db),
):
    """获取单题详情"""
    try:
        from sqlalchemy import select
        from app.models.task2_question_item import Task2QuestionItem

        question = db.get(Task2QuestionItem, question_id)
        if question is None:
            return error(code=ErrorCode.DATA_NOT_FOUND, message="题目不存在")

        return success(Task2QuestionItemResponse.model_validate(question))

    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error("获取题目详情异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/questions/{question_id}/answer")
async def answer_question(
    question_id: int,
    db: Session = Depends(get_db),
):
    """回答本题"""
    try:
        result = task2_runner.answer_single_question(question_id=question_id, db=db)
        db.commit()
        return success(result)
    except ValueError as e:
        db.rollback()
        return error(code=ErrorCode.PARAM_ERROR, message=str(e))
    except ServiceException as e:
        db.rollback()
        return error(code=e.code, message=e.message)
    except Exception as e:
        db.rollback()
        logger.error("回答题目异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"回答失败: {str(e)}")


@router.delete("/questions/{question_id}/answer")
async def delete_answer(
    question_id: int,
    db: Session = Depends(get_db),
):
    """删除当前回答"""
    try:
        result = task2_runner.delete_question_answer(question_id=question_id, db=db)
        db.commit()
        return success(result)
    except ValueError as e:
        db.rollback()
        return error(code=ErrorCode.PARAM_ERROR, message=str(e))
    except ServiceException as e:
        db.rollback()
        return error(code=e.code, message=e.message)
    except Exception as e:
        db.rollback()
        logger.error("删除回答异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"删除失败: {str(e)}")


@router.post("/questions/{question_id}/rerun")
async def rerun_question(
    question_id: int,
    db: Session = Depends(get_db),
):
    """重新回答（删除旧结果后重新执行）"""
    try:
        result = task2_runner.rerun_question(question_id=question_id, db=db)
        db.commit()
        return success(result)
    except ValueError as e:
        db.rollback()
        return error(code=ErrorCode.PARAM_ERROR, message=str(e))
    except ServiceException as e:
        db.rollback()
        return error(code=e.code, message=e.message)
    except Exception as e:
        db.rollback()
        logger.error("重新回答异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"重新回答失败: {str(e)}")


@router.post("/questions/batch-answer")
async def batch_answer(
    scope: str = Query("unfinished", description="处理范围：all全部/unfinished未完成/failed失败"),
    db: Session = Depends(get_db),
):
    """批量回答题目"""
    try:
        workspace = task2_import.get_workspace_info(db)
        if workspace is None:
            return error(code=ErrorCode.DATA_NOT_FOUND, message="工作台不存在，请先导入附件4")

        result = task2_runner.batch_answer_questions(
            workspace_id=workspace.id,
            scope=scope,
            db=db,
        )
        db.commit()
        return success(result)
    except ValueError as e:
        db.rollback()
        return error(code=ErrorCode.PARAM_ERROR, message=str(e))
    except ServiceException as e:
        db.rollback()
        return error(code=e.code, message=e.message)
    except Exception as e:
        db.rollback()
        logger.error("批量回答异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"批量回答失败: {str(e)}")


@router.post("/export")
async def export_result(
    db: Session = Depends(get_db),
):
    """导出result_2.xlsx"""
    try:
        result = task2_export.export_result_2(db=db)
        db.commit()
        return success(result)
    except ValueError as e:
        db.rollback()
        return error(code=ErrorCode.PARAM_ERROR, message=str(e))
    except ServiceException as e:
        db.rollback()
        return error(code=e.code, message=e.message)
    except Exception as e:
        db.rollback()
        logger.error("导出异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"导出失败: {str(e)}")


@router.get("/export/latest")
async def get_latest_export(
    db: Session = Depends(get_db),
):
    """获取最近一次导出结果信息"""
    try:
        result = task2_export.get_latest_export_info(db=db)
        if result is None:
            return success(data=None, message="暂无导出记录")
        return success(result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error("获取导出信息异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")
