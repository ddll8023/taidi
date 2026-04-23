import tempfile
from typing import Annotated

from fastapi import APIRouter, Depends, File, Query, UploadFile
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.schemas import task3 as schemas_task3
from app.schemas.common import ErrorCode
from app.schemas.response import error, success
from app.services.task3 import exporter as services_task3_export
from app.services.task3 import importer as services_task3_import
from app.services.task3 import planner as services_task3_planner
from app.services.task3 import runner as services_task3_runner
from app.services.task3.verifier import verify_execution_trace
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

router = APIRouter(prefix="/api/v1/task3", tags=["任务三-增强智能问数"])
logger = setup_logger(__name__)


@router.post("/question")
async def process_question(
    request: schemas_task3.Task3PlanRequest,
    db: Session = Depends(get_db),
):
    """处理任务三问题"""
    try:
        result = services_task3_planner.process_task3_question(
            question=request.question,
            db=db,
            context=request.context,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"任务三问题处理异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/plan")
async def create_plan(
    request: schemas_task3.Task3PlanRequest,
    db: Session = Depends(get_db),
):
    """生成执行计划（不执行）"""
    try:
        plan = services_task3_planner.plan_task3_question(
            question=request.question,
            context=request.context,
            db=db,
        )
        return success(
            data=schemas_task3.Task3PlanResponse(
                plan=plan,
                reasoning=f"已生成包含 {len(plan.steps)} 个步骤的执行计划",
            )
        )
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"生成执行计划异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/execute")
async def execute_plan(
    request: schemas_task3.Task3PlanRequest,
    db: Session = Depends(get_db),
):
    """生成计划并执行"""
    try:
        plan = services_task3_planner.plan_task3_question(
            question=request.question,
            context=request.context,
            db=db,
        )

        trace = services_task3_planner.execute_plan(plan, db)
        return success(
            data=schemas_task3.Task3ExecuteResponse(
                plan=plan,
                trace=trace,
            )
        )
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"执行计划异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/export/single")
async def export_single(
    question_id: str = Query(..., description="问题编号"),
    question: str = Query(..., description="问题内容"),
    db: Session = Depends(get_db),
):
    """导出单个问题结果（独立模式，不依赖工作台）"""
    try:
        result = services_task3_export.export_single_question_result(
            question_id=question_id,
            question=question,
            db=db,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"单问题导出异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="导出失败")


@router.post("/verify")
async def verify_result(
    request: schemas_task3.Task3PlanRequest,
    db: Session = Depends(get_db),
):
    """验证问题处理结果"""
    try:
        plan = services_task3_planner.plan_task3_question(
            question=request.question,
            context=request.context,
            db=db,
        )

        trace = services_task3_planner.execute_plan(plan, db)
        verification = verify_execution_trace(db, trace)
        return success(
            data=schemas_task3.Task3VerifyResponse(
                answer=trace.final_answer,
                verification=verification,
                references=trace.references,
            )
        )
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"验证异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


# ─────────────────────────────────────────────────────────────────────────────
# 工作台接口
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/workspace")
async def get_workspace(
    db: Session = Depends(get_db),
):
    """获取当前工作台概览"""
    try:
        workspace = services_task3_import.get_workspace_info(db)
        if workspace is None:
            return success(data=None, message="工作台尚未初始化")
        return success(data=schemas_task3.Task3WorkspaceResponse.model_validate(workspace))
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error("获取工作台信息异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/workspace/import")
async def import_fujian6(
    file: Annotated[UploadFile, File(description="附件6文件")],
    db: Session = Depends(get_db),
):
    """上传并解析附件6"""
    try:
        if not file.filename or not file.filename.endswith(".xlsx"):
            return error(code=ErrorCode.PARAM_ERROR, message="请上传xlsx格式的附件6文件")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            content = await file.read()
            tmp_file.write(content)
            tmp_path = tmp_file.name

        import os
        try:
            result = services_task3_import.import_fujian6(
                file_path=tmp_path,
                original_filename=file.filename,
                db=db,
            )
            return success(data=result)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error("导入附件6异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"导入失败: {str(e)}")


@router.get("/questions")
async def get_questions(
    status: int | None = Query(None, description="状态筛选：0待处理 1回答中 2已完成 3失败"),
    db: Session = Depends(get_db),
):
    """获取题目列表"""
    try:
        workspace = services_task3_import.get_workspace_info(db)
        if workspace is None:
            return success(data=schemas_task3.Task3QuestionListResponse())

        questions = services_task3_import.get_question_list(
            db=db,
            workspace_id=workspace.id,
            status=status,
        )

        stats = services_task3_import.get_question_stats(db, workspace.id)

        items = [schemas_task3.Task3QuestionItemResponse.model_validate(q) for q in questions]

        return success(
            data=schemas_task3.Task3QuestionListResponse(
                items=items,
                total=stats.total,
                pending_count=stats.pending,
                answered_count=stats.answered,
                failed_count=stats.failed,
            )
        )

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
        question = services_task3_import.get_question_detail(db=db, question_id=question_id)
        if question is None:
            return error(code=ErrorCode.DATA_NOT_FOUND, message="题目不存在")
        return success(data=schemas_task3.Task3QuestionItemResponse.model_validate(question))

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
        result = services_task3_runner.answer_single_question(question_id=question_id, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error("回答题目异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"回答失败: {str(e)}")


@router.delete("/questions/{question_id}/answer")
async def delete_answer(
    question_id: int,
    db: Session = Depends(get_db),
):
    """删除当前回答"""
    try:
        result = services_task3_runner.delete_question_answer(question_id=question_id, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error("删除回答异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="删除失败")


@router.post("/questions/{question_id}/rerun")
async def rerun_question(
    question_id: int,
    db: Session = Depends(get_db),
):
    """重新回答（删除旧结果后重新执行）"""
    try:
        result = services_task3_runner.rerun_question(question_id=question_id, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error("重新回答异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"重新回答失败: {str(e)}")


@router.post("/questions/batch-answer")
async def batch_answer(
    scope: str = Query("unfinished", description="处理范围：all全部/unfinished未完成/failed失败"),
    db: Session = Depends(get_db),
):
    """批量回答题目"""
    try:
        workspace = services_task3_import.get_workspace_info(db)
        if workspace is None:
            return error(code=ErrorCode.DATA_NOT_FOUND, message="工作台不存在，请先导入附件6")

        result = services_task3_runner.batch_answer_questions(
            workspace_id=workspace.id,
            scope=scope,
            db=db,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error("批量回答异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"批量回答失败: {str(e)}")


@router.post("/export")
async def export_result(
    db: Session = Depends(get_db),
):
    """导出 result_3.xlsx（工作台模式）"""
    try:
        result = services_task3_export.export_result_3_from_workspace(db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error("导出异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message=f"导出失败: {str(e)}")


@router.get("/export/latest")
async def get_latest_export(
    db: Session = Depends(get_db),
):
    """获取最近一次导出结果信息"""
    try:
        result = services_task3_export.get_latest_export_info(db=db)
        if result is None:
            return success(data=None, message="暂无导出记录")
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error("获取导出信息异常: %s", str(e), exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")
