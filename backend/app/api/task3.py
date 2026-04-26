"""任务三-增强智能问数 API 路由"""
from typing import Annotated

from fastapi import APIRouter, Depends, File, Path, Query, UploadFile
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.schemas import task3 as schemas_task3
from app.schemas.response import error, success
from app.services import task3 as services_task3
from app.utils.exception import ServiceException

router = APIRouter(prefix="/api/v1/task3", tags=["任务三-增强智能问数"])


@router.post("/question")
async def process_question(
    request: schemas_task3.Task3PlanRequest,
    db: Annotated[Session, Depends(get_db)],
):
    """处理任务三问题"""
    try:
        result = services_task3.process_task3_question(
            question=request.question,
            db=db,
            context=request.context,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/plan")
async def create_plan(
    request: schemas_task3.Task3PlanRequest,
    db: Annotated[Session, Depends(get_db)],
):
    """生成执行计划（不执行）"""
    try:
        result = services_task3.create_plan_response(
            question=request.question,
            context=request.context,
            db=db,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/execute")
async def execute_plan(
    request: schemas_task3.Task3PlanRequest,
    db: Annotated[Session, Depends(get_db)],
):
    """生成计划并执行"""
    try:
        result = services_task3.plan_and_execute(
            question=request.question,
            context=request.context,
            db=db,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/export/single")
async def export_single(
    question_id: Annotated[str, Query(description="问题编号")],
    question: Annotated[str, Query(description="问题内容")],
    db: Annotated[Session, Depends(get_db)],
):
    """导出单个问题结果（独立模式，不依赖工作台）"""
    try:
        result = services_task3.export_single_question_result(
            question_id=question_id,
            question=question,
            db=db,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/verify")
async def verify_result(
    request: schemas_task3.Task3PlanRequest,
    db: Annotated[Session, Depends(get_db)],
):
    """验证问题处理结果"""
    try:
        result = services_task3.plan_execute_and_verify(
            question=request.question,
            context=request.context,
            db=db,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


# ─────────────────────────────────────────────────────────────────────────────
# 工作台接口
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/workspace")
async def get_workspace(
    db: Annotated[Session, Depends(get_db)],
):
    """获取当前工作台概览"""
    try:
        result = services_task3.get_workspace_info(db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/workspace/import")
async def import_fujian6(
    file: Annotated[UploadFile, File(description="附件6文件")],
    db: Annotated[Session, Depends(get_db)],
):
    """上传并解析附件6"""
    try:
        result = await services_task3.import_fujian6_from_upload(db=db, file=file)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("/questions")
async def get_questions(
    db: Annotated[Session, Depends(get_db)],
    status: Annotated[int | None, Query(description="状态筛选：0待处理 1回答中 2已完成 3失败")] = None,
):
    """获取题目列表"""
    try:
        result = services_task3.get_question_list_response(db, status=status)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("/questions/{question_id}")
async def get_question_detail(
    question_id: Annotated[int, Path(description="题目ID")],
    db: Annotated[Session, Depends(get_db)],
):
    """获取单题详情"""
    try:
        result = services_task3.get_question_detail_or_raise(db=db, question_id=question_id)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/questions/{question_id}/answer")
async def answer_question(
    question_id: Annotated[int, Path(description="题目ID")],
    db: Annotated[Session, Depends(get_db)],
):
    """回答本题"""
    try:
        result = services_task3.answer_single_question(question_id=question_id, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.delete("/questions/{question_id}/answer")
async def delete_answer(
    question_id: Annotated[int, Path(description="题目ID")],
    db: Annotated[Session, Depends(get_db)],
):
    """删除当前回答"""
    try:
        result = services_task3.delete_question_answer(question_id=question_id, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/questions/{question_id}/rerun")
async def rerun_question(
    question_id: Annotated[int, Path(description="题目ID")],
    db: Annotated[Session, Depends(get_db)],
):
    """重新回答（删除旧结果后重新执行）"""
    try:
        result = services_task3.rerun_question(question_id=question_id, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/questions/batch-answer")
async def batch_answer(
    db: Annotated[Session, Depends(get_db)],
    scope: Annotated[str, Query(description="处理范围：all全部/unfinished未完成/failed失败")] = "unfinished",
):
    """批量回答题目"""
    try:
        result = services_task3.batch_answer_with_workspace_check(scope=scope, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/export")
async def export_result(
    db: Annotated[Session, Depends(get_db)],
):
    """导出 result_3.xlsx（工作台模式）"""
    try:
        result = services_task3.export_result_3_from_workspace(db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("/export/latest")
async def get_latest_export(
    db: Annotated[Session, Depends(get_db)],
):
    """获取最近一次导出结果信息"""
    try:
        result = services_task3.get_latest_export_info(db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
