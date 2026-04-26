"""任务二工作台 API 路由"""
from typing import Annotated

from fastapi import APIRouter, Depends, File, Path, Query, UploadFile
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.schemas.response import error, success
from app.services import task2 as services_task2
from app.utils.exception import ServiceException

router = APIRouter(prefix="/api/v1/task2", tags=["任务二工作台"])


@router.get("/workspace", response_model=dict)
async def get_workspace(
    db: Annotated[Session, Depends(get_db)],
):
    """获取当前工作台概览"""
    try:
        result = services_task2.get_workspace_info(db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/workspace/import", response_model=dict)
async def import_fujian4(
    file: Annotated[UploadFile, File(description="附件4文件")],
    db: Annotated[Session, Depends(get_db)],
):
    """上传并解析附件4"""
    try:
        result = await services_task2.import_fujian4_from_upload(db=db, file=file)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("/questions", response_model=dict)
async def get_questions(
    db: Annotated[Session, Depends(get_db)],
    status: Annotated[int | None, Query(description="状态筛选：0待处理 1回答中 2已完成 3失败")] = None,
):
    """获取题目列表"""
    try:
        result = services_task2.get_question_list_response(db, status=status)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("/questions/{question_id}", response_model=dict)
async def get_question_detail(
    question_id: Annotated[int, Path(description="题目ID")],
    db: Annotated[Session, Depends(get_db)],
):
    """获取单题详情"""
    try:
        result = services_task2.get_question_detail(question_id=question_id, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/questions/{question_id}/answer", response_model=dict)
async def answer_question(
    question_id: Annotated[int, Path(description="题目ID")],
    db: Annotated[Session, Depends(get_db)],
):
    """回答本题"""
    try:
        result = services_task2.answer_single_question(question_id=question_id, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.delete("/questions/{question_id}/answer", response_model=dict)
async def delete_answer(
    question_id: Annotated[int, Path(description="题目ID")],
    db: Annotated[Session, Depends(get_db)],
):
    """删除当前回答"""
    try:
        result = services_task2.delete_question_answer(question_id=question_id, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/questions/{question_id}/rerun", response_model=dict)
async def rerun_question(
    question_id: Annotated[int, Path(description="题目ID")],
    db: Annotated[Session, Depends(get_db)],
):
    """重新回答（删除旧结果后重新执行）"""
    try:
        result = services_task2.rerun_question(question_id=question_id, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/questions/batch-answer", response_model=dict)
async def batch_answer(
    db: Annotated[Session, Depends(get_db)],
    scope: Annotated[str, Query(description="处理范围：all全部/unfinished未完成/failed失败")] = "unfinished",
):
    """批量回答题目"""
    try:
        result = services_task2.batch_answer_with_workspace_check(scope=scope, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/export", response_model=dict)
async def export_result(
    db: Annotated[Session, Depends(get_db)],
):
    """导出result_2.xlsx"""
    try:
        result = services_task2.export_result_2(db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("/export/latest", response_model=dict)
async def get_latest_export(
    db: Annotated[Session, Depends(get_db)],
):
    """获取最近一次导出结果信息"""
    try:
        result = services_task2.get_latest_export_info(db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)
