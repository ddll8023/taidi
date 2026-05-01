"""智能问数 API 路由"""
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Path
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.schemas import chat as schemas_chat
from app.schemas.common import ApiResponse
from app.schemas.response import error, success
from app.services import chat as services_chat
from app.utils.exception import ServiceException

router = APIRouter(prefix="/api/v1/chat", tags=["智能问数"])


@router.post("", response_model=ApiResponse)
async def chat(
    request: schemas_chat.ChatRequest,
    db: Annotated[Session, Depends(get_db)],
):
    """发送对话消息"""
    try:
        result = services_chat.process_chat_message(
            session_id=request.session_id,
            question=request.question,
            db=db,
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("/sessions", response_model=ApiResponse)
async def get_sessions(
    db: Annotated[Session, Depends(get_db)],
    page: Annotated[int, Query(ge=1, description="页码")] = 1,
    page_size: Annotated[int, Query(ge=1, le=100, description="每页数量")] = 10,
):
    """获取会话列表"""
    try:
        result = services_chat.get_chat_sessions(db=db, page=page, page_size=page_size)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("/history/{session_id}", response_model=ApiResponse)
async def get_history(
    session_id: Annotated[str, Path(description="会话ID")],
    db: Annotated[Session, Depends(get_db)],
):
    """获取会话历史"""
    try:
        result = services_chat.get_chat_history(session_id=session_id, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post("/export", response_model=ApiResponse)
async def export_result(
    request: schemas_chat.ChatExportRequest,
    db: Annotated[Session, Depends(get_db)],
):
    """导出result_2.xlsx"""
    try:
        result = services_chat.export_chat_results(questions=request.questions, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.put("/sessions/{session_id}/close", response_model=ApiResponse)
async def close_session(
    session_id: Annotated[str, Path(description="会话ID")],
    db: Annotated[Session, Depends(get_db)],
):
    """关闭会话"""
    try:
        result = services_chat.close_chat_session(session_id=session_id, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.delete("/sessions/{session_id}", response_model=ApiResponse)
async def delete_session(
    session_id: Annotated[str, Path(description="会话ID")],
    db: Annotated[Session, Depends(get_db)],
):
    """删除会话及其消息"""
    try:
        result = services_chat.delete_chat_session(session_id=session_id, db=db)
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.put("/sessions/{session_id}/rename", response_model=ApiResponse)
async def rename_session(
    session_id: Annotated[str, Path(description="会话ID")],
    request: schemas_chat.ChatRenameRequest,
    db: Annotated[Session, Depends(get_db)],
):
    """重命名会话"""
    try:
        result = services_chat.rename_chat_session(
            session_id=session_id, name=request.name, db=db
        )
        return success(data=result)
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.get("/images/{filename}")
async def get_chart_image(
    filename: Annotated[str, Path(description="图片文件名")],
):
    """获取图表图片"""
    try:
        file_path = services_chat.get_chart_image_path(filename)
        return FileResponse(
            path=file_path,
            media_type="image/jpeg",
            filename=filename,
        )
    except ServiceException as e:
        return error(code=e.code, message=e.message)
