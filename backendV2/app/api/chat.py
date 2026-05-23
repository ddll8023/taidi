from fastapi import (
    APIRouter,
    Depends,
    Body,
    BackgroundTasks,
)
from fastapi.responses import StreamingResponse
from typing import Annotated
from sqlalchemy.orm import Session
from app.utils.exception import ServiceException
from app.schemas.response import success, error
from app.schemas.common import ApiResponse, PaginatedResponse
from app.db.database import get_db
from app.services import chat as services_chat
from app.schemas import chat as schemas_chat


router = APIRouter(prefix="/api/v1/chat", tags=["聊天功能接口"])


@router.post(
    "",
    description="聊天对话接口（SSE 流式推送进度）",
)
def start_chat(
    db: Annotated[Session, Depends(get_db)],
    background_tasks: BackgroundTasks,
    start_chat_request: Annotated[
        schemas_chat.StartChatRequest, Body(..., description="聊天请求参数")
    ],
):
    """开始聊天（SSE 流式推送：意图识别 → SQL生成 → 数据查询 → 回答生成）

    SSE 流式接口返回 StreamingResponse（原始 HTTP 响应），不走 FastAPI 序列化管道，
    因此不设 response_model（区别于返回 ApiResponse 的常规 JSON 接口）。
    """
    try:
        return StreamingResponse(
            services_chat.start_chat(db, background_tasks, start_chat_request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
    except ServiceException as e:
        return error(code=e.code, message=e.message)


@router.post(
    "/list",
    response_model=ApiResponse[PaginatedResponse],
    description="获取聊天列表",
)
def get_chat_list(
    db: Annotated[Session, Depends(get_db)],
    get_chat_list_request: Annotated[
        schemas_chat.GetChatListRequest,
        Body(..., description="获取聊天列表请求"),
    ],
):
    """获取聊天列表"""
    try:
        return success(services_chat.get_chat_list(db, get_chat_list_request))
    except ServiceException as e:
        return error(code=e.code, message=e.message)
