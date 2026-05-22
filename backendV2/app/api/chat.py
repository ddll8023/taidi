from fastapi import (
    APIRouter,
    Depends,
    Query,
    File,
    Path,
    Body,
    UploadFile,
    BackgroundTasks,
)
from typing import Annotated
from sqlalchemy.orm import Session
from app.db.database import get_db
from app.schemas.response import success, error
from app.schemas.common import ApiResponse, ErrorCode, PaginatedResponse
from app.utils.exception import ServiceException
from app.services import chat as services_chat
from app.schemas import chat as schemas_chat


router = APIRouter(prefix="/api/v1/chat", tags=["聊天功能接口"])


@router.post(
    "",
    response_model=ApiResponse[schemas_chat.ChatResponse],
    description="聊天对话接口",
)
def start_chat(
    db: Annotated[Session, Depends(get_db)],
    background_tasks: BackgroundTasks,
    start_chat_request: Annotated[
        schemas_chat.StartChatRequest, Body(..., description="聊天请求参数")
    ],
):
    """开始聊天"""
    try:
        return success(
            services_chat.start_chat(
                db,
                background_tasks,
                start_chat_request,
            )
        )
    except ServiceException as e:
        return error(
            code=e.code,
            message=e.message,
        )
