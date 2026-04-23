import os
from fastapi import APIRouter, Depends, Query
from fastapi.responses import FileResponse
from typing import Annotated
from fastapi import Path
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.schemas import chat as schemas_chat
from app.schemas.common import ErrorCode
from app.schemas.response import error, success
from app.services import chat as services_chat
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

router = APIRouter(prefix="/api/v1/chat", tags=["智能问数"])
logger = setup_logger(__name__)

CHART_DIR = os.path.join(os.getcwd(), "result")


@router.post("")
async def chat(
    request: schemas_chat.ChatRequest,
    db: Session = Depends(get_db),
):
    """发送对话消息"""
    try:
        result = services_chat.process_chat_message(
            session_id=request.session_id,
            question=request.question,
            db=db,
        )
        return success(result.model_dump())
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"对话异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.get("/sessions")
async def get_sessions(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """获取会话列表"""
    try:
        result = services_chat.get_chat_sessions(db=db, page=page, page_size=page_size)
        return success(result.model_dump())
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"获取会话列表异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.get("/history/{session_id}")
async def get_history(
    session_id: str,
    db: Session = Depends(get_db),
):
    """获取会话历史"""
    try:
        result = services_chat.get_chat_history(session_id=session_id, db=db)
        return success([r.model_dump() for r in result])
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"获取会话历史异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.post("/export")
async def export_result(
    request: schemas_chat.ChatExportRequest,
    db: Session = Depends(get_db),
):
    """导出result_2.xlsx"""
    try:
        result_path = services_chat.export_result_2(questions=request.questions, db=db)
        return success({"file_path": result_path})
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"导出异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="导出失败")


@router.put("/sessions/{session_id}/close")
async def close_session(
    session_id: Annotated[str, Path(description="会话ID")],
    db: Session = Depends(get_db),
):
    """关闭会话"""
    try:
        services_chat.close_chat_session(session_id=session_id, db=db)
        return success({"session_id": session_id, "message": "会话已关闭"})
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"关闭会话异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.delete("/sessions/{session_id}")
async def delete_session(
    session_id: Annotated[str, Path(description="会话ID")],
    db: Session = Depends(get_db),
):
    """删除会话及其消息"""
    try:
        services_chat.delete_chat_session(session_id=session_id, db=db)
        return success({"session_id": session_id, "message": "会话已删除"})
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"删除会话异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.put("/sessions/{session_id}/rename")
async def rename_session(
    session_id: Annotated[str, Path(description="会话ID")],
    request: schemas_chat.ChatRenameRequest,
    db: Session = Depends(get_db),
):
    """重命名会话"""
    try:
        services_chat.rename_chat_session(session_id=session_id, name=request.name, db=db)
        return success({"session_id": session_id, "name": request.name, "message": "会话已重命名"})
    except ServiceException as e:
        return error(code=e.code, message=e.message)
    except Exception as e:
        logger.error(f"重命名会话异常：{e}", exc_info=True)
        return error(code=ErrorCode.INTERNAL_ERROR, message="系统内部错误")


@router.get("/images/{filename}")
async def get_chart_image(
    filename: Annotated[str, Path(description="图片文件名")],
):
    """获取图表图片"""
    file_path = os.path.join(CHART_DIR, filename)
    if not os.path.exists(file_path):
        return error(code=ErrorCode.DATA_NOT_FOUND, message="图片不存在")
    return FileResponse(
        path=file_path,
        media_type="image/jpeg",
        filename=filename,
    )
