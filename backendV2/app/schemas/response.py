from typing import TypeVar

from app.schemas.common import ErrorCode


T = TypeVar("T")


def success(data: T | None = None, message: str = "success"):
    return {"code": ErrorCode.SUCCESS, "message": message, "data": data}


def error(
    code: int = ErrorCode.INTERNAL_ERROR,
    message: str = "服务器错误",
    data: T | None = None,
):
    return {"code": code, "message": message, "data": data}
