from enum import IntEnum
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field


T = TypeVar("T")


class ApiResponse(BaseModel, Generic[T]):
    code: int = 0
    message: str = "success"
    data: T | None = None

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={"example": {"code": 0, "message": "success", "data": {}}},
    )


class PaginationInfo(BaseModel):
    page: int = 1
    page_size: int = 10
    total: int = 0
    total_pages: int = 0


class PaginatedResponse(BaseModel, Generic[T]):
    lists: list[T] = Field(default_factory=list)
    pagination: PaginationInfo = PaginationInfo()

    model_config = ConfigDict(from_attributes=True)


class ErrorCode(IntEnum):
    """错误码"""

    # 成功
    SUCCESS = 0

    # 参数错误
    PARAM_ERROR = 1001
    # 数据不存在
    DATA_NOT_FOUND = 1002
    # 未登录
    NOT_LOGGED_IN = 2001
    # 令牌过期
    TOKEN_EXPIRED = 2002
    # 权限不足
    PERMISSION_DENIED = 2003
    # 令牌无效
    INVALID_TOKEN_ERROR = 2004
    # 文件格式不支持
    UNSUPPORTED_FILE_FORMAT = 3001
    # 文件大小超过限制
    FILE_TOO_LARGE = 3002

    # 调用AI服务错误
    AI_SERVICE_ERROR = 4001

    # 服务器内部错误
    INTERNAL_ERROR = 5001

    # 密码错误
    PASSWORD_ERROR = 6001
