"""知识库管理 Schema"""

from pydantic import BaseModel, Field, ConfigDict


# ========== 辅助类（Support）==========


class InitErrorItem(BaseModel):
    """初始化错误项"""

    row: int = Field(description="行号")
    error: str = Field(description="错误描述")


# ========== 请求类（Request）==========
# 系统初始化使用 multipart/form-data，API 层 Form 逐个声明，不需要请求类


# ========== 响应类（Response）==========


class InitKnowledgeBaseResponse(BaseModel):
    """系统初始化响应"""

    success: bool = Field(description="是否成功")
    message: str = Field(description="结果消息")
    total_count: int = Field(description="总数量", default=0)

    model_config = ConfigDict(from_attributes=True)


class InitStatusResponse(BaseModel):
    """初始化状态响应"""

    initialized: bool = Field(description="是否已初始化")
    stock_metadata_count: int = Field(description="个股研报元数据数量", default=0)
    industry_metadata_count: int = Field(description="行业研报元数据数量", default=0)
    total_metadata_count: int = Field(description="总元数据数量", default=0)

    model_config = ConfigDict(from_attributes=True)
