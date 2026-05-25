"""知识库管理 Schema"""

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, ConfigDict


# ========== 辅助类（Support）==========


class InitErrorItem(BaseModel):
    """初始化错误项"""

    row: int = Field(description="行号")
    error: str = Field(description="错误描述")


class DocumentStatsData(BaseModel):
    """文档统计数据"""

    total: int = Field(default=0, description="文档总数")
    by_chunk_status: dict[str, int] = Field(
        default_factory=dict, description="按切块状态分组统计"
    )
    by_vector_status: dict[str, int] = Field(
        default_factory=dict, description="按向量状态分组统计"
    )
    by_doc_type: dict[str, int] = Field(
        default_factory=dict, description="按文档类型分组统计"
    )


class ChunkStatsData(BaseModel):
    """切块统计数据"""

    total: int = Field(0, description="切块总数")
    by_vector_status: dict[str, int] = Field(
        default_factory=dict, description="按向量状态分组统计"
    )


# ========== 请求类（Request）==========
# 系统初始化使用 multipart/form-data，API 层 Form 逐个声明，不需要请求类


class KnowledgeDocumentListRequest(BaseModel):
    """知识库文档列表请求"""

    page: int = Field(1, ge=1, description="页码")
    page_size: int = Field(10, ge=10, description="每页数量")
    keyword: str | None = Field(None, description="标题关键词搜索")
    doc_type: str | None = Field(None, description="文档类型筛选")
    stock_code: str | None = Field(None, description="股票代码筛选")
    chunk_status: Literal[0, 1, 2, 3] | None = Field(
        None, description="切块状态筛选：0待切块/1切块中/2完成/3失败"
    )
    vector_status: Literal[0, 1, 2, 3] | None = Field(
        None, description="向量状态筛选：0未向量化/1向量化中/2已向量化/3失败"
    )
    sort_by: Literal["created_at", "updated_at"] | None = Field(
        "updated_at", description="排序字段"
    )
    sort_order: Literal["desc", "asc"] | None = Field("desc", description="排序方式")


# ========== 响应类（Response）==========


class KnowledgeDocumentListItemResponse(BaseModel):
    """知识库文档列表项响应"""

    id: int = Field(..., description="文档ID")
    doc_type: str = Field(..., description="文档类型")
    title: str = Field(..., description="文档标题")
    stock_code: str | None = Field(None, description="股票代码")
    stock_abbr: str | None = Field(None, description="股票简称")
    publish_date: date | None = Field(None, description="发布日期")
    chunk_status: int = Field(..., description="切块状态")
    vector_status: int = Field(..., description="向量状态")
    metadata_status: int = Field(..., description="元数据状态")

    model_config = ConfigDict(from_attributes=True)


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


class KnowledgeBaseStatsResponse(BaseModel):
    """知识库统计响应"""

    documents: DocumentStatsData = Field(..., description="文档统计")
    chunks: ChunkStatsData = Field(..., description="切块统计")

    model_config = ConfigDict(from_attributes=True)
