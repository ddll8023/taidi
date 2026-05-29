"""知识库管理 Schema"""

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, ConfigDict


# ========== 辅助类（Support）==========


class InitErrorItem(BaseModel):
    """初始化错误项"""

    row: int = Field(description="行号")
    error: str = Field(description="错误描述")


class KnowledgeDocumentImportItem(BaseModel):
    """Excel 行数据导入中间结构（用于 _import_excel 构造 ORM 实体）"""

    # 基础字段
    metadata_status: int = Field(default=1, description="元数据状态")
    title: str | None = Field(None, description="文档标题")
    org_code: str | None = Field(None, description="券商编码")
    org_name: str | None = Field(None, description="券商全称")
    publish_date: date | None = Field(None, description="发布日期")
    researcher: str | None = Field(None, description="研究员姓名")
    industry_name: str | None = Field(None, description="行���名称")
    em_rating_name: str | None = Field(None, description="当前评级名称")
    last_em_rating_name: str | None = Field(None, description="上次评级名称")
    s_rating_name: str | None = Field(None, description="国际评级名称")
    s_rating_code: str | None = Field(None, description="国际评级编码")

    # 个股研报字段
    stock_code: str | None = Field(None, description="股票代码")
    stock_abbr: str | None = Field(None, description="股票简称")
    predict_next_two_year_eps: str | None = Field(None, description="预测未来两年EPS")
    predict_next_two_year_pe: str | None = Field(None, description="预测未来两年PE")
    predict_next_year_eps: str | None = Field(None, description="预测下一年EPS")
    predict_next_year_pe: str | None = Field(None, description="预测下一年PE")
    predict_this_year_eps: str | None = Field(None, description="预测本年度EPS")
    predict_this_year_pe: str | None = Field(None, description="预测本年度PE")
    predict_last_year_eps: str | None = Field(None, description="预测上一年EPS")
    predict_last_year_pe: str | None = Field(None, description="预测上一年PE")
    indv_is_new: str | None = Field(None, description="是否为新标的")
    new_listing_date: str | None = Field(None, description="上市日期")
    new_purchase_date: str | None = Field(None, description="申购日期")
    new_issue_price: str | None = Field(None, description="发行价格")
    new_pe_issue_a: str | None = Field(None, description="发行市盈率")
    indv_aim_price_t: str | None = Field(None, description="目标价上限")
    indv_aim_price_l: str | None = Field(None, description="目标价下限")
    market: str | None = Field(None, description="交易所(市场)")

    # 行业研报字段
    org_S_Name: str | None = Field(None, description="券商简称(行业研报)")


class DocumentStatsItem(BaseModel):
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
    by_parse_status: dict[str, int] = Field(
        default_factory=dict, description="按解析状态分组统计"
    )


class UploadDocumentItem(BaseModel):
    """上传成功的文档项"""

    document_id: int = Field(..., description="文档ID")
    title: str = Field(..., description="文档标题")
    file_name: str = Field(..., description="原始文件名")

    model_config = ConfigDict(from_attributes=True)


class UploadFailedFileItem(BaseModel):
    """上传失败的文件项"""

    file_name: str = Field(..., description="文件名")
    error: str = Field(..., description="错误描述")

    model_config = ConfigDict(from_attributes=True)


class ParseDocumentsItem(BaseModel):
    """单个文档解析结果"""

    document_id: int = Field(..., description="文档ID")
    title: str = Field("", description="文档标题")
    success: bool = Field(..., description="是否成功")
    error: str | None = Field(None, description="失败原因")
    block_count: int = Field(0, description="标准化后的内容块数")

    model_config = ConfigDict(from_attributes=True)


# ========== 请求类（Request）==========
# 系统初始化使用 multipart/form-data，API 层 Form 逐个声明，不需要请求类


class ParseDocumentsRequest(BaseModel):
    """文档解析请求"""

    document_ids: list[int] = Field(
        ..., min_length=1, description="待解析的文档ID列表"
    )


class GetParseResultRequest(BaseModel):
    """获取解析结果请求"""

    document_id: int = Field(..., description="文档ID")


class GetKnowledgeDocumentListRequest(BaseModel):
    """知识库文档列表请求"""

    page: int = Field(1, ge=1, description="页码")
    page_size: int = Field(10, ge=10, description="每页数量")
    keyword: str | None = Field(None, description="标题关键词搜索")
    doc_type: str | None = Field(None, description="文档类型筛选")
    stock_code: str | None = Field(None, description="股票代码筛选")
    chunk_status: Literal[0, 1, 2, 3] | None = Field(
        None, description="切块状态筛选：0待切块/1切块中/2完成/3失败"
    )
    parse_status: Literal[0, 1, 2, 3] | None = Field(
        None, description="解析状态筛选：0未解析/1解析中/2完成/3失败"
    )
    clean_status: Literal[0, 1] | None = Field(
        None, description="清洗状态筛选：0未清洗/1已清洗"
    )
    vector_status: Literal[0, 1, 2, 3] | None = Field(
        None, description="向量状态筛选：0未向量化/1向量化中/2已向量化/3失败"
    )
    sort_by: Literal["created_at", "updated_at"] | None = Field(
        "updated_at", description="排序字段"
    )
    sort_order: Literal["desc", "asc"] | None = Field("desc", description="排序方式")


# ========== 响应类（Response）==========


class GetKnowledgeDocumentListResponse(BaseModel):
    """知识库文档列表项响应"""

    id: int = Field(..., description="文档ID")
    doc_type: str = Field(..., description="文档类型")
    title: str = Field(..., description="文档标题")
    stock_code: str | None = Field(None, description="股票代码")
    stock_abbr: str | None = Field(None, description="股票简称")
    publish_date: date | None = Field(None, description="发布日期")
    chunk_status: int = Field(..., description="切块状态")
    vector_status: int = Field(..., description="向量状态")
    parse_status: int = Field(0, description="解析状态")
    parse_error_message: str | None = Field(None, description="解析失败原因")
    clean_status: int = Field(0, description="清洗状态：0未清洗/1已清洗")
    metadata_status: int = Field(..., description="元数据状态")
    metadata_status: int = Field(..., description="元数据状态")

    model_config = ConfigDict(from_attributes=True)


class InitKnowledgeBaseResponse(BaseModel):
    """系统初始化响应"""

    success: bool = Field(description="是否成功")
    message: str = Field(description="结果消息")
    total_count: int = Field(description="总数量", default=0)
    duplicate_count: int = Field(description="重复数量", default=0)

    model_config = ConfigDict(from_attributes=True)


class GetInitStatusResponse(BaseModel):
    """初始化状态响应"""

    initialized: bool = Field(description="是否已初始化")
    stock_metadata_count: int = Field(description="个股研报元数据数量", default=0)
    industry_metadata_count: int = Field(description="行业研报元数据数量", default=0)
    total_metadata_count: int = Field(description="总元数据数量", default=0)

    model_config = ConfigDict(from_attributes=True)


class GetKnowledgeBaseStatsResponse(BaseModel):
    """知识库统计响应"""

    documents: DocumentStatsItem = Field(..., description="文档统计")

    model_config = ConfigDict(from_attributes=True)


class ParseDocumentsResponse(BaseModel):
    """批量解析响应"""

    total: int = Field(..., description="请求处理的文档总数")
    success_count: int = Field(0, description="成功数")
    failed_count: int = Field(0, description="失败数")
    results: list[ParseDocumentsItem] = Field(
        default_factory=list, description="逐文档解析结果"
    )

    model_config = ConfigDict(from_attributes=True)


class GetParseResultResponse(BaseModel):
    """解析结果详情响应（返回原始 Markdown）"""

    document_id: int = Field(..., description="文档ID")
    title: str = Field(..., description="文档标题")
    markdown_content: str = Field("", description="原始 Markdown 内容")

    model_config = ConfigDict(from_attributes=True)


class UploadKnowledgeDocumentResponse(BaseModel):
    """批量上传知识库文档响应"""

    total: int = Field(..., description="上传文件总数")
    success_count: int = Field(..., description="成功数")
    failed_count: int = Field(..., description="失败数")
    success_documents: list[UploadDocumentItem] = Field(
        default_factory=list, description="成功上传的文档列表"
    )
    failed_files: list[UploadFailedFileItem] = Field(
        default_factory=list, description="失败文件列表"
    )

    model_config = ConfigDict(from_attributes=True)


class SaveParseResultRequest(BaseModel):
    """保存解析结果（清洗Markdown）请求"""

    document_id: int = Field(..., description="文档ID")
    markdown_content: str = Field(..., description="清洗后的 Markdown 内容")


class SaveParseResultResponse(BaseModel):
    """保存解析结果响应"""

    document_id: int = Field(..., description="文档ID")
    title: str = Field("", description="文档标题")
    saved: bool = Field(True, description="是否保存成功")
    clean_status: int = Field(0, description="清洗后状态")


class ToggleCleanStatusRequest(BaseModel):
    """切换清洗标记请求"""

    document_id: int = Field(..., description="文档ID")


class ToggleCleanStatusResponse(BaseModel):
    """切换清洗标记响应"""

    document_id: int = Field(..., description="文档ID")
    title: str = Field("", description="文档标题")
    clean_status: int = Field(0, description="切换后的清洗状态")


class ChunkDocumentsRequest(BaseModel):
    """文档切块请求"""

    document_ids: list[int] = Field(
        ..., min_length=1, description="待切块的文档ID列表"
    )


class ChunkDocumentsItem(BaseModel):
    """单个文档切块结果"""

    document_id: int = Field(..., description="文档ID")
    title: str = Field("", description="文档标题")
    success: bool = Field(..., description="是否成功")
    chunk_count: int = Field(0, description="切块数量")
    error: str | None = Field(None, description="失败原因")


class ChunkDocumentsResponse(BaseModel):
    """批量切块响应"""

    total: int = Field(..., description="请求处理的文档总数")
    success_count: int = Field(0, description="成功数")
    failed_count: int = Field(0, description="失败数")
    results: list[ChunkDocumentsItem] = Field(
        default_factory=list, description="逐文档切块结果"
    )
