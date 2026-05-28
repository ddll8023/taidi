"""知识库管理 Schema"""

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, ConfigDict


# ========== 辅助类（Support）==========


class InitErrorItem(BaseModel):
    """初始化错误项"""

    row: int = Field(description="行号")
    error: str = Field(description="错误描述")


class GetParseResultBlockItem(BaseModel):
    """解析结果内容块"""

    content_id: str = Field(..., description="内容块唯一ID")
    document_id: int = Field(..., description="文档ID")
    type: str = Field(..., description="内容类型：text/list/table")
    text: str | None = Field(None, description="text或list类型的正文内容")
    table_caption: str | None = Field(None, description="表格标题")
    table_body: str | None = Field(None, description="表格主体（Markdown格式）")
    table_footnote: str | None = Field(None, description="表格脚注")
    page_idx: int = Field(0, description="MinerU原始页码（从0开始）")
    section_path: str | None = Field(None, description="章节路径")
    order: int = Field(0, description="原始阅读顺序")
    # 保留原始字段便于调试
    raw_type: str | None = Field(None, description="MinerU原始类型")
    bbox: list[float] | None = Field(None, description="MinerU原始边界框")
    text_level: int | None = Field(None, description="标题级别")
    # 表格语义增强字段
    table_semantic_title: str | None = Field(None, description="表格语义标题")
    table_context: str | None = Field(None, description="表格用途说明")
    table_schema_summary: str | None = Field(None, description="字段结构说明")
    nearby_text: str | None = Field(None, description="表格附近正文")
    table_quality: str | None = Field(None, description="表格质量：high/medium/low")
    table_context_source: list[str] | None = Field(None, description="表格上下文来源")


class GetParseResultReportItem(BaseModel):
    """解析报告摘要"""

    total_blocks: int = Field(0, description="总内容块数")
    text_blocks: int = Field(0, description="text类型块数")
    list_blocks: int = Field(0, description="list类型块数")
    table_blocks: int = Field(0, description="table类型块数")
    dropped_image_blocks: int = Field(0, description="丢弃的图片块数")
    dropped_equation_blocks: int = Field(0, description="丢弃的公式块数")
    dropped_header_footer_blocks: int = Field(0, description="丢弃的页眉页脚块数")
    page_count: int = Field(0, description="PDF总页数")
    # 表格增强统计（新增）
    table_total_count: int = Field(0, description="表格总数")
    table_caption_missing_count: int = Field(0, description="缺少caption的表格数")
    table_semantic_title_inferred_count: int = Field(0, description="语义标题推断数")
    table_context_generated_count: int = Field(0, description="生成上下文的表格数")
    high_quality_table_count: int = Field(0, description="高质量表格数")
    medium_quality_table_count: int = Field(0, description="中质量表格数")
    low_quality_table_count: int = Field(0, description="低质量表格数")
    table_nearby_text_empty_count: int = Field(0, description="附近正文为空的表格数")


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


class ChunkStatsItem(BaseModel):
    """切块统计数据"""

    total: int = Field(0, description="切块总数")
    by_vector_status: dict[str, int] = Field(
        default_factory=dict, description="按向量状态分组统计"
    )


class ChunkDocumentItem(BaseModel):
    """单个文档切块结果"""

    document_id: int = Field(..., description="文档ID")
    title: str = Field("", description="文档标题")
    chunk_count: int = Field(0, description="切块数量")
    success: bool = Field(..., description="是否成功")
    error: str | None = Field(None, description="失败原因")

    model_config = ConfigDict(from_attributes=True)


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


class ChunkDocumentsRequest(BaseModel):
    """文档切块请求"""

    document_ids: list[int] = Field(
        default_factory=list, min_length=1, description="待切块的文档ID列表"
    )


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
    chunks: ChunkStatsItem = Field(default_factory=lambda: ChunkStatsItem(), description="切块统计")

    model_config = ConfigDict(from_attributes=True)


class ChunkDocumentsResponse(BaseModel):
    """文档切块响应"""

    total: int = Field(..., description="请求处理的文档总数")
    success_count: int = Field(..., description="成功数")
    failed_count: int = Field(..., description="失败数")
    results: list[ChunkDocumentItem] = Field(
        default_factory=list, description="逐文档切块结果"
    )

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
    page_count: int = Field(0, description="PDF 总页数")

    model_config = ConfigDict(from_attributes=True)


class SaveEnrichedBlocksRequest(BaseModel):
    """保存手工编辑的增强数据请求"""

    document_id: int = Field(..., description="文档ID")
    blocks: list[GetParseResultBlockItem] = Field(
        ..., description="手工编辑后的完整内容块列表"
    )


class SaveEnrichedBlocksResponse(BaseModel):
    """保存手工编辑的增强数据响应"""

    document_id: int = Field(..., description="文档ID")
    title: str = Field("", description="文档标题")
    block_count: int = Field(0, description="内容块总数")
    table_count: int = Field(0, description="表格数")

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
