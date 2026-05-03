from datetime import date, datetime
from enum import IntEnum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


# ========== 辅助类（Support）==========

class DocType:
    RESEARCH_REPORT = "RESEARCH_REPORT"
    FINANCIAL_REPORT = "FINANCIAL_REPORT"
    INDUSTRY_REPORT = "INDUSTRY_REPORT"


class ChunkStatus(IntEnum):
    PENDING = 0
    PROCESSING = 1
    COMPLETED = 2
    FAILED = 3


class DocumentVectorStatus(IntEnum):
    PENDING = 0
    PROCESSING = 1
    SUCCESS = 2
    FAILED = 3
    SKIPPED = 4


class ChunkVectorStatus(IntEnum):
    PENDING = 0
    PROCESSING = 1
    COMPLETED = 2
    FAILED = 3


class MetadataStatus(IntEnum):
    NOT_LOADED = 0
    LOADED = 1
    PDF_UPLOADED = 2


class FailedDocumentItem(BaseModel):
    pdf_name: str
    reason: str = ""
    suggestion: str = ""


class RecentProcessedItem(BaseModel):
    id: int
    title: str
    doc_type: str
    status: str = ""
    updated_at: Optional[datetime] = None


class DocumentStatsItem(BaseModel):
    total: int = 0
    by_chunk_status: dict = Field(default_factory=dict)
    by_vector_status: dict = Field(default_factory=dict)
    by_doc_type: dict = Field(default_factory=dict)


class ChunkStatsItem(BaseModel):
    total: int = 0
    by_vector_status: dict = Field(default_factory=dict)


# ========== 请求类（Request）==========

class KnowledgeDocumentItem(BaseModel):
    id: int
    doc_type: str
    title: str
    source_path: str
    stock_code: Optional[str] = None
    stock_abbr: Optional[str] = None
    publish_date: Optional[date] = None
    org_name: Optional[str] = None
    industry_name: Optional[str] = None
    researcher: Optional[str] = None
    em_rating_name: Optional[str] = None
    predict_this_year_eps: Optional[str] = None
    predict_this_year_pe: Optional[str] = None
    financial_report_id: Optional[int] = None
    page_count: Optional[int] = None
    chunk_count: int = 0
    chunk_status: int = 0
    metadata_status: int = 0
    error_message: Optional[str] = None
    vector_status: int = 0
    vector_model: Optional[str] = None
    vector_dim: Optional[int] = None
    vector_version: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class KnowledgeDocumentCreate(BaseModel):
    doc_type: str = Field(..., description="文档类型：RESEARCH_REPORT/FINANCIAL_REPORT/INDUSTRY_REPORT")
    title: str = Field(..., min_length=1, max_length=500, description="文档标题")
    source_path: str = Field(..., min_length=1, max_length=500, description="PDF源文件路径")
    stock_code: Optional[str] = Field(None, max_length=6, description="股票代码")
    stock_abbr: Optional[str] = Field(None, max_length=50, description="股票简称")
    publish_date: Optional[str] = Field(None, description="发布日期，格式YYYY-MM-DD")
    org_name: Optional[str] = Field(None, max_length=255, description="研究机构名称")
    industry_name: Optional[str] = Field(None, max_length=255, description="行业名称")
    financial_report_id: Optional[int] = Field(None, description="关联财报主表ID")
    page_count: Optional[int] = Field(None, ge=0, description="PDF总页数")


class KnowledgeChunkItem(BaseModel):
    id: int
    document_id: int
    page_no: Optional[int] = None
    chunk_index: int
    chunk_text: str
    chunk_hash: str
    char_count: int = 0
    vector_status: int = 0
    milvus_id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class KnowledgeDocumentListRequest(BaseModel):
    doc_type: Optional[str] = Field(None, description="按文档类型筛选")
    stock_code: Optional[str] = Field(None, description="按股票代码筛选")
    stock_abbr: Optional[str] = Field(None, description="按股票简称筛选")
    metadata_status: Optional[int] = Field(None, description="按元数据状态筛选")
    chunk_status: Optional[int] = Field(None, description="按切块状态筛选")
    vector_status: Optional[int] = Field(None, description="按向量状态筛选")
    page: int = Field(1, ge=1, description="页码")
    page_size: int = Field(20, ge=1, le=100, description="每页数量")


class KnowledgeChunkSearchRequest(BaseModel):
    document_id: Optional[int] = Field(None, description="按文档ID筛选")
    stock_code: Optional[str] = Field(None, description="按股票代码筛选（通过文档关联）")
    doc_type: Optional[str] = Field(None, description="按文档类型筛选")
    vector_status: Optional[int] = Field(None, description="按向量状态筛选")
    page: int = Field(1, ge=1, description="页码")
    page_size: int = Field(20, ge=1, le=100, description="每页数量")


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1, description="检索文本")
    stock_code: Optional[str] = Field(None, description="按股票代码筛选")
    doc_type: Optional[str] = Field(None, description="按文档类型筛选")
    top_k: int = Field(5, ge=1, le=100, description="返回结果数量")


class BatchChunkRequest(BaseModel):
    document_ids: list[int] = Field(..., min_length=1, description="文档ID列表")


class ChunkAllRequest(BaseModel):
    limit: int = Field(100, ge=1, le=500, description="最大处理数量")
    doc_type: Optional[str] = Field(None, description="按文档类型筛选")


class VectorizeDocumentRequest(BaseModel):
    batch_size: int = Field(20, ge=1, le=200, description="每批处理数量")
    force: bool = Field(False, description="是否强制重试失败/已完成切块")


class BatchStatusRequest(BaseModel):
    document_ids: list[int] = Field(..., min_length=1, description="文档ID列表")


# ========== 响应类（Response）==========

class DocumentUploadResponse(BaseModel):
    id: int
    title: str
    doc_type: str
    stock_code: Optional[str] = None
    stock_abbr: Optional[str] = None
    source_path: str
    chunk_status: int = 0
    vector_status: int = 0
    metadata_matched: bool = True
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class BatchUploadResponse(BaseModel):
    total: int = 0
    success: int = 0
    failed: int = 0
    documents: list[DocumentUploadResponse] = Field(default_factory=list)
    errors: list[dict] = Field(default_factory=list)


class ChunkSubmitResponse(BaseModel):
    document_id: int
    status: str
    message: str


class BatchChunkSubmitResponse(BaseModel):
    submitted: int = 0
    skipped: int = 0
    submitted_ids: list[int] = Field(default_factory=list)
    message: str = ""


class VectorizeSubmitResponse(BaseModel):
    document_id: int
    status: str
    message: str
    total_chunks: int = 0


class BatchVectorizeSubmitResponse(BaseModel):
    submitted: int = 0
    message: str = ""


class VectorizeResultResponse(BaseModel):
    total: int = 0
    success: int = 0
    failed: int = 0
    errors: list[dict] = Field(default_factory=list)


class DocumentStatusItem(BaseModel):
    id: int
    chunk_status: int = 0
    vector_status: int = 0
    chunk_count: int = 0


class InitResponse(BaseModel):
    success: bool = True
    message: str = "系统初始化成功"
    stock_metadata_count: int = 0
    industry_metadata_count: int = 0
    total_count: int = 0
    duplicates: int = 0
    errors: list[dict] = Field(default_factory=list)


class UploadPdfResponse(BaseModel):
    success: bool = True
    message: str = ""
    processed_count: int = 0
    failed_count: int = 0
    total_processed: int = 0
    total_pending: int = 0
    failed_documents: list[FailedDocumentItem] = Field(default_factory=list)
    errors: list[dict] = Field(default_factory=list)


class UploadSinglePdfResponse(BaseModel):
    success: bool = True
    message: str = ""
    document_id: int
    chunk_count: int = 0


class RetryDocumentResponse(BaseModel):
    success: bool = True
    message: str = ""
    document_id: int
    chunk_count: int = 0


class ProgressResponse(BaseModel):
    total_documents: int = 0
    metadata_loaded: int = 0
    pdf_uploaded: int = 0
    chunked: int = 0
    vectorized: int = 0
    pending_pdf_upload: int = 0
    pending_chunk: int = 0
    pending_vectorize: int = 0
    failed_chunk: int = 0
    failed_vectorize: int = 0
    progress_percentage: float = 0.0
    recent_processed: list[RecentProcessedItem] = Field(default_factory=list)


class InitStatusResponse(BaseModel):
    initialized: bool = False
    stock_metadata_count: int = 0
    industry_metadata_count: int = 0
    total_metadata_count: int = 0


class KnowledgeBaseStatsResponse(BaseModel):
    documents: DocumentStatsItem = Field(default_factory=DocumentStatsItem)
    chunks: ChunkStatsItem = Field(default_factory=ChunkStatsItem)


class ResetVectorStatusResponse(BaseModel):
    document_id: int
    old_status: int
    new_status: int
    chunk_reset_count: int = 0
    message: str = ""
