from sqlalchemy import (
    CHAR,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.orm import validates

from app.db.database import Base
from app.models.company_basic_info import normalize_company_stock_code


ALLOWED_DOC_TYPES = ("RESEARCH_REPORT", "FINANCIAL_REPORT", "INDUSTRY_REPORT")
DOC_TYPE_LABELS = {
    "RESEARCH_REPORT": "个股研报",
    "FINANCIAL_REPORT": "财报原文",
    "INDUSTRY_REPORT": "行业研报",
}
ALLOWED_CHUNK_STATUSES = (0, 1, 2, 3)
CHUNK_STATUS_PENDING = 0
CHUNK_STATUS_PROCESSING = 1
CHUNK_STATUS_COMPLETED = 2
CHUNK_STATUS_FAILED = 3

ALLOWED_VECTOR_STATUSES = (0, 1, 2, 3, 4)
VECTOR_STATUS_PENDING = 0
VECTOR_STATUS_PROCESSING = 1
VECTOR_STATUS_SUCCESS = 2
VECTOR_STATUS_FAILED = 3
VECTOR_STATUS_SKIPPED = 4

ALLOWED_METADATA_STATUSES = (0, 1, 2)
METADATA_STATUS_NOT_LOADED = 0
METADATA_STATUS_LOADED = 1
METADATA_STATUS_PDF_UPLOADED = 2

DOC_TYPES_SQL = ", ".join(f"'{item}'" for item in ALLOWED_DOC_TYPES)


def normalize_doc_type(value: str) -> str:
    normalized = str(value).strip().upper()
    if normalized not in ALLOWED_DOC_TYPES:
        raise ValueError(
            f"doc_type 只允许 {', '.join(ALLOWED_DOC_TYPES)}，当前值：{value}"
        )
    return normalized


class KnowledgeDocument(Base):
    __tablename__ = "knowledge_document"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="文档ID")
    doc_type = Column(
        String(32),
        nullable=False,
        comment="文档类型：RESEARCH_REPORT个股研报/FINANCIAL_REPORT财报原文/INDUSTRY_REPORT行业研报",
    )
    title = Column(String(500), nullable=False, comment="文档标题")
    source_path = Column(String(500), nullable=False, comment="PDF源文件路径")
    stock_code = Column(
        CHAR(6),
        nullable=True,
        comment="股票代码，行业研报可为空",
    )
    stock_abbr = Column(String(50), comment="股票简称")
    publish_date = Column(Date, comment="发布日期")
    org_name = Column(String(255), comment="研究机构名称（研报专用）")
    industry_name = Column(String(255), comment="行业名称（行业研报专用）")
    researcher = Column(String(200), comment="研究员")
    em_rating_name = Column(String(100), comment="评级")
    predict_this_year_eps = Column(String(50), comment="预测EPS")
    predict_this_year_pe = Column(String(50), comment="预测PE")
    financial_report_id = Column(
        Integer,
        ForeignKey("financial_report.id", ondelete="SET NULL"),
        nullable=True,
        comment="关联财报主表ID（仅FINANCIAL_REPORT类型）",
    )
    page_count = Column(Integer, comment="PDF总页数")
    chunk_count = Column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
        comment="切块数量",
    )
    chunk_status = Column(
        SmallInteger,
        nullable=False,
        default=0,
        comment="切块状态：0待切块，1切块中，2切块完成，3切块失败",
    )
    chunk_error_message = Column(Text, comment="切块失败原因")
    metadata_status = Column(
        SmallInteger,
        nullable=False,
        default=0,
        comment="元数据状态：0未加载，1已加载（待上传PDF），2PDF已上传",
    )
    error_message = Column(Text, comment="处理失败原因（通用）")
    vector_status = Column(
        SmallInteger,
        nullable=False,
        default=0,
        comment="向量状态：0待向量化，1向量化中，2向量化成功，3向量化失败，4已跳过",
    )
    vector_model = Column(String(100), comment="向量模型")
    vector_dim = Column(Integer, comment="向量维度")
    vector_version = Column(String(100), comment="向量版本")
    vector_error_message = Column(Text, comment="向量化失败原因")
    vectorized_at = Column(DateTime, comment="向量化完成时间")
    created_at = Column(
        DateTime,
        server_default=func.now(),
        nullable=False,
        comment="创建时间",
    )
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        comment="更新时间",
    )

    @validates("doc_type")
    def validate_doc_type(self, _key: str, value: str) -> str:
        return normalize_doc_type(value)

    @validates("stock_code")
    def validate_stock_code(self, _key: str, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_company_stock_code(value)

    @validates("title", "source_path")
    def normalize_required_text_fields(self, key: str, value: str | None) -> str:
        if value is None:
            raise ValueError(f"{key} 不能为空")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError(f"{key} 不能为空")
        return normalized

    @validates(
        "stock_abbr",
        "org_name",
        "industry_name",
        "researcher",
        "em_rating_name",
        "predict_this_year_eps",
        "predict_this_year_pe",
        "vector_model",
        "vector_version",
        "vector_error_message",
        "chunk_error_message",
        "error_message",
    )
    def normalize_optional_text_fields(self, _key: str, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @validates("chunk_count", "page_count")
    def validate_non_negative_int(self, key: str, value: int | str | None) -> int | None:
        if value is None:
            return None
        normalized = int(str(value).strip())
        if normalized < 0:
            raise ValueError(f"{key} 不能为负数")
        return normalized

    @validates("chunk_status")
    def validate_chunk_status(self, _key: str, value: int | str) -> int:
        normalized = int(str(value).strip())
        if normalized not in ALLOWED_CHUNK_STATUSES:
            raise ValueError(
                f"chunk_status 只允许 {ALLOWED_CHUNK_STATUSES}，当前值：{value}"
            )
        return normalized

    @validates("metadata_status")
    def validate_metadata_status(self, _key: str, value: int | str) -> int:
        normalized = int(str(value).strip())
        if normalized not in ALLOWED_METADATA_STATUSES:
            raise ValueError(
                f"metadata_status 只允许 {ALLOWED_METADATA_STATUSES}，当前值：{value}"
            )
        return normalized

    @validates("vector_status")
    def validate_vector_status(self, _key: str, value: int | str) -> int:
        normalized = int(str(value).strip())
        if normalized not in ALLOWED_VECTOR_STATUSES:
            raise ValueError(
                f"vector_status 只允许 {ALLOWED_VECTOR_STATUSES}，当前值：{value}"
            )
        return normalized

    __table_args__ = (
        CheckConstraint(
            f"doc_type IN ({DOC_TYPES_SQL})",
            name="ck_knowledge_document_doc_type",
        ),
        CheckConstraint(
            "stock_code IS NULL OR length(stock_code) = 6",
            name="ck_knowledge_document_stock_code_length",
        ),
        CheckConstraint(
            "chunk_count >= 0",
            name="ck_knowledge_document_chunk_count",
        ),
        CheckConstraint(
            "page_count IS NULL OR page_count >= 0",
            name="ck_knowledge_document_page_count",
        ),
        CheckConstraint(
            f"chunk_status IN ({', '.join(str(s) for s in ALLOWED_CHUNK_STATUSES)})",
            name="ck_knowledge_document_chunk_status",
        ),
        CheckConstraint(
            f"metadata_status IN ({', '.join(str(s) for s in ALLOWED_METADATA_STATUSES)})",
            name="ck_knowledge_document_metadata_status",
        ),
        CheckConstraint(
            f"vector_status IN ({', '.join(str(s) for s in ALLOWED_VECTOR_STATUSES)})",
            name="ck_knowledge_document_vector_status",
        ),
        Index(
            "idx_knowledge_document_lookup",
            "doc_type",
            "stock_code",
            "publish_date",
        ),
        Index(
            "idx_knowledge_document_stock_abbr",
            "stock_abbr",
        ),
        Index(
            "idx_knowledge_document_status",
            "metadata_status",
            "chunk_status",
            "vector_status",
        ),
    )
