from datetime import datetime
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    func,
    text,
    CHAR,
)

from app.db.database import Base


class KnowledgeChunk(Base):
    __tablename__ = "knowledge_chunk"

    id = Column[int](Integer, primary_key=True, autoincrement=True, comment="切块ID")
    document_id = Column[int](
        Integer,
        ForeignKey("knowledge_document.id", ondelete="CASCADE"),
        nullable=False,
        comment="关联文档ID",
    )
    doc_type = Column[str](
        String(32),
        nullable=False,
        comment="文档类型：RESEARCH_REPORT个股研报/FINANCIAL_REPORT财报原文",
    )
    stock_code = Column[str](
        CHAR(6),
        nullable=True,
        comment="股票代码，行业研报可为空",
    )
    page_no = Column[int](Integer, comment="源文档页码（1-based）")
    chunk_index = Column[int](
        Integer,
        nullable=False,
        comment="切块序号（同一文档内从0递增）",
    )
    chunk_text = Column[str](Text, nullable=False, comment="切块文本内容")
    chunk_hash = Column[str](
        String(64),
        nullable=False,
        comment="切块文本SHA256哈希，用于去重",
    )
    char_count = Column[int](
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
        comment="文本字符数",
    )
    vector_status = Column[int](
        SmallInteger,
        nullable=False,
        default=0,
        comment="向量状态：0待向量化，1向量化中，2已向量化，3向量化失败",
    )
    vector_error_message = Column[str](Text, comment="向量化失败原因")
    vector_model = Column[str](String(100), comment="向量模型")
    vector_dim = Column[int](Integer, comment="向量维度")
    vector_version = Column[str](String(100), comment="向量版本")
    vectorized_at = Column[datetime](DateTime, comment="向量化完成时间")
    created_at = Column[datetime](
        DateTime,
        server_default=func.now(),
        nullable=False,
        comment="创建时间",
    )
    updated_at = Column[datetime](
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        comment="更新时间",
    )

    __table_args__ = (
        Index(
            "idx_knowledge_chunk_document",
            "document_id",
            "chunk_index",
            unique=True,
        ),
        Index(
            "idx_knowledge_chunk_hash",
            "chunk_hash",
            unique=True,
        ),
        Index(
            "idx_knowledge_chunk_vector_status",
            "vector_status",
        ),
    )
