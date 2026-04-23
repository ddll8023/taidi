import hashlib

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import validates

from app.db.database import Base


ALLOWED_CHUNK_VECTOR_STATUSES = (0, 1, 2, 3)
CHUNK_VECTOR_STATUS_PENDING = 0
CHUNK_VECTOR_STATUS_PROCESSING = 1
CHUNK_VECTOR_STATUS_COMPLETED = 2
CHUNK_VECTOR_STATUS_FAILED = 3


def compute_chunk_hash(chunk_text: str) -> str:
    return hashlib.sha256(chunk_text.encode("utf-8")).hexdigest()


class KnowledgeChunk(Base):
    __tablename__ = "knowledge_chunk"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="切块ID")
    document_id = Column(
        Integer,
        ForeignKey("knowledge_document.id", ondelete="CASCADE"),
        nullable=False,
        comment="关联文档ID",
    )
    page_no = Column(Integer, comment="源文档页码（1-based）")
    chunk_index = Column(
        Integer,
        nullable=False,
        comment="切块序号（同一文档内从0递增）",
    )
    chunk_text = Column(Text, nullable=False, comment="切块文本内容")
    chunk_hash = Column(
        String(64),
        nullable=False,
        comment="切块文本SHA256哈希，用于去重",
    )
    char_count = Column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
        comment="文本字符数",
    )
    vector_status = Column(
        SmallInteger,
        nullable=False,
        default=0,
        comment="向量状态：0待向量化，1向量化中，2已向量化，3向量化失败",
    )
    milvus_id = Column(BigInteger, comment="Milvus中的向量记录ID")
    vector_error_message = Column(Text, comment="向量化失败原因")
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

    @validates("document_id")
    def validate_document_id(self, _key: str, value: int | str) -> int:
        normalized = int(str(value).strip())
        if normalized <= 0:
            raise ValueError("document_id 必须大于 0")
        return normalized

    @validates("chunk_index")
    def validate_chunk_index(self, _key: str, value: int | str) -> int:
        normalized = int(str(value).strip())
        if normalized < 0:
            raise ValueError("chunk_index 不能为负数")
        return normalized

    @validates("chunk_text")
    def validate_chunk_text(self, _key: str, value: str | None) -> str:
        if value is None:
            raise ValueError("chunk_text 不能为空")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("chunk_text 不能为空")
        return normalized

    @validates("chunk_hash")
    def validate_chunk_hash(self, _key: str, value: str | None) -> str:
        if value is None:
            raise ValueError("chunk_hash 不能为空")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("chunk_hash 不能为空")
        return normalized

    @validates("page_no")
    def validate_page_no(self, _key: str, value: int | str | None) -> int | None:
        if value is None:
            return None
        normalized = int(str(value).strip())
        if normalized < 0:
            raise ValueError("page_no 不能为负数")
        return normalized

    @validates("char_count")
    def validate_char_count(self, _key: str, value: int | str) -> int:
        normalized = int(str(value).strip())
        if normalized < 0:
            raise ValueError("char_count 不能为负数")
        return normalized

    @validates("vector_status")
    def validate_vector_status(self, _key: str, value: int | str) -> int:
        normalized = int(str(value).strip())
        if normalized not in ALLOWED_CHUNK_VECTOR_STATUSES:
            raise ValueError(
                f"vector_status 只允许 {ALLOWED_CHUNK_VECTOR_STATUSES}，当前值：{value}"
            )
        return normalized

    @validates("vector_error_message")
    def normalize_optional_text(self, _key: str, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    __table_args__ = (
        CheckConstraint(
            "document_id > 0",
            name="ck_knowledge_chunk_document_id",
        ),
        CheckConstraint(
            "chunk_index >= 0",
            name="ck_knowledge_chunk_chunk_index",
        ),
        CheckConstraint(
            "page_no IS NULL OR page_no >= 0",
            name="ck_knowledge_chunk_page_no",
        ),
        CheckConstraint(
            "char_count >= 0",
            name="ck_knowledge_chunk_char_count",
        ),
        CheckConstraint(
            f"vector_status IN ({', '.join(str(s) for s in ALLOWED_CHUNK_VECTOR_STATUSES)})",
            name="ck_knowledge_chunk_vector_status",
        ),
        UniqueConstraint(
            "document_id",
            "chunk_index",
            name="uk_knowledge_chunk_document_chunk",
        ),
        Index(
            "idx_knowledge_chunk_document",
            "document_id",
            "chunk_index",
        ),
        Index(
            "idx_knowledge_chunk_hash",
            "chunk_hash",
        ),
        Index(
            "idx_knowledge_chunk_vector_status",
            "vector_status",
        ),
    )
