from sqlalchemy import Column, DateTime, Index, SmallInteger, String, func
from sqlalchemy.dialects.mysql import JSON

from app.db.database import Base


class ChatSession(Base):
    __tablename__ = "chat_session"

    id = Column(String(36), primary_key=True, comment="会话UUID")
    name = Column(String(100), comment="会话名称")
    status = Column(
        SmallInteger,
        nullable=False,
        default=0,
        server_default="0",
        comment="状态：0活跃 1已关闭",
    )
    context_slots = Column(JSON, comment="当前槽位快照：公司/指标/时间等")
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

    __table_args__ = (
        Index("idx_chat_session_status", "status"),
        Index("idx_chat_session_updated", "updated_at"),
    )
