from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.mysql import JSON

from app.db.database import Base


class ChatMessage(Base):
    __tablename__ = "chat_message"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="消息ID")
    session_id = Column(
        String(36),
        ForeignKey("chat_session.id", ondelete="CASCADE"),
        nullable=False,
        comment="关联会话ID",
    )
    role = Column(String(10), nullable=False, comment="角色：user/assistant")
    content = Column(Text, nullable=False, comment="消息内容")
    intent_result = Column(JSON, comment="意图解析结果（assistant消息专用）")
    sql_query = Column(Text, comment="生成的SQL（assistant消息专用）")
    chart_paths = Column(JSON, comment="图表路径列表（assistant消息专用）")
    created_at = Column(
        DateTime,
        server_default=func.now(),
        nullable=False,
        comment="创建时间",
    )

    __table_args__ = (
        Index("idx_chat_message_session", "session_id", "created_at"),
    )
