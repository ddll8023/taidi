from typing import Any


from app.db.database import Base
from sqlalchemy import (
    Column,
    String,
    SmallInteger,
    DateTime,
    func,
    JSON,
    Integer,
    ForeignKey,
    Text,
)


class ChatMessage(Base):
    __tablename__ = "chat_message"

    id = Column[int](
        Integer, primary_key=True, autoincrement=True, description="消息 ID"
    )
    session_id = Column[str](
        String(36),
        ForeignKey("chat_session.id", ondelete="CASCADE"),
        description="会话 ID",
    )
    message_type = Column[str](
        String(10), nullable=False, description="消息类型, conversation / summary"
    )
    summary_content = Column[str](Text, description="摘要内容")
    query = Column[str](Text, description="用户查询")
    intent_result = Column[dict](JSON, description="意图解析结果")
    sql_query = Column[str](Text, description="生成的 SQL")
    sql_result = Column[list](JSON, description="SQL 执行结果")
    answer = Column[str](Text, description="回答")
    created_at = Column[datetime](
        DateTime, server_default=func.now(), description="创建时间"
    )
    answer_at = Column[datetime](
        DateTime, server_default=func.now(), description="回答时间"
    )
