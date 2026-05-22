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
    role = Column[str](String(10), description="消息角色,user/assistant")
    content = Column[str](Text, description="消息内容")
    intent_result = Column[dict](JSON, description="意图解析结果")
    sql_query = Column[str](Text, description="生成的 SQL")
    chart_paths = Column[list[str]](JSON, description="图表路径列表")
