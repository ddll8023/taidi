from typing import Any


from datetime import datetime


from app.db.database import Base
from sqlalchemy import Column, String, SmallInteger, DateTime, func, JSON
import uuid


class ChatSession(Base):
    __tablename__ = "chat_session"

    id = Column[str](
        String(36), primary_key=True, default=uuid.uuid4(), comment="会话 UUID"
    )
    session_name = Column[str](String(100), nullable=False, comment="会话名称")
    status = Column[int](SmallInteger, default=0, comment="0活跃 1已关闭")
    messages = Column[list[int]](
        JSON, nullable=False, comment="消息列表，滑动窗口，大小为5，保存message_id"
    )
    created_at = Column[datetime](
        DateTime, server_default=func.now(), comment="创建时间"
    )
    updated_at = Column[datetime](
        DateTime, server_default=func.now(), onupdate=func.now(), comment="更新时间"
    )
