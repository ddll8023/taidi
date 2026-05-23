from typing import Any


from datetime import datetime


from app.db.database import Base
from sqlalchemy import Column, String, SmallInteger, DateTime, func, JSON
import uuid


class ChatSession(Base):
    __tablename__ = "chat_session"

    id = Column[str](
        String(36), primary_key=True, default=uuid.uuid4(), description="会话 UUID"
    )
    session_name = Column[str](String(100), nullable=False, description="会话名称")
    status = Column[int](SmallInteger, default=0, description="0活跃 1已关闭")
    messages = Column[list[int]](
        JSON, nullable=False, description="消息列表，滑动窗口，大小为10，保存message_id"
    )
    created_at = Column[datetime](
        DateTime, server_default=func.now(), description="创建时间"
    )
    updated_at = Column[datetime](
        DateTime, server_default=func.now(), onupdate=func.now(), description="更新时间"
    )
