from sqlalchemy import Column, DateTime, Index, Integer, SmallInteger, String, func

from app.db.database import Base


class Task3Workspace(Base):
    __tablename__ = "task3_workspace"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="工作台ID")
    source_file_name = Column(String(255), comment="附件6源文件名")
    source_file_path = Column(String(500), comment="附件6源文件路径")
    import_status = Column(
        SmallInteger,
        nullable=False,
        default=0,
        server_default="0",
        comment="导入状态：0未导入 1导入中 2已导入 3导入失败",
    )
    total_questions = Column(Integer, default=0, server_default="0", comment="题目总数")
    answered_count = Column(Integer, default=0, server_default="0", comment="已回答数量")
    failed_count = Column(Integer, default=0, server_default="0", comment="失败数量")
    pending_count = Column(Integer, default=0, server_default="0", comment="待处理数量")
    last_export_path = Column(String(500), comment="最近导出文件路径")
    last_exported_at = Column(DateTime, comment="最近导出时间")
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
        Index("idx_task3_workspace_status", "import_status"),
        Index("idx_task3_workspace_updated", "updated_at"),
    )
