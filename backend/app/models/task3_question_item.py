from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, SmallInteger, String, Text, func
from sqlalchemy.dialects.mysql import JSON

from app.db.database import Base


class Task3QuestionItem(Base):
    __tablename__ = "task3_question_item"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="题目ID")
    workspace_id = Column(
        Integer,
        ForeignKey("task3_workspace.id", ondelete="CASCADE"),
        nullable=False,
        comment="关联工作台ID",
    )
    question_code = Column(String(20), nullable=False, comment="题目编号（如C1001）")
    question_type = Column(String(50), comment="问题类型")
    question_raw_json = Column(Text, comment="原始问题JSON字符串")
    status = Column(
        SmallInteger,
        nullable=False,
        default=0,
        server_default="0",
        comment="状态：0待处理 1回答中 2已完成 3失败",
    )
    answer_json = Column(JSON, comment="回答JSON数组 [{Q, A{content,image,references}}]")
    sql_text = Column(Text, comment="生成的SQL语句")
    chart_type = Column(String(50), comment="图表类型")
    image_paths_json = Column(JSON, comment="图表文件路径列表")
    execution_plan = Column(JSON, comment="执行计划对象")
    verification = Column(JSON, comment="校验结果对象")
    retrieval_summary = Column(JSON, comment="知识库检索摘要")
    last_error = Column(Text, comment="最后一次错误信息")
    answered_at = Column(DateTime, comment="回答完成时间")
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
        Index("idx_task3_question_workspace", "workspace_id"),
        Index("idx_task3_question_code", "question_code"),
        Index("idx_task3_question_status", "status"),
    )
