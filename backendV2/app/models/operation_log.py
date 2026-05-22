"""操作日志模型：记录上传、解析、向量化等各阶段的操作状态"""

from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, SmallInteger, String, Text, func

from app.db.database import Base

# ── 操作类型常量 ──
OPERATION_UPLOAD = "UPLOAD"        # 文件上传建档
OPERATION_PARSE = "PARSE"          # 财报解析（LLM抽取）
OPERATION_VECTORIZE = "VECTORIZE"  # 向量化

# ── 操作状态常量 ──
STATUS_PROCESSING = "PROCESSING"  # 处理中
STATUS_SUCCESS = "SUCCESS"        # 成功
STATUS_FAILED = "FAILED"          # 失败


class OperationLog(Base):
    """操作日志：记录上传、解析、向量化等所有后台操作的执行结果"""

    __tablename__ = "operation_log"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="日志ID")
    operation_type = Column(
        String(16), nullable=False, comment="操作类型：UPLOAD/PARSE/VECTORIZE"
    )
    operation_status = Column(
        String(16), nullable=False, comment="状态：PROCESSING/SUCCESS/FAILED"
    )
    source_file_name = Column(String(255), nullable=True, comment="源文件名")
    storage_path = Column(String(500), nullable=True, comment="文件存储路径")
    stock_code = Column(String(6), nullable=True, comment="股票代码（成功时才有）")
    report_id = Column(Integer, nullable=True, comment="关联的 financial_report ID")
    error_message = Column(Text, nullable=True, comment="失败原因")
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
