from datetime import date, datetime

from sqlalchemy import (
    CHAR,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    func,
    text,
)

from app.db.database import Base


class FinancialReport(Base):
    __tablename__ = "financial_report"

    id = Column[int](
        Integer, primary_key=True, autoincrement=True, comment="财报主表ID"
    )
    stock_code = Column[str](
        CHAR(6),
        ForeignKey("company_basic_info.stock_code"),
        nullable=False,
        comment="股票代码，关联 company_basic_info.stock_code",
    )
    stock_abbr = Column[str](String(50), nullable=False, comment="股票简称")
    report_year = Column[int](Integer, nullable=False, comment="报告期-年份")
    report_period = Column[str](
        String(2), nullable=False, comment="报告期：Q1/HY/Q3/FY"
    )
    report_type = Column[str](
        String(10),
        nullable=False,
        default="REPORT",
        server_default=text("'REPORT'"),
        comment="报告类型：REPORT正式报告，SUMMARY摘要",
    )
    report_label = Column[str](
        String(20),
        nullable=False,
        comment="报告标签：一季度报告/半年度报告/三季度报告/年度报告/年度报告摘要",
    )
    exchange = Column[str](String(2), nullable=False, comment="交易所标识：SH/SZ/BJ")
    report_title = Column[str](String(255), nullable=False, comment="报告标题")
    report_date = Column[date](Date, comment="报告披露日期，样例中可稳定识别时写入")
    period_sort_key = Column[int](
        SmallInteger,
        nullable=False,
        server_default=text("0"),
        comment="报告期排序键：Q1=1，HY=2，Q3=3，FY=4",
    )
    source_priority = Column[int](
        SmallInteger,
        nullable=False,
        server_default=text("0"),
        comment="来源优先级：值越大优先级越高；REPORT 高于 SUMMARY",
    )
    source_file_name = Column[str](String(512), nullable=False, comment="上传源文件名")
    storage_path = Column[str](String(512), nullable=False, comment="PDF本地存储路径")
    structured_json_path = Column[str](String(512), comment="结构化JSON文件路径")
    parse_status = Column[int](
        SmallInteger,
        nullable=False,
        server_default=text("1"),
        comment="解析状态：0待处理，1成功，2失败",
    )
    review_status = Column[int](
        SmallInteger,
        nullable=False,
        server_default=text("0"),
        comment="审核状态：0待审核，1已通过，2已驳回",
    )
    validate_status = Column[int](
        SmallInteger,
        nullable=False,
        server_default=text("0"),
        comment="校验状态：0待校验，1已通过，2已失败",
    )
    validate_message = Column[str](Text, comment="校验结果说明")
    import_status = Column[int](
        SmallInteger,
        nullable=False,
        server_default=text("1"),
        comment="入库状态：0待入库，1已成功，2已失败",
    )
    vector_status = Column[int](
        SmallInteger,
        nullable=False,
        server_default=text("0"),
        comment="向量状态：0待向量化，1向量化中，2向量化成功，3向量化失败，4已跳过",
    )
    vector_model = Column[str](String(100), comment="向量模型")
    vector_dim = Column[int](Integer, comment="向量维度")
    vector_version = Column[str](String(100), comment="向量版本")
    vector_error_message = Column[str](Text, comment="向量化失败原因")
    vectorized_at = Column[datetime](DateTime, comment="向量化完成时间")
    created_at = Column[datetime](
        DateTime,
        server_default=func.now(),
        nullable=False,
        comment="创建时间",
    )
    updated_at = Column[datetime](
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        comment="更新时间",
    )

    __table_args__ = (
        Index(
            "idx_financial_report_lookup",
            "stock_code",
            "report_year",
            "period_sort_key",
            "report_type",
        ),
        Index("idx_financial_report_stock_abbr", "stock_abbr"),
        Index("idx_financial_report_exchange_year", "exchange", "report_year"),
        Index(
            "idx_financial_report_status",
            "parse_status",
            "review_status",
            "validate_status",
            "import_status",
            "vector_status",
        ),
    )
