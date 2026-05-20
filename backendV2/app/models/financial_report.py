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
        ForeignKey("company_basic_info.stock_code", ondelete="CASCADE"),
        nullable=False,
        comment="股票代码，关联 company_basic_info.stock_code",
    )
    report_year = Column[int](Integer, nullable=False, comment="报告期-年份")
    report_period = Column[str](
        String(2), nullable=False, comment="报告期：Q1/HY/Q3/FY"
    )
    report_type = Column[str](
        String(10),
        nullable=False,
        comment="报告类型：REPORT正式报告，SUMMARY摘要",
    )
    report_label = Column[str](
        String(20),
        nullable=False,
        default="",
        comment="报告标签：一季度报告/半年度报告/三季度报告/年度报告/年度报告摘要",
    )
    report_title = Column[str](String(255), nullable=False, comment="报告标题")
    report_date = Column[date](Date, comment="报告披露日期，样例中可稳定识别时写入")
    period_sort_key = Column[int](
        SmallInteger,
        nullable=False,
        server_default=text("0"),
        comment="报告期排序键：Q1=1，Q2=2，Q3=3，Q4=4，FY=5，HY=6",
    )
    source_priority = Column[int](
        SmallInteger,
        nullable=False,
        server_default=text("0"),
        comment="来源优先级：值越大优先级越高；REPORT 0， SUMMARY 1，",
    )
    source_file_name = Column[str](String(512), nullable=False, comment="上传源文件名")
    stock_abbr = Column[str](String(50), nullable=False, comment="A股简称")
    storage_path = Column[str](String(512), nullable=False, comment="PDF本地存储路径")
    structured_json_path = Column[str](String(512), comment="结构化JSON文件路径")
    parse_status = Column[int](
        SmallInteger,
        nullable=False,
        server_default=text("0"),
        comment="解析状态：0待处理，1成功，2失败",
    )
    import_status = Column[int](
        SmallInteger,
        nullable=False,
        server_default=text("0"),
        comment="入库状态：0待入库，1已成功，2已失败",
    )
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
            "stock_abbr",
            unique=True,
        ),
        Index(
            "idx_financial_report_status",
            "parse_status",
            "import_status",
        ),
    )
