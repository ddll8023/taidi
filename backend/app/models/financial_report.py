from sqlalchemy import (
    CHAR,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import validates

from app.db.database import Base
from app.models.company_basic_info import (
    ALLOWED_EXCHANGES,
    normalize_company_stock_code,
    normalize_exchange_code,
)


ALLOWED_REPORT_PERIODS = ("Q1", "HY", "Q3", "FY")
ALLOWED_REPORT_TYPES = ("REPORT", "SUMMARY")
ALLOWED_REPORT_LABELS = (
    "一季度报告",
    "半年度报告",
    "半年度报告摘要",
    "三季度报告",
    "年度报告",
    "年度报告摘要",
)
REPORT_PERIOD_SORT_MAP = {
    "Q1": 1,
    "HY": 2,
    "Q3": 3,
    "FY": 4,
}
REPORT_TYPE_PRIORITY_MAP = {
    "SUMMARY": 10,
    "REPORT": 20,
}
ALLOWED_EXCHANGES_SQL = ", ".join(f"'{item}'" for item in ALLOWED_EXCHANGES)


def normalize_report_period(value: str) -> str:
    normalized = str(value).strip().upper()
    if normalized not in ALLOWED_REPORT_PERIODS:
        raise ValueError(
            f"report_period 只允许 {', '.join(ALLOWED_REPORT_PERIODS)}，当前值：{value}"
        )
    return normalized


def normalize_report_type(value: str) -> str:
    normalized = str(value).strip().upper()
    if normalized not in ALLOWED_REPORT_TYPES:
        raise ValueError(
            f"report_type 只允许 {', '.join(ALLOWED_REPORT_TYPES)}，当前值：{value}"
        )
    return normalized


def normalize_report_label(value: str) -> str:
    normalized = str(value).strip()
    if normalized not in ALLOWED_REPORT_LABELS:
        raise ValueError(
            f"report_label 只允许 {', '.join(ALLOWED_REPORT_LABELS)}，当前值：{value}"
        )
    return normalized


def get_period_sort_key(report_period: str) -> int:
    return REPORT_PERIOD_SORT_MAP[normalize_report_period(report_period)]


def get_report_source_priority(report_type: str) -> int:
    return REPORT_TYPE_PRIORITY_MAP[normalize_report_type(report_type)]


class FinancialReport(Base):
    __tablename__ = "financial_report"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="财报主表ID")
    stock_code = Column(
        CHAR(6),
        ForeignKey("company_basic_info.stock_code"),
        nullable=False,
        comment="股票代码，关联 company_basic_info.stock_code",
    )
    stock_abbr = Column(String(50), nullable=False, comment="股票简称")
    report_year = Column(Integer, nullable=False, comment="报告期-年份")
    report_period = Column(String(2), nullable=False, comment="报告期：Q1/HY/Q3/FY")
    report_type = Column(
        String(10),
        nullable=False,
        default="REPORT",
        server_default=text("'REPORT'"),
        comment="报告类型：REPORT正式报告，SUMMARY摘要",
    )
    report_label = Column(
        String(20),
        nullable=False,
        comment="报告标签：一季度报告/半年度报告/三季度报告/年度报告/年度报告摘要",
    )
    exchange = Column(String(2), nullable=False, comment="交易所标识：SH/SZ/BJ")
    report_title = Column(String(255), nullable=False, comment="报告标题")
    report_date = Column(Date, comment="报告披露日期，样例中可稳定识别时写入")
    period_sort_key = Column(
        SmallInteger,
        nullable=False,
        default=0,
        server_default=text("0"),
        comment="报告期排序键：Q1=1，HY=2，Q3=3，FY=4",
    )
    source_priority = Column(
        SmallInteger,
        nullable=False,
        default=0,
        server_default=text("0"),
        comment="来源优先级：值越大优先级越高；REPORT 高于 SUMMARY",
    )
    source_file_name = Column(String(255), nullable=False, comment="上传源文件名")
    storage_path = Column(String(500), nullable=False, comment="PDF本地存储路径")
    structured_json_path = Column(String(500), comment="结构化JSON文件路径")
    parse_status = Column(
        SmallInteger,
        nullable=False,
        default=1,
        comment="解析状态：0待处理，1成功，2失败",
    )
    review_status = Column(
        SmallInteger,
        nullable=False,
        default=0,
        comment="审核状态：0待审核，1已通过，2已驳回",
    )
    validate_status = Column(
        SmallInteger,
        nullable=False,
        default=0,
        comment="校验状态：0待校验，1已通过，2已失败",
    )
    validate_message = Column(Text, comment="校验结果说明")
    import_status = Column(
        SmallInteger,
        nullable=False,
        default=1,
        comment="入库状态：0待入库，1已成功，2已失败",
    )
    vector_status = Column(
        SmallInteger,
        nullable=False,
        default=0,
        comment="向量状态：0待向量化，1向量化中，2向量化成功，3向量化失败，4已跳过",
    )
    vector_model = Column(String(100), comment="向量模型")
    vector_dim = Column(Integer, comment="向量维度")
    vector_version = Column(String(100), comment="向量版本")
    vector_error_message = Column(Text, comment="向量化失败原因")
    vectorized_at = Column(DateTime, comment="向量化完成时间")
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

    @validates("stock_code")
    def validate_stock_code(self, _key: str, value: str) -> str:
        return normalize_company_stock_code(value)

    @validates("exchange")
    def validate_exchange(self, _key: str, value: str) -> str:
        return normalize_exchange_code(value)

    @validates("report_year")
    def validate_report_year(self, _key: str, value: int | str) -> int:
        normalized = int(str(value).strip())
        if normalized < 2000 or normalized > 2100:
            raise ValueError(f"report_year 超出允许范围，当前值：{value}")
        return normalized

    @validates("report_period")
    def validate_report_period(self, _key: str, value: str) -> str:
        return normalize_report_period(value)

    @validates("report_type")
    def validate_report_type(self, _key: str, value: str) -> str:
        return normalize_report_type(value)

    @validates("report_label")
    def validate_report_label(self, _key: str, value: str) -> str:
        return normalize_report_label(value)

    @validates("period_sort_key", "source_priority")
    def validate_non_negative_int(self, key: str, value: int | str) -> int:
        normalized = int(str(value).strip())
        if normalized < 0:
            raise ValueError(f"{key} 不能为负数")
        return normalized

    @validates("stock_abbr", "report_title", "source_file_name", "storage_path")
    def normalize_required_text_fields(self, key: str, value: str | None) -> str:
        if value is None:
            raise ValueError(f"{key} 不能为空")

        normalized = str(value).strip()
        if not normalized:
            raise ValueError(f"{key} 不能为空")
        return normalized

    @validates(
        "structured_json_path",
        "validate_message",
        "vector_model",
        "vector_version",
        "vector_error_message",
    )
    def normalize_optional_text_fields(self, _key: str, value: str | None) -> str | None:
        if value is None:
            return None

        normalized = str(value).strip()
        return normalized or None

    __table_args__ = (
        CheckConstraint(
            "length(stock_code) = 6",
            name="ck_financial_report_stock_code_length",
        ),
        CheckConstraint(
            "report_period IN ('Q1', 'HY', 'Q3', 'FY')",
            name="ck_financial_report_report_period",
        ),
        CheckConstraint(
            "report_type IN ('REPORT', 'SUMMARY')",
            name="ck_financial_report_report_type",
        ),
        CheckConstraint(
            "report_label IN ('一季度报告', '半年度报告', '半年度报告摘要', '三季度报告', '年度报告', '年度报告摘要')",
            name="ck_financial_report_report_label",
        ),
        CheckConstraint(
            f"exchange IN ({ALLOWED_EXCHANGES_SQL})",
            name="ck_financial_report_exchange",
        ),
        CheckConstraint(
            "report_year >= 2000 AND report_year <= 2100",
            name="ck_financial_report_report_year",
        ),
        CheckConstraint(
            "period_sort_key >= 0",
            name="ck_financial_report_period_sort_key",
        ),
        CheckConstraint(
            "source_priority >= 0",
            name="ck_financial_report_source_priority",
        ),
        UniqueConstraint(
            "stock_code",
            "report_year",
            "report_period",
            "report_type",
            name="uk_financial_report_identity",
        ),
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
