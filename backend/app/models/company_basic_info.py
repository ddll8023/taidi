from decimal import Decimal, InvalidOperation

from sqlalchemy import (
    CHAR,
    DECIMAL,
    CheckConstraint,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    func,
)
from sqlalchemy.orm import validates

from app.db.database import Base


ALLOWED_EXCHANGES = ("SH", "SZ", "BJ")
EXCHANGE_ALIAS_MAP = {
    "SH": "SH",
    "SSE": "SH",
    "上海证券交易所": "SH",
    "上交所": "SH",
    "SZ": "SZ",
    "SZSE": "SZ",
    "深圳证券交易所": "SZ",
    "深交所": "SZ",
    "BJ": "BJ",
    "BSE": "BJ",
    "北京证券交易所": "BJ",
    "北交所": "BJ",
}


def normalize_company_stock_code(raw_value: str) -> str:
    """将附件1中的股票代码统一规范为6位字符串。"""
    text = str(raw_value).strip().upper()
    if not text:
        raise ValueError("stock_code 不能为空")

    digits = "".join(char for char in text if char.isdigit())
    if not digits:
        raise ValueError(f"stock_code 必须包含数字，当前值：{raw_value}")

    if len(digits) > 6:
        raise ValueError(f"stock_code 最多允许 6 位数字，当前值：{raw_value}")

    return digits.zfill(6)


def normalize_exchange_code(raw_value: str) -> str:
    """将附件1中的上市交易所映射为 SH / SZ / BJ。"""
    text = str(raw_value).strip().upper()
    if not text:
        raise ValueError("exchange 不能为空")

    normalized = EXCHANGE_ALIAS_MAP.get(text)
    if normalized is None:
        raise ValueError(
            f"exchange 只允许映射为 {', '.join(ALLOWED_EXCHANGES)}，当前值：{raw_value}"
        )

    return normalized


class CompanyBasicInfo(Base):
    __tablename__ = "company_basic_info"

    stock_code = Column(CHAR(6), primary_key=True, comment="附件1-股票代码，统一补零为6位")
    stock_abbr = Column(String(50), nullable=False, comment="附件1-A股简称")
    company_name = Column(String(255), nullable=False, comment="附件1-公司名称")
    english_name = Column(String(255), comment="附件1-英文名称")
    csrc_industry = Column(String(255), comment="附件1-所属证监会行业")
    listed_exchange = Column(String(50), nullable=False, comment="附件1-上市交易所原始文本")
    exchange = Column(String(2), nullable=False, comment="标准化交易所代码：SH/SZ/BJ")
    security_category = Column(String(100), comment="附件1-证券类别")
    registered_region = Column(String(100), comment="附件1-注册区域")
    registered_capital_raw = Column(String(50), comment="附件1-注册资本原始文本")
    registered_capital_yuan = Column(
        DECIMAL(20, 2),
        comment="注册资本标准化数值，单位：元",
    )
    employee_count = Column(Integer, comment="附件1-雇员人数")
    management_count = Column(Integer, comment="附件1-管理人员人数")
    source_row_no = Column(Integer, nullable=False, comment="附件1原始序号")
    source_file_name = Column(String(255), nullable=False, comment="附件1源文件名")
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

    @validates(
        "stock_abbr",
        "company_name",
        "english_name",
        "csrc_industry",
        "listed_exchange",
        "security_category",
        "registered_region",
        "registered_capital_raw",
        "source_file_name",
    )
    def normalize_text_fields(self, key: str, value: str | None) -> str | None:
        required_fields = {
            "stock_abbr",
            "company_name",
            "listed_exchange",
            "source_file_name",
        }
        if value is None:
            if key in required_fields:
                raise ValueError(f"{key} 不能为空")
            return None

        normalized = str(value).strip()
        if not normalized:
            if key in required_fields:
                raise ValueError(f"{key} 不能为空")
            return None

        return normalized

    @validates("employee_count", "management_count", "source_row_no")
    def validate_non_negative_int_fields(
        self,
        key: str,
        value: int | str | None,
    ) -> int | None:
        if value is None:
            if key == "source_row_no":
                raise ValueError("source_row_no 不能为空")
            return None

        normalized = int(str(value).strip())
        if key == "source_row_no" and normalized <= 0:
            raise ValueError("source_row_no 必须大于 0")
        if key != "source_row_no" and normalized < 0:
            raise ValueError(f"{key} 不能为负数")
        return normalized

    @validates("registered_capital_yuan")
    def validate_registered_capital_yuan(
        self,
        _key: str,
        value: Decimal | str | float | int | None,
    ) -> Decimal | None:
        if value is None:
            return None

        try:
            normalized = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(
                f"registered_capital_yuan 不是合法数值，当前值：{value}"
            ) from exc

        if normalized < 0:
            raise ValueError("registered_capital_yuan 不能为负数")

        return normalized

    __table_args__ = (
        CheckConstraint(
            "length(stock_code) = 6",
            name="ck_company_basic_info_stock_code_length",
        ),
        CheckConstraint(
            "exchange IN ('SH', 'SZ', 'BJ')",
            name="ck_company_basic_info_exchange",
        ),
        CheckConstraint(
            "(registered_capital_yuan IS NULL OR registered_capital_yuan >= 0)",
            name="ck_company_basic_info_registered_capital_yuan",
        ),
        CheckConstraint(
            "(employee_count IS NULL OR employee_count >= 0)",
            name="ck_company_basic_info_employee_count",
        ),
        CheckConstraint(
            "(management_count IS NULL OR management_count >= 0)",
            name="ck_company_basic_info_management_count",
        ),
        CheckConstraint(
            "source_row_no > 0",
            name="ck_company_basic_info_source_row_no",
        ),
        Index("idx_company_basic_info_stock_abbr", "stock_abbr"),
        Index("idx_company_basic_info_company_name", "company_name"),
        Index(
            "idx_company_basic_info_lookup",
            "exchange",
            "csrc_industry",
            "security_category",
        ),
    )
