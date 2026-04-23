from sqlalchemy import CHAR, CheckConstraint, Column, ForeignKey, Index, Integer, String
from sqlalchemy.orm import validates

from app.models.company_basic_info import normalize_company_stock_code
from app.models.financial_report import (
    ALLOWED_REPORT_PERIODS,
    ALLOWED_REPORT_TYPES,
    normalize_report_period,
    normalize_report_type,
)


REPORT_PERIODS_SQL = ", ".join(f"'{item}'" for item in ALLOWED_REPORT_PERIODS)
REPORT_TYPES_SQL = ", ".join(f"'{item}'" for item in ALLOWED_REPORT_TYPES)


class ReportFactIdentityMixin:
    report_id = Column(
        Integer,
        ForeignKey("financial_report.id", ondelete="CASCADE"),
        primary_key=True,
        comment="财报主表ID，事实表唯一主键",
    )
    stock_code = Column(
        CHAR(6),
        ForeignKey("company_basic_info.stock_code"),
        nullable=False,
        comment="股票代码，由 financial_report 主表统一回填",
    )
    stock_abbr = Column(
        String(50),
        nullable=False,
        comment="股票简称，由 financial_report 主表统一回填",
    )
    report_year = Column(
        Integer,
        nullable=False,
        comment="报告期-年份，由 financial_report 主表统一回填",
    )
    report_period = Column(
        String(2),
        nullable=False,
        comment="报告期：Q1/HY/Q3/FY，由 financial_report 主表统一回填",
    )
    report_type = Column(
        String(10),
        nullable=False,
        comment="报告类型：REPORT/SUMMARY，由 financial_report 主表统一回填",
    )

    @validates("stock_code")
    def validate_stock_code(self, _key: str, value: str) -> str:
        return normalize_company_stock_code(value)

    @validates("stock_abbr")
    def validate_stock_abbr(self, _key: str, value: str) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("stock_abbr 不能为空")
        return normalized

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


def build_report_fact_table_args(table_name: str):
    return (
        CheckConstraint(
            "length(stock_code) = 6",
            name=f"ck_{table_name}_stock_code_length",
        ),
        CheckConstraint(
            "report_year >= 2000 AND report_year <= 2100",
            name=f"ck_{table_name}_report_year",
        ),
        CheckConstraint(
            f"report_period IN ({REPORT_PERIODS_SQL})",
            name=f"ck_{table_name}_report_period",
        ),
        CheckConstraint(
            f"report_type IN ({REPORT_TYPES_SQL})",
            name=f"ck_{table_name}_report_type",
        ),
        Index(
            f"idx_{table_name}_stock_period_type",
            "stock_code",
            "report_year",
            "report_period",
            "report_type",
        ),
        Index(
            f"idx_{table_name}_abbr_period_type",
            "stock_abbr",
            "report_year",
            "report_period",
            "report_type",
        ),
        Index(
            f"idx_{table_name}_period_type",
            "report_year",
            "report_period",
            "report_type",
        ),
    )
