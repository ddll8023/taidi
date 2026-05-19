from sqlalchemy import (
    CHAR,
    Column,
    DECIMAL,
    ForeignKey,
    Index,
    Integer,
    String,
)
from app.db.database import Base
from app.constants.import_company_base_info import ReportTypeEnum


class CorePerformanceIndicatorsSheet(Base):
    __tablename__ = "core_performance_indicators_sheet"

    report_id = Column[int](
        Integer,
        ForeignKey("financial_report.id", ondelete="CASCADE"),
        primary_key=True,
        comment="财报主表ID，事实表唯一主键",
    )
    stock_code = Column[str](
        CHAR(6),
        ForeignKey("company_basic_info.stock_code"),
        nullable=False,
        comment="股票代码，由 financial_report 主表统一回填",
    )
    stock_abbr = Column[str](
        String(50),
        nullable=False,
        comment="股票简称，由 financial_report 主表统一回填",
    )
    report_year = Column[int](
        Integer,
        nullable=False,
        comment="报告期-年份，由 financial_report 主表统一回填",
    )
    report_period = Column[str](
        String(2),
        nullable=False,
        comment="报告期：Q1/HY/Q3/FY，由 financial_report 主表统一回填",
    )
    report_type = Column[str](
        String(10),
        nullable=False,
        comment="报告类型：REPORT/SUMMARY，由 financial_report 主表统一回填",
    )
    eps = Column[float](DECIMAL(10, 4), comment="每股收益(元)")
    total_operating_revenue = Column[float](DECIMAL(20, 2), comment="营业总收入(万元)")
    operating_revenue_yoy_growth = Column[float](
        DECIMAL(10, 4), comment="营业总收入-同比增长(%)"
    )
    operating_revenue_qoq_growth = Column[float](
        DECIMAL(10, 4), comment="营业总收入-季度环比增长(%)"
    )
    net_profit_10k_yuan = Column[float](DECIMAL(20, 2), comment="净利润(万元)")
    net_profit_yoy_growth = Column[float](DECIMAL(10, 4), comment="净利润-同比增长(%)")
    net_profit_qoq_growth = Column[float](
        DECIMAL(10, 4), comment="净利润-季度环比增长(%)"
    )
    net_asset_per_share = Column[float](DECIMAL(10, 4), comment="每股净资产(元)")
    roe = Column[float](DECIMAL(10, 4), comment="净资产收益率(%)")
    operating_cf_per_share = Column[float](
        DECIMAL(10, 4), comment="每股经营现金流量(元)"
    )
    net_profit_excl_non_recurring = Column[float](
        DECIMAL(20, 2), comment="扣非净利润（万元）"
    )
    net_profit_excl_non_recurring_yoy = Column[float](
        DECIMAL(10, 4), comment="扣非净利润同比增长（%）"
    )
    gross_profit_margin = Column[float](DECIMAL(10, 4), comment="销售毛利率(%)")
    net_profit_margin = Column[float](DECIMAL(10, 4), comment="销售净利率（%）")
    roe_weighted_excl_non_recurring = Column[float](
        DECIMAL(10, 4), comment="加权平均净资产收益率（扣非）（%）"
    )

    __table_args__ = (
        Index(
            f"idx_core_performance_indicators_sheet_stock_period_type",
            "stock_code",
            "report_year",
            "report_period",
            "report_type",
        ),
        Index(
            f"idx_core_performance_indicators_sheet_abbr_period_type",
            "stock_abbr",
            "report_year",
            "report_period",
            "report_type",
        ),
        Index(
            f"idx_core_performance_indicators_sheet_period_type",
            "report_year",
            "report_period",
            "report_type",
        ),
    )
