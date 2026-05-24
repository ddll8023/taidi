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


class CashFlowSheet(Base):
    __tablename__ = "cash_flow_sheet"

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
    net_cash_flow = Column[float](DECIMAL(20, 2), comment="净现金流(元)")
    net_cash_flow_yoy_growth = Column[float](
        DECIMAL(10, 4), comment="净现金流-同比增长(%)"
    )
    operating_cf_net_amount = Column[float](
        DECIMAL(20, 2), comment="经营性现金流-现金流量净额(万元)"
    )
    operating_cf_ratio_of_net_cf = Column[float](
        DECIMAL(10, 4), comment="经营性现金流-净现金流占比(%)"
    )
    operating_cf_cash_from_sales = Column[float](
        DECIMAL(20, 2), comment="经营性现金流-销售商品收到的现金（万元）"
    )
    investing_cf_net_amount = Column[float](
        DECIMAL(20, 2), comment="投资性现金流-现金流量净额(万元)"
    )
    investing_cf_ratio_of_net_cf = Column[float](
        DECIMAL(10, 4), comment="投资性现金流-净现金流占比(%)"
    )
    investing_cf_cash_for_investments = Column[float](
        DECIMAL(20, 2), comment="投资性现金流-投资支付的现金（万元）"
    )
    investing_cf_cash_from_investment_recovery = Column[float](
        DECIMAL(20, 2), comment="投资性现金流-收回投资收到的现金（万元）"
    )
    financing_cf_cash_from_borrowing = Column[float](
        DECIMAL(20, 2), comment="融资性现金流-取得借款收到的现金（万元）"
    )
    financing_cf_cash_for_debt_repayment = Column[float](
        DECIMAL(20, 2), comment="融资性现金流-偿还债务支付的现金（万元）"
    )
    financing_cf_net_amount = Column[float](
        DECIMAL(20, 2), comment="融资性现金流-现金流量净额(万元)"
    )
    financing_cf_ratio_of_net_cf = Column[float](
        DECIMAL(10, 4), comment="融资性现金流-净现金流占比(%)"
    )

    __table_args__ = (
        Index(
            "idx_cash_flow_sheet_stock_period_type",
            "stock_code",
            "report_year",
            "report_period",
            "report_type",
        ),
        Index(
            "idx_cash_flow_sheet_abbr_period_type",
            "stock_abbr",
            "report_year",
            "report_period",
            "report_type",
        ),
        Index(
            "idx_cash_flow_sheet_period_type",
            "report_year",
            "report_period",
            "report_type",
        ),
    )
