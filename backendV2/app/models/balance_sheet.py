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


class BalanceSheet(Base):
    __tablename__ = "balance_sheet"

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
    asset_cash_and_cash_equivalents = Column[float](
        DECIMAL(20, 2), comment="资产-货币资金(万元)"
    )
    asset_accounts_receivable = Column[float](
        DECIMAL(20, 2), comment="资产-应收账款(万元)"
    )
    asset_inventory = Column[float](DECIMAL(20, 2), comment="资产-存货(万元)")
    asset_trading_financial_assets = Column[float](
        DECIMAL(20, 2), comment="资产-交易性金融资产（万元）"
    )
    asset_construction_in_progress = Column[float](
        DECIMAL(20, 2), comment="资产-在建工程（万元）"
    )
    asset_total_assets = Column[float](DECIMAL(20, 2), comment="资产-总资产(万元)")
    asset_total_assets_yoy_growth = Column[float](
        DECIMAL(10, 4), comment="资产-总资产同比(%)"
    )
    liability_accounts_payable = Column[float](
        DECIMAL(20, 2), comment="负债-应付账款(万元)"
    )
    liability_advance_from_customers = Column[float](
        DECIMAL(20, 2), comment="负债-预收账款(万元)"
    )
    liability_total_liabilities = Column[float](
        DECIMAL(20, 2), comment="负债-总负债(万元)"
    )
    liability_total_liabilities_yoy_growth = Column[float](
        DECIMAL(10, 4), comment="负债-总负债同比(%)"
    )
    liability_contract_liabilities = Column[float](
        DECIMAL(20, 2), comment="负债-合同负债（万元）"
    )
    liability_short_term_loans = Column[float](
        DECIMAL(20, 2), comment="负债-短期借款（万元）"
    )
    asset_liability_ratio = Column[float](DECIMAL(10, 4), comment="资产负债率(%)")
    equity_unappropriated_profit = Column[float](
        DECIMAL(20, 2), comment="股东权益-未分配利润（万元）"
    )
    equity_total_equity = Column[float](DECIMAL(20, 2), comment="股东权益合计(万元)")

    __table_args__ = (
        Index(
            "idx_balance_sheet_stock_period_type",
            "stock_code",
            "report_year",
            "report_period",
            "report_type",
        ),
        Index(
            "idx_balance_sheet_abbr_period_type",
            "stock_abbr",
            "report_year",
            "report_period",
            "report_type",
        ),
        Index(
            "idx_balance_sheet_period_type",
            "report_year",
            "report_period",
            "report_type",
        ),
    )
