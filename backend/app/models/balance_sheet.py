from sqlalchemy import Column, DECIMAL

from app.db.database import Base
from app.models.report_fact_base import (
    ReportFactIdentityMixin,
    build_report_fact_table_args,
)


class BalanceSheet(ReportFactIdentityMixin, Base):
    __tablename__ = "balance_sheet"

    asset_cash_and_cash_equivalents = Column(DECIMAL(20, 2), comment="资产-货币资金(万元)")
    asset_accounts_receivable = Column(DECIMAL(20, 2), comment="资产-应收账款(万元)")
    asset_inventory = Column(DECIMAL(20, 2), comment="资产-存货(万元)")
    asset_trading_financial_assets = Column(DECIMAL(20, 2), comment="资产-交易性金融资产（万元）")
    asset_construction_in_progress = Column(DECIMAL(20, 2), comment="资产-在建工程（万元）")
    asset_total_assets = Column(DECIMAL(20, 2), comment="资产-总资产(万元)")
    asset_total_assets_yoy_growth = Column(DECIMAL(10, 4), comment="资产-总资产同比(%)")
    liability_accounts_payable = Column(DECIMAL(20, 2), comment="负债-应付账款(万元)")
    liability_advance_from_customers = Column(DECIMAL(20, 2), comment="负债-预收账款(万元)")
    liability_total_liabilities = Column(DECIMAL(20, 2), comment="负债-总负债(万元)")
    liability_total_liabilities_yoy_growth = Column(DECIMAL(10, 4), comment="负债-总负债同比(%)")
    liability_contract_liabilities = Column(DECIMAL(20, 2), comment="负债-合同负债（万元）")
    liability_short_term_loans = Column(DECIMAL(20, 2), comment="负债-短期借款（万元）")
    asset_liability_ratio = Column(DECIMAL(10, 4), comment="资产负债率(%)")
    equity_unappropriated_profit = Column(DECIMAL(20, 2), comment="股东权益-未分配利润（万元）")
    equity_total_equity = Column(DECIMAL(20, 2), comment="股东权益合计(万元)")
    __table_args__ = build_report_fact_table_args(__tablename__)
