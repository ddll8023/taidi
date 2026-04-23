from sqlalchemy import Column, DECIMAL

from app.db.database import Base
from app.models.report_fact_base import (
    ReportFactIdentityMixin,
    build_report_fact_table_args,
)


class CashFlowSheet(ReportFactIdentityMixin, Base):
    __tablename__ = "cash_flow_sheet"

    net_cash_flow = Column(DECIMAL(20, 2), comment="净现金流(元)")
    net_cash_flow_yoy_growth = Column(DECIMAL(10, 4), comment="净现金流-同比增长(%)")
    operating_cf_net_amount = Column(DECIMAL(20, 2), comment="经营性现金流-现金流量净额(万元)")
    operating_cf_ratio_of_net_cf = Column(DECIMAL(10, 4), comment="经营性现金流-净现金流占比(%)")
    operating_cf_cash_from_sales = Column(DECIMAL(20, 2), comment="经营性现金流-销售商品收到的现金（万元）")
    investing_cf_net_amount = Column(DECIMAL(20, 2), comment="投资性现金流-现金流量净额(万元)")
    investing_cf_ratio_of_net_cf = Column(DECIMAL(10, 4), comment="投资性现金流-净现金流占比(%)")
    investing_cf_cash_for_investments = Column(DECIMAL(20, 2), comment="投资性现金流-投资支付的现金（万元）")
    investing_cf_cash_from_investment_recovery = Column(DECIMAL(20, 2), comment="投资性现金流-收回投资收到的现金（万元）")
    financing_cf_cash_from_borrowing = Column(DECIMAL(20, 2), comment="融资性现金流-取得借款收到的现金（万元）")
    financing_cf_cash_for_debt_repayment = Column(DECIMAL(20, 2), comment="融资性现金流-偿还债务支付的现金（万元）")
    financing_cf_net_amount = Column(DECIMAL(20, 2), comment="融资性现金流-现金流量净额(万元)")
    financing_cf_ratio_of_net_cf = Column(DECIMAL(10, 4), comment="融资性现金流-净现金流占比(%)")
    __table_args__ = build_report_fact_table_args(__tablename__)
