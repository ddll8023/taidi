from sqlalchemy import Column, DECIMAL

from app.db.database import Base
from app.models.report_fact_base import (
    ReportFactIdentityMixin,
    build_report_fact_table_args,
)


class CorePerformanceIndicatorsSheet(ReportFactIdentityMixin, Base):
    __tablename__ = "core_performance_indicators_sheet"

    eps = Column(DECIMAL(10, 4), comment="每股收益(元)")
    total_operating_revenue = Column(DECIMAL(20, 2), comment="营业总收入(万元)")
    operating_revenue_yoy_growth = Column(DECIMAL(10, 4), comment="营业总收入-同比增长(%)")
    operating_revenue_qoq_growth = Column(DECIMAL(10, 4), comment="营业总收入-季度环比增长(%)")
    net_profit_10k_yuan = Column(DECIMAL(20, 2), comment="净利润(万元)")
    net_profit_yoy_growth = Column(DECIMAL(10, 4), comment="净利润-同比增长(%)")
    net_profit_qoq_growth = Column(DECIMAL(10, 4), comment="净利润-季度环比增长(%)")
    net_asset_per_share = Column(DECIMAL(10, 4), comment="每股净资产(元)")
    roe = Column(DECIMAL(10, 4), comment="净资产收益率(%)")
    operating_cf_per_share = Column(DECIMAL(10, 4), comment="每股经营现金流量(元)")
    net_profit_excl_non_recurring = Column(DECIMAL(20, 2), comment="扣非净利润（万元）")
    net_profit_excl_non_recurring_yoy = Column(DECIMAL(10, 4), comment="扣非净利润同比增长（%）")
    gross_profit_margin = Column(DECIMAL(10, 4), comment="销售毛利率(%)")
    net_profit_margin = Column(DECIMAL(10, 4), comment="销售净利率（%）")
    roe_weighted_excl_non_recurring = Column(DECIMAL(10, 4), comment="加权平均净资产收益率（扣非）（%）")
    __table_args__ = build_report_fact_table_args(__tablename__)
