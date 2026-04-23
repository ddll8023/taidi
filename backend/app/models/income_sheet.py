from sqlalchemy import Column, DECIMAL

from app.db.database import Base
from app.models.report_fact_base import (
    ReportFactIdentityMixin,
    build_report_fact_table_args,
)


class IncomeSheet(ReportFactIdentityMixin, Base):
    __tablename__ = "income_sheet"

    net_profit = Column(DECIMAL(20, 2), comment="净利润(万元)")
    net_profit_yoy_growth = Column(DECIMAL(10, 4), comment="净利润同比(%)")
    other_income = Column(DECIMAL(20, 2), comment="其他收益（万元）")
    total_operating_revenue = Column(DECIMAL(20, 2), comment="营业总收入(万元)")
    operating_revenue_yoy_growth = Column(DECIMAL(10, 4), comment="营业总收入同比(%)")
    operating_expense_cost_of_sales = Column(DECIMAL(20, 2), comment="营业总支出-营业支出(万元)")
    operating_expense_selling_expenses = Column(DECIMAL(20, 2), comment="营业总支出-销售费用(万元)")
    operating_expense_administrative_expenses = Column(DECIMAL(20, 2), comment="营业总支出-管理费用(万元)")
    operating_expense_financial_expenses = Column(DECIMAL(20, 2), comment="营业总支出-财务费用(万元)")
    operating_expense_rnd_expenses = Column(DECIMAL(20, 2), comment="营业总支出-研发费用（万元）")
    operating_expense_taxes_and_surcharges = Column(DECIMAL(20, 2), comment="营业总支出-税金及附加（万元）")
    total_operating_expenses = Column(DECIMAL(20, 2), comment="营业总支出(万元)")
    operating_profit = Column(DECIMAL(20, 2), comment="营业利润(万元)")
    total_profit = Column(DECIMAL(20, 2), comment="利润总额(万元)")
    asset_impairment_loss = Column(DECIMAL(20, 2), comment="资产减值损失（万元）")
    credit_impairment_loss = Column(DECIMAL(20, 2), comment="信用减值损失（万元）")
    __table_args__ = build_report_fact_table_args(__tablename__)
