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


class IncomeSheet(Base):
    __tablename__ = "income_sheet"

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
    net_profit = Column[float](DECIMAL(20, 2), comment="净利润(万元)")
    net_profit_yoy_growth = Column[float](DECIMAL(10, 4), comment="净利润同比(%)")
    other_income = Column[float](DECIMAL(20, 2), comment="其他收益（万元）")
    total_operating_revenue = Column[float](
        DECIMAL(20, 2), comment="营业总收入(万元)"
    )
    operating_revenue_yoy_growth = Column[float](
        DECIMAL(10, 4), comment="营业总收入同比(%)"
    )
    operating_expense_cost_of_sales = Column[float](
        DECIMAL(20, 2), comment="营业总支出-营业支出(万元)"
    )
    operating_expense_selling_expenses = Column[float](
        DECIMAL(20, 2), comment="营业总支出-销售费用(万元)"
    )
    operating_expense_administrative_expenses = Column[float](
        DECIMAL(20, 2), comment="营业总支出-管理费用(万元)"
    )
    operating_expense_financial_expenses = Column[float](
        DECIMAL(20, 2), comment="营业总支出-财务费用(万元)"
    )
    operating_expense_rnd_expenses = Column[float](
        DECIMAL(20, 2), comment="营业总支出-研发费用（万元）"
    )
    operating_expense_taxes_and_surcharges = Column[float](
        DECIMAL(20, 2), comment="营业总支出-税金及附加（万元）"
    )
    total_operating_expenses = Column[float](
        DECIMAL(20, 2), comment="营业总支出(万元)"
    )
    operating_profit = Column[float](DECIMAL(20, 2), comment="营业利润(万元)")
    total_profit = Column[float](DECIMAL(20, 2), comment="利润总额(万元)")
    asset_impairment_loss = Column[float](
        DECIMAL(20, 2), comment="资产减值损失（万元）"
    )
    credit_impairment_loss = Column[float](
        DECIMAL(20, 2), comment="信用减值损失（万元）"
    )

    __table_args__ = (
        Index(
            "idx_income_sheet_stock_period_type",
            "stock_code",
            "report_year",
            "report_period",
            "report_type",
        ),
        Index(
            "idx_income_sheet_abbr_period_type",
            "stock_abbr",
            "report_year",
            "report_period",
            "report_type",
        ),
        Index(
            "idx_income_sheet_period_type",
            "report_year",
            "report_period",
            "report_type",
        ),
    )
