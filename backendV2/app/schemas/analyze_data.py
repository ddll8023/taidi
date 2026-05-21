from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from app.constants.financial_report_base_info import (
    ExchangeEnum,
    ReportPeriodEnum,
    ReportTypeEnum,
)


# ========== 辅助类（Support）==========


class SuccessReportItem(BaseModel):
    """成功建档的财报记录项"""

    report_id: int = Field(..., description="财报记录ID")
    stock_code: str = Field(..., description="股票简称")
    stock_abbr: str = Field(..., description="股票简称")
    report_title: str = Field(..., description="财报标题")
    file_name: str = Field(..., description="文件名")

    model_config = ConfigDict(from_attributes=True)


class FailedFileItem(BaseModel):
    """失败文件项"""

    file_name: str = Field(..., description="文件名")
    error: str = Field(..., description="错误信息")

    model_config = ConfigDict(from_attributes=True)


class FileMetadataItem(BaseModel):
    """文件元数据项"""

    stock_code: str = Field(..., min_length=6, max_length=6, description="股票代码")
    report_year: int = Field(..., description="报告年份")
    report_period: ReportPeriodEnum = Field(..., description="报告期间")
    report_type: ReportTypeEnum = Field(..., description="报告类型")
    report_label: str = Field(..., description="报告中文标签")
    report_title: str = Field(..., description="财报标题")
    report_date: date | None = Field(None, description="报告日期")

    model_config = ConfigDict(from_attributes=True)


class StructCorePerformanceIndicatorsSheetItem(BaseModel):
    """核心指标表模型结果项"""

    eps: float | None = Field(None, description="每股收益(元)")
    total_operating_revenue: float | None = Field(None, description="营业总收入(万元)")
    operating_revenue_yoy_growth: float | None = Field(
        None, description="营业总收入-同比增长(%)"
    )
    operating_revenue_qoq_growth: float | None = Field(
        None, description="营业总收入-季度环比增长(%)"
    )
    net_profit_10k_yuan: float | None = Field(None, description="净利润(万元)")
    net_profit_yoy_growth: float | None = Field(None, description="净利润-同比增长(%)")
    net_profit_qoq_growth: float | None = Field(
        None, description="净利润-季度环比增长(%)"
    )
    roe: float | None = Field(None, description="净资产收益率(%)")
    net_asset_per_share: float | None = Field(None, description="每股净资产(元)")
    roe: float | None = Field(None, description="净资产收益率(%)")
    operating_cf_per_share: float | None = Field(
        None, description="每股经营现金流量(元)"
    )
    net_profit_excl_non_recurring: float | None = Field(
        None, description="扣非净利润（万元）"
    )
    net_profit_excl_non_recurring_yoy: float | None = Field(
        None, description="扣非净利润同比增长(%)"
    )
    gross_profit_margin: float | None = Field(None, description="销售毛利率(%)")
    net_profit_margin: float | None = Field(None, description="销售净利率(%)")
    roe_weighted_excl_non_recurring: float | None = Field(
        None, description="加权平均净资产收益率（扣非）(%)"
    )

    model_config = ConfigDict(from_attributes=True)


class StructBalanceSheetItem(BaseModel):
    """资产负债表模型结果项"""

    asset_cash_and_cash_equivalents: float | None = Field(
        None, description="资产-货币资金(万元)"
    )
    asset_accounts_receivable: float | None = Field(
        None, description="资产-应收账款(万元)"
    )
    asset_inventory: float | None = Field(None, description="资产-存货(万元)")
    asset_trading_financial_assets: float | None = Field(
        None, description="资产-交易性金融资产(万元)"
    )
    asset_construction_in_progress: float | None = Field(
        None, description="资产-在建工程(万元)"
    )
    asset_total_assets: float | None = Field(None, description="资产-总资产(万元)")
    asset_total_assets_yoy_growth: float | None = Field(
        None, description="资产-总资产同比(%)"
    )
    liability_accounts_payable: float | None = Field(
        None, description="负债-应付账款(万元)"
    )
    liability_advance_from_customers: float | None = Field(
        None, description="负债-预收账款(万元)"
    )
    liability_total_liabilities: float | None = Field(
        None, description="负债-总负债(万元)"
    )
    liability_total_liabilities_yoy_growth: float | None = Field(
        None, description="负债-总负债同比(%)"
    )
    liability_contract_liabilities: float | None = Field(
        None, description="负债-合同负债(万元)"
    )
    liability_short_term_loans: float | None = Field(
        None, description="负债-短期借款(万元)"
    )
    asset_liability_ratio: float | None = Field(None, description="资产负债负债率(%)")
    equity_unappropriated_profit: float | None = Field(
        None, description="股东权益-未分配利润(万元)"
    )
    equity_total_equity: float | None = Field(None, description="股东权益合计(万元)")

    model_config = ConfigDict(from_attributes=True)


class StructCashFlowSheetItem(BaseModel):
    """现金流量表模型结果项"""

    net_cash_flow: float | None = Field(None, description="净现金流(元)")
    net_cash_flow_yoy_growth: float | None = Field(
        None, description="净现金流-同比增长(%)"
    )
    operating_cf_net_amount: float | None = Field(
        None, description="经营活动-现金流量净额(万元)"
    )
    operating_cf_ratio_of_net_cf: float | None = Field(
        None, description="经营活动-净现金流占比(%)"
    )
    operating_cf_cash_from_sales: float | None = Field(
        None, description="经营活动-销售商品收到的现金（万元）"
    )
    investing_cf_net_amount: float | None = Field(
        None, description="投资性-现金流量净额(万元)"
    )
    investing_cf_ratio_of_net_cf: float | None = Field(
        None, description="投资性-净现金流占比(%)"
    )
    investing_cf_cash_for_investments: float | None = Field(
        None, description="投资性-投资支付的现金(万元)"
    )
    investing_cf_cash_from_investment_recovery: float | None = Field(
        None, description="投资性-收回投资收到的现金(万元)"
    )
    financing_cf_cash_from_borrowing: float | None = Field(
        None, description="融资性-取得借款收到的现金(万元)"
    )
    financing_cf_cash_for_debt_repayment: float | None = Field(
        None, description="融资性-偿还债务支付的现金(万元)"
    )
    financing_cf_net_amount: float | None = Field(
        None, description="融资性-现金流量净额(万元)"
    )
    financing_cf_ratio_of_net_cf: float | None = Field(
        None, description="融资性-净现金流占比(%)"
    )

    model_config = ConfigDict(from_attributes=True)


class StructIncomeSheetItem(BaseModel):
    """利润表表模型结果项"""

    net_profit: float | None = Field(None, description="净利润(万元)")
    net_profit_yoy_growth: float | None = Field(None, description="净利润同比(%)")
    other_income: float | None = Field(None, description="其他收益（万元）")
    total_operating_revenue: float | None = Field(None, description="营业总收入(万元)")
    operating_revenue_yoy_growth: float | None = Field(
        None, description="营业总收入同比(%)"
    )
    operating_expense_cost_of_sales: float | None = Field(
        None, description="营业总支出-营业支出(万元)"
    )
    operating_expense_selling_expenses: float | None = Field(
        None, description="营业总支出-销售费用(万元)"
    )
    operating_expense_administrative_expenses: float | None = Field(
        None, description="营业总支出-管理费用(万元)"
    )
    operating_expense_financial_expenses: float | None = Field(
        None, description="营业总支出-财务费用(万元)"
    )
    operating_expense_rnd_expenses: float | None = Field(
        None, description="营业总支出-研发费用(万元)"
    )
    operating_expense_taxes_and_surcharges: float | None = Field(
        None, description="营业总支出-税金及附加(万元)"
    )
    total_operating_expenses: float | None = Field(None, description="营业总支出(万元)")
    operating_profit: float | None = Field(None, description="营业利润(万元)")
    total_profit: float | None = Field(None, description="利润总额(万元)")
    asset_impairment_loss: float | None = Field(None, description="资产减值损失(万元)")
    credit_impairment_loss: float | None = Field(None, description="信用减值损失(万元)")

    model_config = ConfigDict(from_attributes=True)


## ========== 请求类（Request）==========


class GetReportListRequest(BaseModel):
    """获取财报列表请求"""

    page: int = Field(1, ge=1, description="页码")
    page_size: int = Field(10, ge=10, description="每页数量")
    keyword: str | None = Field(None, description="报告标题关键词搜索")
    report_type: ReportTypeEnum | None = Field(None, description="报告类型筛选")
    report_year: int | None = Field(None, description="报告年份筛选")
    parse_status: Literal[0, 1, 2, 3] | None = Field(
        None, description="解析状态筛选：0 待处理 / 1 成功 / 2 失败 / 3 解析中"
    )
    import_status: Literal[0, 1, 2] | None = Field(
        None, description="入库状态筛选：0 待入库 / 1 成功 / 2 失败"
    )
    sort_by: Literal["created_at", "updated_at"] | None = Field(
        "updated_at", description="排序字段：`created_at` / `updated_at`"
    )
    sort_order: Literal["desc", "asc"] | None = Field(
        "desc", description="排序方式：`desc` / `asc`"
    )


class ParseReportRequest(BaseModel):
    """解析财报请求"""

    report_ids: list[int] = Field(..., default_factory=list, description="财报记录ID")


# ========== 响应类（Response）==========


class UploadFileResponse(BaseModel):
    """上传财报PDF响应"""

    total: int = Field(..., description="上传文件总数")
    success_count: int = Field(..., description="成功建档数量")
    failed_count: int = Field(..., description="失败数量")
    success_reports: list[SuccessReportItem] = Field(
        ..., default_factory=list, description="成功建档的财报记录列表"
    )
    failed_files: list[FailedFileItem] = Field(
        ..., default_factory=list, description="失败文件列表"
    )

    model_config = ConfigDict(from_attributes=True)


class GetReportListResponse(BaseModel):
    """获取财报列表响应"""

    id: int = Field(..., description="财报记录ID")
    file_name: str = Field(
        ..., validation_alias="source_file_name", description="文件名"
    )
    report_title: str = Field(..., description="财报标题")
    stock_code: str = Field(..., description="股票代码")
    stock_abbr: str = Field(..., description="股票简称")
    report_year: int = Field(..., description="报告年份")
    report_period: ReportPeriodEnum = Field(..., description="报告期间")
    report_type: ReportTypeEnum = Field(..., description="报告类型")
    parse_status: Literal[0, 1, 2, 3] = Field(
        ..., description="解析状态, 0 待处理 / 1 成功 / 2 失败 / 3 解析中"
    )
    import_status: Literal[0, 1, 2] = Field(
        ..., description="入库状态, 0 待入库 / 1 成功 / 2 失败"
    )
    created_at: datetime = Field(..., description="创建时间")

    model_config = ConfigDict(from_attributes=True)


class ParseReportResponse(BaseModel):
    """解析财报响应"""

    total: int = Field(0, description="解析文件总数")
    start_parse_count: int = Field(0, description="开始解析的文件数量")
    skip_report_ids: list[dict[int, str]] = Field(
        ..., default_factory=list, description="跳过的财报记录列表以及跳过原因"
    )

    model_config = ConfigDict(from_attributes=True)
