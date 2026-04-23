from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.common import PaginatedResponse


# ========== 辅助类（Support）==========


class CorePerformanceIndicatorsData(BaseModel):
    """核心业绩指标表数据"""

    eps: Decimal | None = Field(None, description="每股收益(元)")
    total_operating_revenue: Decimal | None = Field(None, description="营业总收入(万元)")
    operating_revenue_yoy_growth: Decimal | None = Field(None, description="营业总收入-同比增长(%)")
    operating_revenue_qoq_growth: Decimal | None = Field(None, description="营业总收入-季度环比增长(%)")
    net_profit_10k_yuan: Decimal | None = Field(None, description="净利润(万元)")
    net_profit_yoy_growth: Decimal | None = Field(None, description="净利润-同比增长(%)")
    net_profit_qoq_growth: Decimal | None = Field(None, description="净利润-季度环比增长(%)")
    net_asset_per_share: Decimal | None = Field(None, description="每股净资产(元)")
    roe: Decimal | None = Field(None, description="净资产收益率(%)")
    operating_cf_per_share: Decimal | None = Field(None, description="每股经营现金流量(元)")
    net_profit_excl_non_recurring: Decimal | None = Field(None, description="扣非净利润（万元）")
    net_profit_excl_non_recurring_yoy: Decimal | None = Field(None, description="扣非净利润同比增长（%）")
    gross_profit_margin: Decimal | None = Field(None, description="销售毛利率(%)")
    net_profit_margin: Decimal | None = Field(None, description="销售净利率（%）")
    roe_weighted_excl_non_recurring: Decimal | None = Field(None, description="加权平均净资产收益率（扣非）（%）")

    model_config = ConfigDict(from_attributes=True)


class BalanceSheetData(BaseModel):
    """资产负债表数据"""

    asset_cash_and_cash_equivalents: Decimal | None = Field(None, description="资产-货币资金(万元)")
    asset_accounts_receivable: Decimal | None = Field(None, description="资产-应收账款(万元)")
    asset_inventory: Decimal | None = Field(None, description="资产-存货(万元)")
    asset_trading_financial_assets: Decimal | None = Field(None, description="资产-交易性金融资产（万元）")
    asset_construction_in_progress: Decimal | None = Field(None, description="资产-在建工程（万元）")
    asset_total_assets: Decimal | None = Field(None, description="资产-总资产(万元)")
    asset_total_assets_yoy_growth: Decimal | None = Field(None, description="资产-总资产同比(%)")
    liability_accounts_payable: Decimal | None = Field(None, description="负债-应付账款(万元)")
    liability_advance_from_customers: Decimal | None = Field(None, description="负债-预收账款(万元)")
    liability_total_liabilities: Decimal | None = Field(None, description="负债-总负债(万元)")
    liability_total_liabilities_yoy_growth: Decimal | None = Field(None, description="负债-总负债同比(%)")
    liability_contract_liabilities: Decimal | None = Field(None, description="负债-合同负债（万元）")
    liability_short_term_loans: Decimal | None = Field(None, description="负债-短期借款（万元）")
    asset_liability_ratio: Decimal | None = Field(None, description="资产负债率(%)")
    equity_unappropriated_profit: Decimal | None = Field(None, description="股东权益-未分配利润（万元）")
    equity_total_equity: Decimal | None = Field(None, description="股东权益合计(万元)")

    model_config = ConfigDict(from_attributes=True)


class CashFlowSheetData(BaseModel):
    """现金流量表数据"""

    net_cash_flow: Decimal | None = Field(None, description="净现金流(元)")
    net_cash_flow_yoy_growth: Decimal | None = Field(None, description="净现金流-同比增长(%)")
    operating_cf_net_amount: Decimal | None = Field(None, description="经营性现金流-现金流量净额(万元)")
    operating_cf_ratio_of_net_cf: Decimal | None = Field(None, description="经营性现金流-净现金流占比(%)")
    operating_cf_cash_from_sales: Decimal | None = Field(None, description="经营性现金流-销售商品收到的现金（万元）")
    investing_cf_net_amount: Decimal | None = Field(None, description="投资性现金流-现金流量净额(万元)")
    investing_cf_ratio_of_net_cf: Decimal | None = Field(None, description="投资性现金流-净现金流占比(%)")
    investing_cf_cash_for_investments: Decimal | None = Field(None, description="投资性现金流-投资支付的现金（万元）")
    investing_cf_cash_from_investment_recovery: Decimal | None = Field(None, description="投资性现金流-收回投资收到的现金（万元）")
    financing_cf_cash_from_borrowing: Decimal | None = Field(None, description="融资性现金流-取得借款收到的现金（万元）")
    financing_cf_cash_for_debt_repayment: Decimal | None = Field(None, description="融资性现金流-偿还债务支付的现金（万元）")
    financing_cf_net_amount: Decimal | None = Field(None, description="融资性现金流-现金流量净额(万元)")
    financing_cf_ratio_of_net_cf: Decimal | None = Field(None, description="融资性现金流-净现金流占比(%)")

    model_config = ConfigDict(from_attributes=True)


class IncomeSheetData(BaseModel):
    """利润表数据"""

    net_profit: Decimal | None = Field(None, description="净利润(万元)")
    net_profit_yoy_growth: Decimal | None = Field(None, description="净利润同比(%)")
    other_income: Decimal | None = Field(None, description="其他收益（万元）")
    total_operating_revenue: Decimal | None = Field(None, description="营业总收入(万元)")
    operating_revenue_yoy_growth: Decimal | None = Field(None, description="营业总收入同比(%)")
    operating_expense_cost_of_sales: Decimal | None = Field(None, description="营业总支出-营业支出(万元)")
    operating_expense_selling_expenses: Decimal | None = Field(None, description="营业总支出-销售费用(万元)")
    operating_expense_administrative_expenses: Decimal | None = Field(None, description="营业总支出-管理费用(万元)")
    operating_expense_financial_expenses: Decimal | None = Field(None, description="营业总支出-财务费用(万元)")
    operating_expense_rnd_expenses: Decimal | None = Field(None, description="营业总支出-研发费用（万元）")
    operating_expense_taxes_and_surcharges: Decimal | None = Field(None, description="营业总支出-税金及附加（万元）")
    total_operating_expenses: Decimal | None = Field(None, description="营业总支出(万元)")
    operating_profit: Decimal | None = Field(None, description="营业利润(万元)")
    total_profit: Decimal | None = Field(None, description="利润总额(万元)")
    asset_impairment_loss: Decimal | None = Field(None, description="资产减值损失（万元）")
    credit_impairment_loss: Decimal | None = Field(None, description="信用减值损失（万元）")

    model_config = ConfigDict(from_attributes=True)


# ========== 请求类（Request）==========  # 入参校验


class BatchParseRequest(BaseModel):
    """批量解析请求"""
    report_ids: list[int] = Field(..., description="待解析的报告ID列表")


class BatchStatusRequest(BaseModel):
    """批量状态查询请求"""
    report_ids: list[int] = Field(..., description="报告ID列表")


class DataListRequest(BaseModel):
    """数据列表请求"""

    page: int = Field(1, ge=1)
    page_size: int = Field(10, ge=10)
    stock_code: Annotated[str, Field(max_length=6)] | None = None
    stock_abbr: str | None = None
    report_year: int | None = Field(None, ge=2000, le=2100)
    report_period: str | None = None
    report_type: str | None = None
    import_status: int | None = Field(None)
    keyword: str | None = Field(None, description="文件名关键词搜索")
    parse_status: int | None = Field(None, description="解析状态：0待处理 1成功 2失败")
    vector_status: int | None = Field(None, description="向量化状态：0待向量化 1向量化中 2成功 3失败 4跳过")
    sort_by: str | None = Field(None, description="排序字段：created_at(创建时间), updated_at(更新时间)")
    sort_order: str | None = Field(None, description="排序方式：desc(倒序), asc(正序)")


# ========== 响应类（Response）==========  # 返回数据结构


class FinancialReportItem(BaseModel):
    """财报记录列表项"""

    id: int
    file_name: str = Field(description="源文件名")
    report_title: str = Field(description="报告标题")
    stock_code: str = Field(max_length=6)
    stock_abbr: str
    report_year: int
    report_period: str = Field(description="Q1/HY/Q3/FY")
    report_type: str = Field(description="REPORT/SUMMARY")
    parse_status: int = Field(description="解析状态：0待处理 1成功 2失败")
    import_status: int = Field(description="入库状态：0待入库 1成功 2失败")
    vector_status: int = Field(
        description="向量化状态：0待向量化 1向量化中 2成功 3失败 4跳过"
    )
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class FinancialReportDetail(BaseModel):
    """财报详情"""

    id: int
    file_name: str = Field(description="源文件名")
    report_title: str = Field(description="报告标题")
    stock_code: str = Field(max_length=6)
    stock_abbr: str
    report_year: int
    report_period: str = Field(description="Q1/HY/Q3/FY")
    report_type: str = Field(description="REPORT/SUMMARY")
    report_label: str = Field(description="报告标签")
    exchange: str = Field(description="交易所标识")
    report_date: datetime | None = Field(None, description="报告披露日期")
    parse_status: int = Field(description="解析状态：0待处理 1成功 2失败")
    review_status: int = Field(description="审核状态：0待审核 1已通过 2已驳回")
    validate_status: int = Field(description="校验状态：0待校验 1已通过 2已失败")
    validate_message: str | None = Field(None, description="校验结果说明")
    import_status: int = Field(description="入库状态：0待入库 1成功 2失败")
    vector_status: int = Field(description="向量化状态：0待向量化 1向量化中 2成功 3失败 4跳过")
    vector_model: str | None = Field(None, description="向量模型")
    vector_dim: int | None = Field(None, description="向量维度")
    vector_version: str | None = Field(None, description="向量版本")
    vector_error_message: str | None = Field(None, description="向量化失败原因")
    vectorized_at: datetime | None = Field(None, description="向量化完成时间")
    created_at: datetime
    updated_at: datetime
    core_performance_indicators: CorePerformanceIndicatorsData | None = Field(None, description="核心业绩指标表")
    balance_sheet: BalanceSheetData | None = Field(None, description="资产负债表")
    cash_flow_sheet: CashFlowSheetData | None = Field(None, description="现金流量表")
    income_sheet: IncomeSheetData | None = Field(None, description="利润表")

    model_config = ConfigDict(from_attributes=True)


class FinancialReportArchiveResponse(BaseModel):
    """上传建档响应"""
    report_id: int
    stock_code: str
    stock_abbr: str
    report_title: str
    parse_status: int
    message: str = "文件上传成功，请稍后执行解析"


class BatchUploadResponse(BaseModel):
    """批量上传响应"""
    total: int
    success_count: int
    failed_count: int
    success_reports: list[dict]
    failed_files: list[dict]


class SingleParseSubmitResponse(BaseModel):
    """单个解析提交响应"""
    report_id: int
    status: str
    message: str


class BatchParseSubmitResponse(BaseModel):
    """批量解析提交响应"""
    submitted_count: int
    skipped_count: int
    submitted_report_ids: list[int]
    skipped_report_ids: list[int]
    message: str


class BatchParseStatusItem(BaseModel):
    """单个报告解析状态"""
    parse_status: int
    parse_status_text: str
    validate_message: str | None = None


class BatchParseStatusResponse(BaseModel):
    """批量解析状态查询响应"""
    results: dict[int, BatchParseStatusItem]
    processing_count: int
    completed_count: int
    total_count: int


class FinancialReportDeleteResponse(BaseModel):
    """删除响应"""
    id: int


class JsonContentResponse(BaseModel):
    """JSON文件内容响应"""
    file_name: str
    file_size: int
    content: Any
