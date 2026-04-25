"""事实表模型映射与字段常量定义"""

from app.models import balance_sheet as models_balance_sheet
from app.models import cash_flow_sheet as models_cash_flow_sheet
from app.models import (
    core_performance_indicators_sheet as models_core_performance_indicators_sheet,
)
from app.models import income_sheet as models_income_sheet

UNSET = object()

MAX_CONCURRENT_PARSES = 5

FACT_MODEL_MAP = {
    "core_performance_indicators_sheet": models_core_performance_indicators_sheet.CorePerformanceIndicatorsSheet,
    "balance_sheet": models_balance_sheet.BalanceSheet,
    "cash_flow_sheet": models_cash_flow_sheet.CashFlowSheet,
    "income_sheet": models_income_sheet.IncomeSheet,
}

FACT_IDENTITY_FIELDS = frozenset(
    {
        "report_id",
        "stock_code",
        "stock_abbr",
        "report_year",
        "report_period",
        "report_type",
    }
)

FACT_MODEL_COLUMNS = {
    table_name: tuple(
        column
        for column in model_class.__table__.columns
        if column.name not in FACT_IDENTITY_FIELDS
    )
    for table_name, model_class in FACT_MODEL_MAP.items()
}

FACT_MODEL_FIELD_SET = {
    table_name: frozenset(column.name for column in columns)
    for table_name, columns in FACT_MODEL_COLUMNS.items()
}
