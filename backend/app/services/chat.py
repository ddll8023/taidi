import json
import os
import re
import uuid
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import yaml
from langchain_core.messages import HumanMessage, SystemMessage
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.chat_message import ChatMessage
from app.models.chat_session import ChatSession
from app.models.company_basic_info import CompanyBasicInfo
from app.schemas.chat import (
    AnswerContent,
    ChatMessageResponse,
    ChatResponse,
    ChatSessionResponse,
    DerivedMetricType,
    IntentResult,
    QueryCapability,
    QueryType,
)
from app.schemas.common import ErrorCode, PaginatedResponse, PaginationInfo
from app.schemas.response import error
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.utils.model_factory import get_model

logger = setup_logger(__name__)

FORBIDDEN_KEYWORDS = [
    "DROP",
    "DELETE",
    "UPDATE",
    "INSERT",
    "ALTER",
    "CREATE",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
]
ALLOWED_TABLES = [
    "income_sheet",
    "balance_sheet",
    "cash_flow_sheet",
    "core_performance_indicators_sheet",
    "company_basic_info",
]


def _build_allowed_column_names() -> set[str]:
    """从 SCHEMA_INFO 构建所有允许的列名集合"""
    column_names: set[str] = set()
    for table_name, table_info in SCHEMA_INFO.items():
        if "identity_fields" in table_info:
            column_names.update(table_info["identity_fields"])
        if "metric_fields" in table_info:
            column_names.update(table_info["metric_fields"].keys())
        if "fields" in table_info:
            column_names.update(table_info["fields"].keys())
    return column_names


def _extract_select_columns(sql: str) -> list[str]:
    """从 SELECT 子句中提取所有列名（不含表别名前缀）"""
    sql_stripped = re.sub(r"--.*$", "", sql.strip(), flags=re.MULTILINE)

    def _find_top_level_keyword(statement: str, keyword: str, start: int = 0) -> int:
        upper_statement = statement.upper()
        keyword_upper = keyword.upper()
        keyword_len = len(keyword_upper)
        depth = 0
        in_single_quote = False
        in_double_quote = False
        i = start

        while i < len(statement):
            char = statement[i]

            if in_single_quote:
                if char == "'" and statement[i + 1 : i + 2] == "'":
                    i += 2
                    continue
                if char == "'":
                    in_single_quote = False
            elif in_double_quote:
                if char == '"' and statement[i + 1 : i + 2] == '"':
                    i += 2
                    continue
                if char == '"':
                    in_double_quote = False
            else:
                if char == "'":
                    in_single_quote = True
                elif char == '"':
                    in_double_quote = True
                elif char == "(":
                    depth += 1
                elif char == ")" and depth > 0:
                    depth -= 1
                elif depth == 0 and upper_statement.startswith(keyword_upper, i):
                    prev_char = statement[i - 1] if i > 0 else " "
                    next_index = i + keyword_len
                    next_char = statement[next_index] if next_index < len(statement) else " "
                    if not (prev_char.isalnum() or prev_char == "_") and not (
                        next_char.isalnum() or next_char == "_"
                    ):
                        return i

            i += 1

        return -1

    select_index = _find_top_level_keyword(sql_stripped, "SELECT")
    if select_index < 0:
        return []

    from_index = _find_top_level_keyword(sql_stripped, "FROM", start=select_index + 6)
    if from_index < 0:
        select_part = sql_stripped[select_index + 6 :]
    else:
        select_part = sql_stripped[select_index + 6 : from_index]

    columns: list[str] = []
    depth = 0
    in_single_quote = False
    in_double_quote = False
    token_start = 0
    i = 0

    while i < len(select_part):
        c = select_part[i]

        if in_single_quote:
            if c == "'":
                in_single_quote = False
        elif in_double_quote:
            if c == '"':
                in_double_quote = False
        else:
            if c == "'":
                in_single_quote = True
            elif c == '"':
                in_double_quote = True
            elif c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif depth == 0 and c == ",":
                token = select_part[token_start:i].strip()
                if token:
                    columns.append(token)
                token_start = i + 1
        i += 1

    tail = select_part[token_start:].strip()
    if tail:
        columns.append(tail)

    return columns


SQL_RESERVED_IDENTIFIERS = {
    "ALL",
    "AND",
    "AS",
    "ASC",
    "BETWEEN",
    "BY",
    "CASE",
    "CAST",
    "CROSS",
    "DESC",
    "DISTINCT",
    "ELSE",
    "END",
    "EXISTS",
    "FROM",
    "FULL",
    "GROUP",
    "HAVING",
    "IN",
    "INNER",
    "IS",
    "JOIN",
    "LEFT",
    "LIKE",
    "LIMIT",
    "NOT",
    "NULL",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "OVER",
    "PARTITION",
    "RIGHT",
    "ROWS",
    "SELECT",
    "THEN",
    "UNBOUNDED",
    "UNION",
    "WHEN",
    "WHERE",
    "WITH",
}


def _extract_column_refs_from_expression(expression: str) -> list[str]:
    """从单个SELECT表达式中提取真实字段名，忽略函数名、关键字和别名。"""
    expression_without_alias = re.sub(
        r"\s+AS\s+(?:`[^`]+`|\"[^\"]+\"|'[^']+'|[^\s,]+)\s*$",
        "",
        expression.strip(),
        flags=re.IGNORECASE,
    )
    expression_clean = re.sub(r"'(?:''|[^'])*'", " ", expression_without_alias)
    expression_clean = re.sub(r'"(?:\"\"|[^\"])*"', " ", expression_clean)

    refs: list[str] = []
    seen_refs: set[str] = set()
    for match in re.finditer(
        r"(?:\b([A-Za-z_]\w*)\s*\.\s*)?\b([A-Za-z_]\w*)\b",
        expression_clean,
    ):
        candidate = match.group(2)
        next_char = expression_clean[match.end() : match.end() + 1]
        if next_char == "(":
            continue

        if candidate.upper() in SQL_RESERVED_IDENTIFIERS:
            continue

        if candidate not in seen_refs:
            refs.append(candidate)
            seen_refs.add(candidate)

    return refs


def _extract_column_refs_from_select(columns: list[str]) -> list[str]:
    """从SELECT列表中提取字段引用，用于白名单校验。"""
    bare_refs: list[str] = []
    seen_refs: set[str] = set()
    for col in columns:
        for ref in _extract_column_refs_from_expression(col):
            if ref not in seen_refs:
                bare_refs.append(ref)
                seen_refs.add(ref)

    return bare_refs


def _extract_declared_column_aliases(sql: str) -> set[str]:
    """提取 SQL 中通过 AS 声明的列别名，允许后续 SELECT 引用这些派生列。"""
    aliases: set[str] = set()
    for match in re.finditer(
        r"""(?ix)
        \bAS\s+
        (?:
            `([^`]+)` |
            "([^"]+)" |
            '([^']+)' |
            ([A-Za-z_]\w*)
        )
        """,
        sql,
    ):
        alias = next((group for group in match.groups() if group), "")
        if alias and alias.upper() not in SQL_RESERVED_IDENTIFIERS:
            aliases.add(alias.lower())

    return aliases


SCHEMA_INFO = {
    "income_sheet": {
        "identity_fields": [
            "report_id",
            "stock_code",
            "stock_abbr",
            "report_year",
            "report_period",
            "report_type",
        ],
        "metric_fields": {
            "net_profit": "净利润(万元)",
            "net_profit_yoy_growth": "净利润同比(%)",
            "other_income": "其他收益（万元）",
            "total_operating_revenue": "营业总收入(万元)",
            "operating_revenue_yoy_growth": "营业总收入同比(%)",
            "operating_expense_cost_of_sales": "营业总支出-营业支出(万元)",
            "operating_expense_selling_expenses": "营业总支出-销售费用(万元)",
            "operating_expense_administrative_expenses": "营业总支出-管理费用(万元)",
            "operating_expense_financial_expenses": "营业总支出-财务费用(万元)",
            "operating_expense_rnd_expenses": "营业总支出-研发费用（万元）",
            "operating_expense_taxes_and_surcharges": "营业总支出-税金及附加（万元）",
            "total_operating_expenses": "营业总支出(万元)",
            "operating_profit": "营业利润(万元)",
            "total_profit": "利润总额(万元)",
            "asset_impairment_loss": "资产减值损失（万元）",
            "credit_impairment_loss": "信用减值损失（万元）",
        },
    },
    "balance_sheet": {
        "identity_fields": [
            "report_id",
            "stock_code",
            "stock_abbr",
            "report_year",
            "report_period",
            "report_type",
        ],
        "metric_fields": {
            "asset_cash_and_cash_equivalents": "资产-货币资金(万元)",
            "asset_accounts_receivable": "资产-应收账款(万元)",
            "asset_inventory": "资产-存货(万元)",
            "asset_trading_financial_assets": "资产-交易性金融资产（万元）",
            "asset_construction_in_progress": "资产-在建工程（万元）",
            "asset_total_assets": "资产-总资产(万元)",
            "asset_total_assets_yoy_growth": "资产-总资产同比(%)",
            "liability_accounts_payable": "负债-应付账款(万元)",
            "liability_advance_from_customers": "负债-预收账款(万元)",
            "liability_total_liabilities": "负债-总负债(万元)",
            "liability_total_liabilities_yoy_growth": "负债-总负债同比(%)",
            "liability_contract_liabilities": "负债-合同负债（万元）",
            "liability_short_term_loans": "负债-短期借款（万元）",
            "asset_liability_ratio": "资产负债率(%)",
            "equity_unappropriated_profit": "股东权益-未分配利润（万元）",
            "equity_total_equity": "股东权益合计(万元)",
        },
    },
    "cash_flow_sheet": {
        "identity_fields": [
            "report_id",
            "stock_code",
            "stock_abbr",
            "report_year",
            "report_period",
            "report_type",
        ],
        "metric_fields": {
            "net_cash_flow": "净现金流(元)",
            "net_cash_flow_yoy_growth": "净现金流-同比增长(%)",
            "operating_cf_net_amount": "经营性现金流-现金流量净额(万元)",
            "operating_cf_ratio_of_net_cf": "经营性现金流-净现金流占比(%)",
            "operating_cf_cash_from_sales": "经营性现金流-销售商品收到的现金（万元）",
            "investing_cf_net_amount": "投资性现金流-现金流量净额(万元)",
            "investing_cf_ratio_of_net_cf": "投资性现金流-净现金流占比(%)",
            "investing_cf_cash_for_investments": "投资性现金流-投资支付的现金（万元）",
            "investing_cf_cash_from_investment_recovery": "投资性现金流-收回投资收到的现金（万元）",
            "financing_cf_cash_from_borrowing": "融资性现金流-取得借款收到的现金（万元）",
            "financing_cf_cash_for_debt_repayment": "融资性现金流-偿还债务支付的现金（万元）",
            "financing_cf_net_amount": "融资性现金流-现金流量净额(万元)",
            "financing_cf_ratio_of_net_cf": "融资性现金流-净现金流占比(%)",
        },
    },
    "core_performance_indicators_sheet": {
        "identity_fields": [
            "report_id",
            "stock_code",
            "stock_abbr",
            "report_year",
            "report_period",
            "report_type",
        ],
        "metric_fields": {
            "eps": "每股收益(元)",
            "total_operating_revenue": "营业总收入(万元)",
            "operating_revenue_yoy_growth": "营业总收入-同比增长(%)",
            "operating_revenue_qoq_growth": "营业总收入-季度环比增长(%)",
            "net_profit_10k_yuan": "净利润(万元)",
            "net_profit_yoy_growth": "净利润-同比增长(%)",
            "net_profit_qoq_growth": "净利润-季度环比增长(%)",
            "net_asset_per_share": "每股净资产(元)",
            "roe": "净资产收益率(%)",
            "operating_cf_per_share": "每股经营现金流量(元)",
            "net_profit_excl_non_recurring": "扣非净利润（万元）",
            "net_profit_excl_non_recurring_yoy": "扣非净利润同比增长（%）",
            "gross_profit_margin": "销售毛利率(%)",
            "net_profit_margin": "销售净利率（%）",
            "roe_weighted_excl_non_recurring": "加权平均净资产收益率（扣非）（%）",
        },
    },
    "company_basic_info": {
        "fields": {
            "stock_code": "股票代码",
            "stock_abbr": "股票简称",
            "company_name": "公司名称",
            "csrc_industry": "所属证监会行业，常见值如'制造业-医药制造业'；用户提到'中药公司'、'中药行业'时，优先使用该行业值筛选，不要写 LIKE '%中药%'",
            "exchange": "交易所标识(SH/SZ/BJ)",
        },
    },
}


def _normalize_result_column_name(column: Any) -> str:
    return re.sub(r"[\W_]+", "", str(column)).lower()


def _build_ten_thousand_unit_column_names() -> set[str]:
    """构建所有标注为万元的结果列候选名，用于结果归一化。"""
    names: set[str] = set()

    for table_info in SCHEMA_INFO.values():
        for mapping_key in ("metric_fields", "fields"):
            field_mapping = table_info.get(mapping_key, {})
            if not isinstance(field_mapping, dict):
                continue

            for field_name, display_name in field_mapping.items():
                display_text = str(display_name)
                if "万元" not in display_text:
                    continue

                names.add(_normalize_result_column_name(field_name))
                names.add(_normalize_result_column_name(display_text))

                display_text_without_unit = re.sub(
                    r"[（(]?\s*万元\s*[）)]?",
                    "",
                    display_text,
                )
                names.add(_normalize_result_column_name(display_text_without_unit))

    return {name for name in names if name}


ALLOWED_COLUMN_NAMES: set[str] = _build_allowed_column_names()
TEN_THOUSAND_UNIT_COLUMN_NAMES: set[str] = _build_ten_thousand_unit_column_names()
ABNORMAL_TEN_THOUSAND_UNIT_THRESHOLD = Decimal("100000000")
ABNORMAL_TEN_THOUSAND_UNIT_RATIO = Decimal("100")

METRIC_ALIAS_MAP = {
    "利润总额": {"table": "income_sheet", "field": "total_profit"},
    "总利润": {"table": "income_sheet", "field": "total_profit"},
    "净利润": {"table": "income_sheet", "field": "net_profit"},
    "营业收入": {"table": "income_sheet", "field": "total_operating_revenue"},
    "营业总收入": {"table": "income_sheet", "field": "total_operating_revenue"},
    "营业利润": {"table": "income_sheet", "field": "operating_profit"},
    "营业支出": {"table": "income_sheet", "field": "operating_expense_cost_of_sales"},
    "销售费用": {
        "table": "income_sheet",
        "field": "operating_expense_selling_expenses",
    },
    "管理费用": {
        "table": "income_sheet",
        "field": "operating_expense_administrative_expenses",
    },
    "财务费用": {
        "table": "income_sheet",
        "field": "operating_expense_financial_expenses",
    },
    "研发费用": {"table": "income_sheet", "field": "operating_expense_rnd_expenses"},
    "营业总支出": {"table": "income_sheet", "field": "total_operating_expenses"},
    "其他收益": {"table": "income_sheet", "field": "other_income"},
    "资产减值损失": {"table": "income_sheet", "field": "asset_impairment_loss"},
    "信用减值损失": {"table": "income_sheet", "field": "credit_impairment_loss"},
    "总资产": {"table": "balance_sheet", "field": "asset_total_assets"},
    "货币资金": {"table": "balance_sheet", "field": "asset_cash_and_cash_equivalents"},
    "应收账款": {"table": "balance_sheet", "field": "asset_accounts_receivable"},
    "存货": {"table": "balance_sheet", "field": "asset_inventory"},
    "交易性金融资产": {
        "table": "balance_sheet",
        "field": "asset_trading_financial_assets",
    },
    "在建工程": {"table": "balance_sheet", "field": "asset_construction_in_progress"},
    "总负债": {"table": "balance_sheet", "field": "liability_total_liabilities"},
    "应付账款": {"table": "balance_sheet", "field": "liability_accounts_payable"},
    "预收账款": {"table": "balance_sheet", "field": "liability_advance_from_customers"},
    "合同负债": {"table": "balance_sheet", "field": "liability_contract_liabilities"},
    "短期借款": {"table": "balance_sheet", "field": "liability_short_term_loans"},
    "资产负债率": {"table": "balance_sheet", "field": "asset_liability_ratio"},
    "未分配利润": {"table": "balance_sheet", "field": "equity_unappropriated_profit"},
    "股东权益合计": {"table": "balance_sheet", "field": "equity_total_equity"},
    "净资产": {"table": "balance_sheet", "field": "equity_total_equity"},
    "净现金流": {"table": "cash_flow_sheet", "field": "net_cash_flow"},
    "经营性现金流净额": {
        "table": "cash_flow_sheet",
        "field": "operating_cf_net_amount",
    },
    "经营现金流": {"table": "cash_flow_sheet", "field": "operating_cf_net_amount"},
    "投资性现金流净额": {
        "table": "cash_flow_sheet",
        "field": "investing_cf_net_amount",
    },
    "融资性现金流净额": {
        "table": "cash_flow_sheet",
        "field": "financing_cf_net_amount",
    },
    "每股收益": {"table": "core_performance_indicators_sheet", "field": "eps"},
    "EPS": {"table": "core_performance_indicators_sheet", "field": "eps"},
    "净资产收益率": {"table": "core_performance_indicators_sheet", "field": "roe"},
    "ROE": {"table": "core_performance_indicators_sheet", "field": "roe"},
    "毛利率": {
        "table": "core_performance_indicators_sheet",
        "field": "gross_profit_margin",
    },
    "销售毛利率": {
        "table": "core_performance_indicators_sheet",
        "field": "gross_profit_margin",
    },
    "净利率": {
        "table": "core_performance_indicators_sheet",
        "field": "net_profit_margin",
    },
    "销售净利率": {
        "table": "core_performance_indicators_sheet",
        "field": "net_profit_margin",
    },
    "每股净资产": {
        "table": "core_performance_indicators_sheet",
        "field": "net_asset_per_share",
    },
    "扣非净利润": {
        "table": "core_performance_indicators_sheet",
        "field": "net_profit_excl_non_recurring",
    },
    "每股经营现金流": {
        "table": "core_performance_indicators_sheet",
        "field": "operating_cf_per_share",
    },
}

PERIOD_ALIAS_MAP = {
    "一季度": "Q1",
    "第一季度": "Q1",
    "Q1": "Q1",
    "季报": "Q1",
    "半年度": "HY",
    "中期": "HY",
    "中报": "HY",
    "HY": "HY",
    "三季度": "Q3",
    "第三季度": "Q3",
    "Q3": "Q3",
    "年度": "FY",
    "全年": "FY",
    "年报": "FY",
    "FY": "FY",
}

DERIVED_METRIC_KEYWORDS = {
    DerivedMetricType.YOY_GROWTH: [
        "同比",
        "同比增长",
        "同比增速",
        "年同比",
        "去年同期比",
        "较去年同期",
        "与去年同期相比",
        "年增长率",
        "同比增长率",
    ],
    DerivedMetricType.QOQ_GROWTH: [
        "环比",
        "环比增长",
        "环比增速",
        "季度环比",
        "较上季度",
        "与上季度相比",
        "较上一季度",
        "环比增长率",
        "季度增长率",
    ],
    DerivedMetricType.CAGR: [
        "复合增长率",
        "年均复合增长率",
        "CAGR",
        "年化增长率",
        "年均增长率",
        "复合增速",
        "年均复合增速",
    ],
    DerivedMetricType.RATIO: [
        "占比",
        "比例",
        "比重",
        "百分比",
        "占",
        "率",
        "占营业",
        "占总",
        "占收入",
        "占资产",
        "占比率",
        "比例值",
    ],
    DerivedMetricType.INDUSTRY_AVG: [
        "行业均值",
        "行业平均",
        "行业平均水平",
        "平均数",
        "全行业平均",
        "行业均值水平",
        "行业平均值",
    ],
    DerivedMetricType.MEDIAN: [
        "中位数",
        "中位值",
        "中间值",
    ],
    DerivedMetricType.CORRELATION: [
        "相关性",
        "相关系数",
        "关联度",
        "相关关系",
    ],
    DerivedMetricType.DIFFERENCE: [
        "差值",
        "差额",
        "差异",
        "相差",
        "差",
        "波动",
        "变化幅度",
        "变动幅度",
    ],
    DerivedMetricType.PERCENTAGE: [
        "百分比",
        "百分率",
        "占比",
    ],
}

UNSUPPORTED_KEYWORDS = [
    "出口业务占比",
    "出口占比",
    "海外业务占比",
    "存货周转率",
    "应收账款周转率",
    "主营业务收入",
    "医保目录",
    "国家医保",
    "新增中药",
    "中药产品",
]

PRESTORED_DERIVED_FIELDS = {
    "operating_revenue_yoy_growth",
    "operating_revenue_qoq_growth",
    "net_profit_yoy_growth",
    "net_profit_qoq_growth",
    "asset_total_assets_yoy_growth",
    "liability_total_liabilities_yoy_growth",
    "net_cash_flow_yoy_growth",
    "net_profit_excl_non_recurring_yoy",
}

YOY_GROWTH_FIELD_MAPPING = {
    "operating_revenue_yoy_growth": (
        "total_operating_revenue",
        "core_performance_indicators_sheet",
    ),
    "net_profit_yoy_growth": (
        "net_profit_10k_yuan",
        "core_performance_indicators_sheet",
    ),
    "asset_total_assets_yoy_growth": (
        "asset_total_assets",
        "balance_sheet",
    ),
    "liability_total_liabilities_yoy_growth": (
        "liability_total_liabilities",
        "balance_sheet",
    ),
    "net_cash_flow_yoy_growth": (
        "net_cash_flow",
        "cash_flow_sheet",
    ),
    "net_profit_excl_non_recurring_yoy": (
        "net_profit_excl_non_recurring",
        "core_performance_indicators_sheet",
    ),
}

QOQ_GROWTH_FIELD_MAPPING = {
    "operating_revenue_qoq_growth": (
        "total_operating_revenue",
        "income_sheet",
    ),
    "net_profit_qoq_growth": (
        "net_profit",
        "income_sheet",
    ),
}

UNSUPPORTED_METRIC_HINTS = {
    "出口业务占比": "数据库中没有出口业务相关字段",
    "出口占比": "数据库中没有出口业务相关字段",
    "海外业务占比": "数据库中没有海外业务相关字段",
    "存货周转率": "数据库中没有存货周转率字段，需通过存货和营业成本计算",
    "应收账款周转率": "数据库中没有应收账款周转率字段，需通过应收账款和营业收入计算",
    "主营业务收入": "数据库中没有主营业务收入字段，建议使用营业总收入代替",
}

BUSINESS_DEFINITION_RESPONSE_PATTERNS = [
    r"指的是",
    r"是指",
    r"在业务场景中",
    r"业务场景中",
    r"定义为",
    r"这里说的",
    r"这里的",
    r"即",
]

RESULT_IDENTITY_COLUMNS = {
    "stock_code",
    "stock_abbr",
    "company_name",
    "company",
    "report_year",
    "report_period",
}

RESULT_IDENTITY_PREFIXES = ("year_", "period_")

AGGREGATION_COLLECTION_KEYWORDS = [
    "平均",
    "均值",
    "平均值",
    "中位数",
    "中位值",
    "全行业",
    "行业整体",
    "所有公司",
    "全部公司",
    "上市公司",
    "家公司",
    "分布",
    "历史分布",
    "频次分布",
    "频率分布",
]

AGGREGATION_RESULT_KEYWORDS = [
    "分别是多少",
    "分别为多少",
    "分别是哪些",
    "有哪些",
    "均排名前",
    "排名前十",
    "排名前五",
    "前十",
    "前五",
]

CROSS_TABLE_TOPN_RATIO_KEYWORDS = [
    "未分配利润",
    "净利润",
    "比例",
]

TCM_CONTEST_UNIVERSE_KEYWORDS = [
    "中药公司",
    "中药上市公司",
    "中药企业",
    "中药行业",
]

TCM_CONTEST_INDUSTRY_SQL_PATTERNS = [
    r"(?:\w+\.)?csrc_industry\s+LIKE\s+'%中药%'",
    r"(?:\w+\.)?csrc_industry\s*=\s*'中药'",
    r"(?:\w+\.)?csrc_industry\s*=\s*'中药公司'",
    r"(?:\w+\.)?csrc_industry\s*=\s*'中药上市公司'",
    r"(?:\w+\.)?csrc_industry\s*=\s*'中药行业'",
    r"(?:\w+\.)?csrc_industry\s*=\s*'制造业-医药制造业'",
]

DEFAULT_LATEST_TIME_RANGE = {
    "report_year": 2025,
    "report_period": "Q3",
    "is_range": False,
}


def _normalize_time_range(time_range: dict | list | None) -> dict | None:
    """规范化time_range格式

    LLM可能返回数组格式的time_range，需要转换为字典格式：
    - 如果是数组：提取所有report_year组成数组，report_period取第一个
    - 如果是字典：直接返回
    - 如果是None：返回None
    """
    if time_range is None:
        return None

    if isinstance(time_range, dict):
        return time_range

    if isinstance(time_range, list) and len(time_range) > 0:
        years = []
        periods = []
        for item in time_range:
            if isinstance(item, dict):
                if item.get("report_year"):
                    years.append(item["report_year"])
                if item.get("report_period"):
                    periods.append(item["report_period"])

        if not years:
            return None

        result = {
            "report_year": years[0] if len(years) == 1 else years,
            "report_period": periods[0] if periods else "FY",
            "is_range": False,
        }
        logger.info("time_range数组格式已转换为字典: %s", result)
        return result

    return None


def _build_time_range(
    report_year: int | None = None,
    report_period: str | None = None,
    is_range: bool = False,
) -> dict | None:
    if report_year is None and not report_period:
        return None

    result = {"is_range": is_range}
    if report_year is not None:
        result["report_year"] = report_year
    if report_period:
        result["report_period"] = report_period
    return result


def _extract_ordered_time_mentions(question: str) -> list[dict[str, Any]]:
    if not question:
        return []

    aliases = sorted(set(PERIOD_ALIAS_MAP.keys()), key=len, reverse=True)
    period_pattern = "|".join(re.escape(alias) for alias in aliases)
    time_pattern = re.compile(rf"(\d{{4}})\s*年\s*({period_pattern})")

    mentions: list[dict[str, Any]] = []
    for match in time_pattern.finditer(question):
        year = int(match.group(1))
        period_alias = match.group(2)
        period_code = PERIOD_ALIAS_MAP.get(period_alias)
        if not period_code:
            continue
        mentions.append(
            {
                "report_year": year,
                "report_period": period_code,
                "is_range": False,
            }
        )
    return mentions


def _merge_metric_payload(
    current_metric: dict | list[dict] | None,
    inferred_metrics: list[dict],
) -> dict | list[dict] | None:
    merged_metrics: list[dict] = []
    seen_metric_keys: set[tuple[str, str]] = set()

    def _append_metric(metric: dict) -> None:
        field = str(metric.get("field", ""))
        table = str(metric.get("table", ""))
        if not field or not table:
            return
        metric_key = (table, field)
        if metric_key in seen_metric_keys:
            return
        merged_metrics.append(metric)
        seen_metric_keys.add(metric_key)

    current_metric_list = []
    normalized_current = _normalize_metric_payload(current_metric)
    if isinstance(normalized_current, dict):
        current_metric_list = [normalized_current]
    elif isinstance(normalized_current, list):
        current_metric_list = normalized_current

    for metric in current_metric_list:
        if isinstance(metric, dict):
            _append_metric(metric)
    for metric in inferred_metrics:
        if isinstance(metric, dict):
            _append_metric(metric)

    if not merged_metrics:
        return None
    if len(merged_metrics) == 1:
        return merged_metrics[0]
    return merged_metrics


def _get_metric_by_field(field_name: str | None) -> dict | None:
    if not field_name:
        return None

    for alias, metric in METRIC_ALIAS_MAP.items():
        if metric.get("field") == field_name:
            resolved_metric = dict(metric)
            resolved_metric["display_name"] = alias
            return resolved_metric
    return None


def _normalize_metric_payload(metric_data: Any) -> dict | list[dict] | None:
    """将 LLM 返回的 metric 统一规范成 dict 或 list[dict]。"""
    if metric_data is None:
        return None

    def _coerce_metric_item(item: Any) -> list[dict]:
        if not isinstance(item, dict):
            return []

        fields = item.get("field")
        if isinstance(fields, list):
            normalized_items: list[dict] = []
            raw_tables = item.get("table")
            raw_display_names = item.get("display_name")

            for idx, raw_field in enumerate(fields):
                if not isinstance(raw_field, str) or not raw_field:
                    continue

                resolved_metric = _get_metric_by_field(raw_field) or {"field": raw_field}

                if isinstance(raw_tables, list):
                    table_value = raw_tables[idx] if idx < len(raw_tables) else None
                    if isinstance(table_value, str) and table_value:
                        resolved_metric["table"] = table_value
                elif isinstance(raw_tables, str) and raw_tables:
                    resolved_metric.setdefault("table", raw_tables)

                if isinstance(raw_display_names, list):
                    display_name = (
                        raw_display_names[idx] if idx < len(raw_display_names) else None
                    )
                    if isinstance(display_name, str) and display_name:
                        resolved_metric["display_name"] = display_name

                normalized_items.append(resolved_metric)

            return normalized_items

        if isinstance(fields, str) and fields:
            return [item]

        return []

    if isinstance(metric_data, list):
        normalized_metrics: list[dict] = []
        seen_metric_keys: set[tuple[str, str]] = set()

        for item in metric_data:
            for normalized_item in _coerce_metric_item(item):
                metric_key = (
                    str(normalized_item.get("table", "")),
                    str(normalized_item.get("field", "")),
                )
                if metric_key in seen_metric_keys:
                    continue
                normalized_metrics.append(normalized_item)
                seen_metric_keys.add(metric_key)

        if not normalized_metrics:
            return None
        if len(normalized_metrics) == 1:
            return normalized_metrics[0]
        return normalized_metrics

    if isinstance(metric_data, dict):
        normalized_items = _coerce_metric_item(metric_data)
        if not normalized_items:
            return None
        if len(normalized_items) == 1:
            return normalized_items[0]
        return normalized_items

    return None


def _resolve_current_report_period(report_period: Any) -> str:
    """派生指标模板需要一个明确的当前期，列表场景默认取最后一个周期。"""
    if isinstance(report_period, list):
        valid_periods = [item for item in report_period if isinstance(item, str) and item]
        if valid_periods:
            return valid_periods[-1]
        return "FY"

    if isinstance(report_period, str) and report_period:
        return report_period

    return "FY"


def _extract_comparison_time_points(time_range: dict | None) -> list[dict[str, Any]]:
    """提取 comparison 场景下的双时间点结构。"""
    if not isinstance(time_range, dict):
        return []

    indexed_points: list[tuple[int, dict[str, Any]]] = []
    for key, value in time_range.items():
        match = re.fullmatch(r"report_year_(\d+)", str(key))
        if not match or not isinstance(value, int):
            continue

        index = int(match.group(1))
        period = time_range.get(f"report_period_{index}")
        if not isinstance(period, str) or not period:
            continue

        indexed_points.append(
            (
                index,
                {
                    "report_year": value,
                    "report_period": period,
                },
            )
        )

    indexed_points.sort(key=lambda item: item[0])
    if indexed_points:
        return [point for _, point in indexed_points]

    report_year = time_range.get("report_year")
    report_period = time_range.get("report_period")
    if isinstance(report_year, list) and isinstance(report_period, str) and report_period:
        normalized_years = sorted(
            year for year in report_year if isinstance(year, int)
        )
        if len(normalized_years) == 2:
            return [
                {"report_year": normalized_years[0], "report_period": report_period},
                {"report_year": normalized_years[1], "report_period": report_period},
            ]

    return []


def _resolve_prestored_derived_metric_source(
    metric_field: str,
    table_name: str,
    derived_type: DerivedMetricType,
) -> tuple[str, str]:
    field_mapping: dict[str, tuple[str, str]] = {}
    if derived_type == DerivedMetricType.YOY_GROWTH:
        field_mapping = YOY_GROWTH_FIELD_MAPPING
    elif derived_type == DerivedMetricType.QOQ_GROWTH:
        field_mapping = QOQ_GROWTH_FIELD_MAPPING

    source_metric = field_mapping.get(metric_field)
    if not source_metric:
        return metric_field, table_name

    source_field, source_table = source_metric
    return source_field, source_table or table_name


def _has_non_null_measure_values(
    rows: list[dict],
    metric_field: str = "",
) -> bool:
    if not rows:
        return False

    for row in rows:
        if not isinstance(row, dict):
            continue

        if metric_field and row.get(metric_field) is not None:
            return True

        for column, value in row.items():
            column_name = str(column)
            if value is None:
                continue
            if column_name in RESULT_IDENTITY_COLUMNS:
                continue
            if any(column_name.startswith(prefix) for prefix in RESULT_IDENTITY_PREFIXES):
                continue
            return True

    return False


def _generate_qoq_comparison_sql(
    intent: IntentResult,
    metric_field: str,
    table_name: str,
    comparison_points: list[dict[str, Any]],
    period_sequence: dict[str, dict[str, Any]],
) -> str | None:
    if len(comparison_points) != 2:
        return None

    ordered_points = sorted(
        comparison_points,
        key=lambda point: (
            point.get("report_year", 0),
            str(point.get("report_period", "")),
        ),
    )
    earlier_point, later_point = ordered_points
    earlier_year = earlier_point.get("report_year")
    earlier_period = earlier_point.get("report_period")
    later_year = later_point.get("report_year")
    later_period = later_point.get("report_period")

    if not all(
        isinstance(value, int) for value in (earlier_year, later_year)
    ) or not all(
        isinstance(value, str) and value for value in (earlier_period, later_period)
    ):
        return None

    earlier_period_info = period_sequence.get(
        earlier_period, {"prev_year_offset": 0, "prev_period": "Q1"}
    )
    later_period_info = period_sequence.get(
        later_period, {"prev_year_offset": 0, "prev_period": "Q1"}
    )

    earlier_suffix = str(earlier_year)
    later_suffix = str(later_year)
    earlier_company_filter = _build_company_filter(intent, table_alias=f"curr_{earlier_suffix}")
    later_company_filter = _build_company_filter(intent, table_alias=f"curr_{later_suffix}")

    earlier_expr = (
        f"ROUND((curr_{earlier_suffix}.{metric_field} - prev_{earlier_suffix}.{metric_field}) "
        f"/ ABS(prev_{earlier_suffix}.{metric_field}) * 100, 2)"
    )
    later_expr = (
        f"ROUND((curr_{later_suffix}.{metric_field} - prev_{later_suffix}.{metric_field}) "
        f"/ ABS(prev_{later_suffix}.{metric_field}) * 100, 2)"
    )

    return (
        "SELECT \n"
        f"  curr_{later_suffix}.stock_abbr,\n"
        f"  curr_{later_suffix}.report_year AS year_{later_suffix},\n"
        f"  curr_{later_suffix}.report_period AS period_{later_suffix},\n"
        f"  prev_{later_suffix}.{metric_field} AS prev_value_{later_suffix},\n"
        f"  curr_{later_suffix}.{metric_field} AS current_value_{later_suffix},\n"
        "  CASE \n"
        f"    WHEN prev_{later_suffix}.{metric_field} IS NULL OR prev_{later_suffix}.{metric_field} = 0 THEN NULL\n"
        f"    ELSE {later_expr}\n"
        f"  END AS qoq_growth_{later_suffix},\n"
        f"  curr_{earlier_suffix}.report_year AS year_{earlier_suffix},\n"
        f"  curr_{earlier_suffix}.report_period AS period_{earlier_suffix},\n"
        f"  prev_{earlier_suffix}.{metric_field} AS prev_value_{earlier_suffix},\n"
        f"  curr_{earlier_suffix}.{metric_field} AS current_value_{earlier_suffix},\n"
        "  CASE \n"
        f"    WHEN prev_{earlier_suffix}.{metric_field} IS NULL OR prev_{earlier_suffix}.{metric_field} = 0 THEN NULL\n"
        f"    ELSE {earlier_expr}\n"
        f"  END AS qoq_growth_{earlier_suffix},\n"
        "  CASE \n"
        f"    WHEN prev_{later_suffix}.{metric_field} IS NULL OR prev_{later_suffix}.{metric_field} = 0\n"
        f"      OR prev_{earlier_suffix}.{metric_field} IS NULL OR prev_{earlier_suffix}.{metric_field} = 0 THEN NULL\n"
        f"    ELSE ROUND({later_expr} - {earlier_expr}, 2)\n"
        "  END AS qoq_growth_change\n"
        f"FROM {table_name} curr_{later_suffix}\n"
        f"LEFT JOIN {table_name} prev_{later_suffix} \n"
        f"  ON curr_{later_suffix}.stock_code = prev_{later_suffix}.stock_code \n"
        f"  AND prev_{later_suffix}.report_year = curr_{later_suffix}.report_year + {later_period_info.get('prev_year_offset', 0)}\n"
        f"  AND prev_{later_suffix}.report_period = '{later_period_info.get('prev_period', 'Q1')}'\n"
        f"  AND prev_{later_suffix}.report_type = 'REPORT'\n"
        f"JOIN {table_name} curr_{earlier_suffix} \n"
        f"  ON curr_{later_suffix}.stock_code = curr_{earlier_suffix}.stock_code\n"
        f"LEFT JOIN {table_name} prev_{earlier_suffix} \n"
        f"  ON curr_{earlier_suffix}.stock_code = prev_{earlier_suffix}.stock_code \n"
        f"  AND prev_{earlier_suffix}.report_year = curr_{earlier_suffix}.report_year + {earlier_period_info.get('prev_year_offset', 0)}\n"
        f"  AND prev_{earlier_suffix}.report_period = '{earlier_period_info.get('prev_period', 'Q1')}'\n"
        f"  AND prev_{earlier_suffix}.report_type = 'REPORT'\n"
        f"WHERE {later_company_filter}\n"
        f"  AND curr_{later_suffix}.report_year = {later_year}\n"
        f"  AND curr_{later_suffix}.report_period = '{later_period}'\n"
        f"  AND curr_{later_suffix}.report_type = 'REPORT'\n"
        f"  AND {earlier_company_filter}\n"
        f"  AND curr_{earlier_suffix}.report_year = {earlier_year}\n"
        f"  AND curr_{earlier_suffix}.report_period = '{earlier_period}'\n"
        f"  AND curr_{earlier_suffix}.report_type = 'REPORT'\n"
        f"ORDER BY curr_{later_suffix}.stock_abbr"
    )


def _extract_metrics_from_question(question: str) -> list[dict]:
    matched_metrics: list[dict] = []
    seen_metric_keys: set[tuple[str, str]] = set()

    for alias, metric in sorted(
        METRIC_ALIAS_MAP.items(), key=lambda item: len(item[0]), reverse=True
    ):
        if alias not in question:
            continue

        metric_key = (metric.get("table", ""), metric.get("field", ""))
        if metric_key in seen_metric_keys:
            continue

        resolved_metric = dict(metric)
        resolved_metric["display_name"] = alias
        matched_metrics.append(resolved_metric)
        seen_metric_keys.add(metric_key)

    return matched_metrics


def _is_cross_table_topn_ratio_question(question: str) -> bool:
    if not question:
        return False
    return (
        "排名前" in question
        and _extract_topn_limit(question) is not None
        and all(keyword in question for keyword in CROSS_TABLE_TOPN_RATIO_KEYWORDS)
        and any(keyword in question for keyword in ["占未分配利润", "占未分配利润的比例", "比例"])
    )


def _infer_cross_table_topn_ratio_time_ranges(
    question: str,
    fallback_time_range: dict | None,
) -> tuple[dict | None, dict | None]:
    ordered_mentions = _extract_ordered_time_mentions(question)
    ranking_time_range = ordered_mentions[0] if ordered_mentions else fallback_time_range

    aliases = sorted(set(PERIOD_ALIAS_MAP.keys()), key=len, reverse=True)
    period_pattern = "|".join(re.escape(alias) for alias in aliases)
    calculation_match = re.search(
        rf"(\d{{4}})年(?!\s*(?:{period_pattern}))(?:的)?[^，。；]*净利润",
        question,
    )
    calculation_time_range = None
    if calculation_match:
        calculation_time_range = _build_time_range(
            report_year=int(calculation_match.group(1)),
            report_period="FY",
            is_range=False,
        )

    if calculation_time_range is None:
        calculation_time_range = fallback_time_range

    return ranking_time_range, calculation_time_range


CHINESE_NUMERAL_MAP = {
    "一": 1,
    "二": 2,
    "两": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10,
}


def _extract_topn_limit(question: str) -> int | None:
    if not question:
        return None

    digit_match = re.search(r"前\s*(\d+)", question)
    if digit_match:
        return int(digit_match.group(1))

    chinese_match = re.search(r"前\s*([一二两三四五六七八九十]+)", question)
    if not chinese_match:
        return None

    numeral = chinese_match.group(1)
    if numeral == "十":
        return 10
    if "十" in numeral:
        parts = numeral.split("十")
        tens = CHINESE_NUMERAL_MAP.get(parts[0], 1) if parts[0] else 1
        ones = CHINESE_NUMERAL_MAP.get(parts[1], 0) if len(parts) > 1 and parts[1] else 0
        return tens * 10 + ones

    return CHINESE_NUMERAL_MAP.get(numeral)


def _is_multi_metric_topn_intersection_question(intent: IntentResult) -> bool:
    question = intent.question or ""
    metrics = intent.get_metric_list()
    if intent.query_type != QueryType.RANKING or len(metrics) < 2:
        return False

    return "均排名前" in question and _extract_topn_limit(question) is not None


def _generate_multi_metric_topn_intersection_sql(intent: IntentResult) -> str | None:
    if not _is_multi_metric_topn_intersection_question(intent):
        return None

    time_range = intent.time_range or {}
    report_year = time_range.get("report_year")
    report_period = time_range.get("report_period")
    if not isinstance(report_year, int) or not isinstance(report_period, str):
        return None

    metrics = intent.get_metric_list()
    normalized_metrics: list[dict[str, str]] = []
    seen_fields: set[tuple[str, str]] = set()
    for metric in metrics:
        field = metric.get("field")
        table = metric.get("table")
        if not isinstance(field, str) or not isinstance(table, str):
            return None
        metric_key = (table, field)
        if metric_key in seen_fields:
            continue
        seen_fields.add(metric_key)
        normalized_metrics.append({
            "field": field,
            "table": table,
            "display_name": str(metric.get("display_name") or field),
        })

    if len(normalized_metrics) < 2:
        return None

    base_table = normalized_metrics[0]["table"]
    if any(metric["table"] != base_table for metric in normalized_metrics):
        return None

    limit = _extract_topn_limit(intent.question or "")
    if limit is None:
        return None

    table_alias = "t"
    select_columns = [
        f"{table_alias}.stock_code",
        f"{table_alias}.stock_abbr",
    ]
    rank_columns: list[str] = []
    filters: list[str] = []
    order_columns: list[str] = []

    for index, metric in enumerate(normalized_metrics, start=1):
        field = metric["field"]
        select_columns.append(f"{table_alias}.{field}")
        rank_alias = f"metric_rank_{index}"
        rank_columns.append(
            f"RANK() OVER (ORDER BY {table_alias}.{field} DESC) AS {rank_alias}"
        )
        filters.append(f"ranked.{rank_alias} <= {limit}")
        order_columns.append(f"ranked.{rank_alias}")

    inner_select = ",\n            ".join(select_columns + rank_columns)
    outer_select = ",\n    ".join(
        ["ranked.stock_code", "ranked.stock_abbr"]
        + [f"ranked.{metric['field']}" for metric in normalized_metrics]
    )

    return (
        "WITH ranked AS (\n"
        "    SELECT\n"
        f"            {inner_select}\n"
        f"    FROM {base_table} {table_alias}\n"
        f"    WHERE {table_alias}.report_year = {report_year}\n"
        f"      AND {table_alias}.report_period = '{report_period}'\n"
        ")\n"
        "SELECT\n"
        f"    {outer_select}\n"
        "FROM ranked\n"
        f"WHERE {' AND '.join(filters)}\n"
        f"ORDER BY {', '.join(order_columns)}"
    )


def _generate_cross_table_topn_ratio_sql(intent: IntentResult) -> str | None:
    question = intent.question or ""
    if not _is_cross_table_topn_ratio_question(question):
        return None

    topn_limit = _extract_topn_limit(question)
    if topn_limit is None:
        return None

    ranking_time_range = intent.ranking_time_range or intent.time_range or {}
    ranking_year = ranking_time_range.get("report_year")
    ranking_period = _resolve_current_report_period(
        ranking_time_range.get("report_period", "FY")
    )
    if not isinstance(ranking_year, int) or not isinstance(ranking_period, str):
        return None

    calculation_time_range = intent.calculation_time_range or intent.time_range or {}
    calculation_year = calculation_time_range.get("report_year")
    calculation_period = _resolve_current_report_period(
        calculation_time_range.get("report_period", "FY")
    )
    if not isinstance(calculation_year, int) or not isinstance(calculation_period, str):
        return None

    return (
        "WITH top_companies AS (\n"
        "    SELECT\n"
        "        stock_code,\n"
        "        stock_abbr,\n"
        "        equity_unappropriated_profit\n"
        "    FROM balance_sheet\n"
        f"    WHERE report_year = {ranking_year}\n"
        f"      AND report_period = '{ranking_period}'\n"
        "      AND equity_unappropriated_profit IS NOT NULL\n"
        "    ORDER BY equity_unappropriated_profit DESC\n"
        f"    LIMIT {topn_limit}\n"
        ")\n"
        "SELECT\n"
        "    top.stock_code,\n"
        "    top.stock_abbr,\n"
        "    top.equity_unappropriated_profit AS equity_unappropriated_profit,\n"
        "    income.net_profit AS net_profit,\n"
        "    CASE\n"
        "        WHEN income.net_profit IS NULL\n"
        "             OR top.equity_unappropriated_profit IS NULL\n"
        "             OR top.equity_unappropriated_profit = 0 THEN NULL\n"
        "        ELSE ROUND(income.net_profit / ABS(top.equity_unappropriated_profit) * 100, 2)\n"
        "    END AS ratio_percent,\n"
        "    income.report_year AS calculation_report_year,\n"
        "    income.report_period AS calculation_report_period\n"
        "FROM top_companies top\n"
        "LEFT JOIN income_sheet income\n"
        "    ON top.stock_code = income.stock_code\n"
        f"   AND income.report_year = {calculation_year}\n"
        f"   AND income.report_period = '{calculation_period}'\n"
        "ORDER BY top.equity_unappropriated_profit DESC"
    )


def _is_business_definition_response(question: str) -> bool:
    return any(pattern in question for pattern in BUSINESS_DEFINITION_RESPONSE_PATTERNS)


def _is_aggregation_collection_question(question: str) -> bool:
    return any(keyword in question for keyword in AGGREGATION_COLLECTION_KEYWORDS)


def _references_collection_result(question: str) -> bool:
    # 如果问题中包含自包含的筛选条件，则"这些公司/那些公司"指代的是当前筛选结果
    # 而非上一轮结果，此时不应视为对上一轮结果的引用
    self_contained_filter_keywords = [
        "为负数",
        "为负",
        "大于",
        "小于",
        "等于",
        "不低于",
        "不超过",
        "高于",
        "低于",
        "满足",
        "符合",
        "筛选",
        "条件",
    ]
    has_self_contained_filter = any(kw in question for kw in self_contained_filter_keywords)

    for pattern, _ in COLLECTION_COREFERENCE_PATTERNS:
        if re.search(pattern, question):
            # 如果包含自包含筛选条件，"这些公司"指代的是当前查询的筛选结果
            if has_self_contained_filter and pattern in (r"这些公司", r"那些公司", r"这几家公司", r"那几家公司"):
                return False
            return True

    return bool(
        re.search(r"这[0-9一二三四五六七八九十两几]+家(?:公司|企业|上市公司)", question)
        or re.search(r"前[0-9一二三四五六七八九十两]+家(?:公司|企业|上市公司)", question)
        or re.search(r"上述[0-9一二三四五六七八九十两几]*家(?:公司|企业|上市公司)", question)
    )


def _infer_query_type_from_question(question: str) -> QueryType | None:
    if any(keyword in question for keyword in ["对比", "比较", "相比", "差异", "变化", "增长率如何"]):
        return QueryType.COMPARISON
    if any(keyword in question for keyword in AGGREGATION_RESULT_KEYWORDS):
        return QueryType.RANKING
    if any(keyword in question for keyword in ["趋势", "近几年", "近三年", "近五年"]):
        return QueryType.TREND
    return None


def _is_business_definition_followup(intent: IntentResult) -> bool:
    question = intent.question or ""
    return bool(
        _detect_business_definition_needed(question)
        and _is_business_definition_response(question)
        and intent.query_type == QueryType.RANKING
        and intent.company is None
        and intent.metric is not None
    )


def _repair_intent_from_question(intent: IntentResult) -> IntentResult:
    question = intent.question or ""
    if not question:
        return intent

    patched_intent = intent.model_copy(deep=True)

    if not patched_intent.time_range:
        inferred_time_range = _resolve_time_expression(question)
        if inferred_time_range:
            patched_intent.time_range = inferred_time_range

    if not patched_intent.metric:
        inferred_metrics = _extract_metrics_from_question(question)
        if len(inferred_metrics) == 1:
            patched_intent.metric = inferred_metrics[0]
        elif len(inferred_metrics) > 1:
            patched_intent.metric = inferred_metrics

    business_def = _detect_business_definition_needed(question)
    if business_def and _is_business_definition_response(question) and not patched_intent.metric:
        fallback_metric = _get_metric_by_field(business_def.get("fallback_metric"))
        if fallback_metric:
            patched_intent.metric = fallback_metric

    if (
        patched_intent.query_type is None
        or (
            patched_intent.query_type == QueryType.SINGLE_VALUE
            and _is_aggregation_collection_question(question)
        )
    ):
        inferred_query_type = _infer_query_type_from_question(question)
        if inferred_query_type:
            patched_intent.query_type = inferred_query_type

    if _is_aggregation_collection_question(question):
        patched_intent.capability = QueryCapability.AGGREGATION

    if not patched_intent.time_range and _is_business_definition_followup(patched_intent):
        patched_intent.time_range = dict(DEFAULT_LATEST_TIME_RANGE)

    if (
        not patched_intent.time_range
        and _is_tcm_contest_universe_question(question)
        and patched_intent.query_type != QueryType.TREND
    ):
        patched_intent.time_range = dict(DEFAULT_LATEST_TIME_RANGE)

    if _is_cross_table_topn_ratio_question(question):
        inferred_metrics = _extract_metrics_from_question(question)
        patched_intent.metric = _merge_metric_payload(
            patched_intent.metric,
            inferred_metrics,
        )
        patched_intent.query_type = QueryType.RANKING
        patched_intent.capability = QueryCapability.CROSS_TABLE
        patched_intent.derived_metric_type = DerivedMetricType.RATIO

        ranking_time_range, calculation_time_range = (
            _infer_cross_table_topn_ratio_time_ranges(
                question,
                patched_intent.time_range,
            )
        )
        if ranking_time_range:
            patched_intent.ranking_time_range = ranking_time_range
            patched_intent.time_range = ranking_time_range
        if calculation_time_range:
            patched_intent.calculation_time_range = calculation_time_range

    return patched_intent


def _load_derived_metrics_config() -> dict:
    """加载派生指标配置文件"""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config",
        "prompts",
        "derived_metrics.yaml",
    )
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.warning("加载派生指标配置失败: %s", e)
        return {}


def _build_company_filter(intent: IntentResult, table_alias: str = "") -> str:
    """构建公司筛选条件

    Args:
        intent: 意图解析结果
        table_alias: 表别名（如 "t1"），用于JOIN查询时避免字段歧义
    """
    companies = intent.get_company_list()
    if not companies:
        return "1=1"

    prefix = f"{table_alias}." if table_alias else ""
    conditions = []
    for company in companies:
        value = company.get("value", "")
        company_type = company.get("type", "stock_abbr")
        if company_type == "stock_code":
            conditions.append(f"{prefix}stock_code = '{value}'")
        else:
            conditions.append(f"{prefix}stock_abbr = '{value}'")

    if len(conditions) == 1:
        return conditions[0]
    return "(" + " OR ".join(conditions) + ")"


def _generate_derived_metric_sql(
    intent: IntentResult,
    derived_type: DerivedMetricType,
) -> str | None:
    """根据派生指标类型生成SQL模板"""
    config = _load_derived_metrics_config()
    if not config:
        return None

    first_metric = intent.get_first_metric()
    metric = first_metric or {}
    metric_field = metric.get("field", "")
    table_name = metric.get("table", "")
    metric_field, table_name = _resolve_prestored_derived_metric_source(
        metric_field,
        table_name,
        derived_type,
    )
    time_range = intent.time_range or {}
    report_year = time_range.get("report_year", 2025)
    report_period = _resolve_current_report_period(
        time_range.get("report_period", "FY")
    )
    comparison_points = _extract_comparison_time_points(time_range)

    if isinstance(report_year, list) and not (
        intent.query_type == QueryType.COMPARISON and len(comparison_points) == 2
    ):
        logger.warning(
            "report_year为数组格式 %s，派生指标模板不支持多年查询，返回None让LLM生成SQL",
            report_year,
        )
        return None

    JOIN_DERIVED_TYPES = {
        DerivedMetricType.YOY_GROWTH,
        DerivedMetricType.QOQ_GROWTH,
        DerivedMetricType.DIFFERENCE,
    }
    table_alias = "t1" if derived_type in JOIN_DERIVED_TYPES else ""
    company_filter = _build_company_filter(intent, table_alias=table_alias)

    type_key = derived_type.value
    template_config = config.get(type_key)
    if not template_config:
        return None

    sql_template = template_config.get("sql_template", "")

    try:
        if derived_type == DerivedMetricType.YOY_GROWTH:
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                company_filter=company_filter,
                report_year=report_year,
                report_period=report_period,
            )

        elif derived_type == DerivedMetricType.QOQ_GROWTH:
            period_sequence = template_config.get("period_sequence", {})
            if (
                intent.query_type == QueryType.COMPARISON
                and len(comparison_points) == 2
            ):
                comparison_sql = _generate_qoq_comparison_sql(
                    intent,
                    metric_field,
                    table_name,
                    comparison_points,
                    period_sequence,
                )
                if comparison_sql:
                    return comparison_sql

            period_info = period_sequence.get(
                report_period, {"prev_year_offset": 0, "prev_period": "Q1"}
            )
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                company_filter=company_filter,
                report_year=report_year,
                report_period=report_period,
                prev_year_offset=period_info.get("prev_year_offset", 0),
                prev_period=period_info.get("prev_period", "Q1"),
            )

        elif derived_type == DerivedMetricType.CAGR:
            start_year = report_year - 3 if isinstance(report_year, int) else 2022
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                company_filter=company_filter,
                start_year=start_year,
                end_year=report_year,
                report_period=report_period,
            )

        elif derived_type == DerivedMetricType.RATIO:
            common_ratios = template_config.get("common_ratios", {})
            ratio_config = None
            for ratio_key, ratio_info in common_ratios.items():
                if ratio_info.get("numerator") == metric_field:
                    ratio_config = ratio_info
                    break

            if ratio_config:
                return sql_template.format(
                    numerator_field=ratio_config.get("numerator", metric_field),
                    denominator_field=ratio_config.get(
                        "denominator", "total_operating_income"
                    ),
                    table_name=table_name,
                    company_filter=company_filter,
                    report_year=report_year,
                    report_period=report_period,
                )
            return None

        elif derived_type == DerivedMetricType.INDUSTRY_AVG:
            metric_name = metric.get("display_name", metric_field)
            return sql_template.format(
                metric_field=metric_field,
                metric_name=metric_name,
                table_name=table_name,
                report_year=report_year,
                report_period=report_period,
            )

        elif derived_type == DerivedMetricType.MEDIAN:
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                report_year=report_year,
                report_period=report_period,
            )

        elif derived_type == DerivedMetricType.DIFFERENCE:
            if "," in metric_field or "," in table_name:
                return None
            return sql_template.format(
                metric_field=metric_field,
                table_name=table_name,
                year_1=report_year,
                year_2=report_year - 1 if isinstance(report_year, int) else 2024,
                report_period=report_period,
            )

        elif derived_type == DerivedMetricType.CORRELATION:
            numeric_cols = [metric_field]
            if len(numeric_cols) < 2:
                return None
            start_year = report_year - 3 if isinstance(report_year, int) else 2022
            return sql_template.format(
                metric_field_1=numeric_cols[0],
                metric_field_2=(
                    numeric_cols[1] if len(numeric_cols) > 1 else numeric_cols[0]
                ),
                table_name=table_name,
                company_filter=company_filter,
                start_year=start_year,
                end_year=report_year,
            )

        return None
    except Exception as e:
        logger.warning("派生指标SQL模板生成失败: %s", e)
        return None


def _detect_derived_metric(question: str) -> DerivedMetricType | None:
    """检测问题中是否包含派生指标关键词"""
    for metric_type, keywords in DERIVED_METRIC_KEYWORDS.items():
        for keyword in keywords:
            if keyword in question:
                return metric_type
    return None


def _contains_continuity_keyword(question: str) -> bool:
    """检测问题中是否包含连续性关键词"""
    continuity_keywords = [
        "连续", "均超过", "均低于", "均满足", "都是", "全部", "每一",
        "连续N个", "连续N年", "连续N季度", "连续N期",
    ]
    for keyword in continuity_keywords:
        if keyword in question:
            return True
    return False


def _generate_continuity_sql(intent: IntentResult) -> str | None:
    """生成连续性查询SQL，处理'连续N期满足某条件'的查询"""
    continuity_cfg = intent.continuity_config or {}
    period_count = continuity_cfg.get("period_count")
    condition = continuity_cfg.get("condition")
    start_period = continuity_cfg.get("start_period")
    end_period = continuity_cfg.get("end_period")
    
    first_metric = intent.get_first_metric()
    if not first_metric:
        return None
    
    metric_field = first_metric.get("field", "")
    table_name = first_metric.get("table", "core_performance_indicators_sheet")
    
    if not metric_field or not table_name:
        return None
    
    if not period_count:
        import re
        question = intent.question or ""
        match = re.search(r'连续(\d+)个', question)
        if match:
            period_count = int(match.group(1))
        else:
            match = re.search(r'连续(\d+)', question)
            if match:
                period_count = int(match.group(1))
    
    if not period_count:
        period_count = 4
    
    if not start_period and intent.time_range:
        start_period = intent.time_range
    
    if not end_period and intent.time_range:
        end_period = intent.time_range
    
    start_year = start_period.get("report_year") if isinstance(start_period, dict) else None
    start_period_val = start_period.get("report_period") if isinstance(start_period, dict) else None
    end_year = end_period.get("report_year") if isinstance(end_period, dict) else None
    end_period_val = end_period.get("report_period") if isinstance(end_period, dict) else None
    
    if not start_year:
        start_year = 2022
    if not end_year:
        end_year = 2025
    
    if not condition:
        condition = f"{metric_field} IS NOT NULL"
    
    period_order_case = """
        CASE report_period 
            WHEN 'Q1' THEN 1 
            WHEN 'HY' THEN 2 
            WHEN 'Q3' THEN 3 
            WHEN 'FY' THEN 4 
        END
    """
    
    sql = f"""
WITH qualified_periods AS (
    SELECT 
        stock_code, 
        stock_abbr, 
        report_year, 
        report_period, 
        {metric_field},
        ROW_NUMBER() OVER (
            PARTITION BY stock_code 
            ORDER BY report_year, {period_order_case}
        ) as rn
    FROM {table_name}
    WHERE {condition}
      AND report_year BETWEEN {start_year} AND {end_year}
),
company_continuous_count AS (
    SELECT 
        stock_code, 
        stock_abbr,
        COUNT(*) as continuous_count
    FROM qualified_periods
    GROUP BY stock_code, stock_abbr
    HAVING COUNT(*) >= {period_count}
)
SELECT 
    q.stock_code,
    q.stock_abbr,
    q.report_year,
    q.report_period,
    q.{metric_field}
FROM qualified_periods q
INNER JOIN company_continuous_count c 
    ON q.stock_code = c.stock_code
ORDER BY q.stock_code, q.report_year, {period_order_case}
""".strip()
    
    return sql


def _detect_unsupported(question: str) -> str | None:
    """检测问题中是否包含不支持的数据源关键词（返回第一个匹配的关键词）"""
    for keyword in UNSUPPORTED_KEYWORDS:
        if keyword in question:
            return keyword
    return None


def _detect_all_unsupported_keywords(question: str) -> list[str]:
    """检测问题中所有不支持的数据源关键词（返回所有匹配的关键词列表）"""
    found = []
    for keyword in UNSUPPORTED_KEYWORDS:
        if keyword in question:
            found.append(keyword)
    return found


def _detect_business_definition_needed(question: str) -> dict | None:
    """检测问题中是否包含需要业务定义澄清的关键词"""
    config = _load_derived_metrics_config()
    if not config:
        return None

    business_definitions = config.get("business_definitions", {})
    for def_key, def_info in business_definitions.items():
        keyword = def_info.get("keyword", "")
        aliases = def_info.get("keyword_aliases", [])
        all_keywords = [keyword] + aliases

        for kw in all_keywords:
            if kw in question:
                return {
                    "keyword": keyword,
                    "definition": def_info.get("definition", ""),
                    "clarification": def_info.get("clarification", ""),
                    "fallback_metric": def_info.get("fallback_metric"),
                    "unsupported": def_info.get("unsupported", False),
                }
    return None


def _handle_business_definition_clarification(
    question: str,
    intent: IntentResult,
) -> tuple[bool, str]:
    """处理业务定义澄清，返回(是否需要澄清, 澄清问题或空字符串)"""
    business_def = _detect_business_definition_needed(question)
    if not business_def:
        return False, ""

    if business_def.get("unsupported"):
        return (
            True,
            f"抱歉，{business_def.get('clarification', '当前数据源不支持该查询')}",
        )

    if _is_business_definition_response(question):
        fallback_metric = business_def.get("fallback_metric")
        current_metric = intent.get_first_metric()
        current_field = current_metric.get("field") if current_metric else None
        if fallback_metric and current_field == fallback_metric:
            return False, ""

    clarification = business_def.get("clarification", "")
    if clarification:
        return True, clarification

    return False, ""


def _classify_query_capability(
    question: str,
    metric: dict | None,
    derived_metric_type: DerivedMetricType | None,
) -> QueryCapability:
    """分类查询能力

    返回逻辑：
    - 如果包含不支持关键词，且同时有metric → PARTIAL_SUPPORT（部分支持）
    - 如果包含不支持关键词，但没有metric → UNSUPPORTED（完全不支持）
    - 如果包含"分布/历史分布/频次"等聚合关键词 → AGGREGATION（忽略派生指标）
    - 如果有派生指标类型 → DERIVED_METRIC
    - 如果没有metric → DIRECT_FIELD
    """
    unsupported_keyword = _detect_unsupported(question)
    if unsupported_keyword:
        # 有metric时为部分支持，无metric时为完全不支持
        if metric is not None:
            return QueryCapability.PARTIAL_SUPPORT
        return QueryCapability.UNSUPPORTED

    if _is_aggregation_collection_question(question):
        return QueryCapability.AGGREGATION

    if derived_metric_type:
        return QueryCapability.DERIVED_METRIC

    if metric is None:
        return QueryCapability.DIRECT_FIELD

    return QueryCapability.DIRECT_FIELD


def _convert_path_to_url(file_path: str) -> str:
    import os

    filename = os.path.basename(file_path)
    return f"/api/v1/chat/images/{filename}"


def _get_chat_config() -> dict:
    return settings.PROMPT_CONFIG.get_chat_config


def _build_schema_info_text() -> str:
    lines = []
    for table_name, table_info in SCHEMA_INFO.items():
        lines.append(f"\n表名: {table_name}")
        if "identity_fields" in table_info:
            lines.append(f"  身份字段: {', '.join(table_info['identity_fields'])}")
        if "metric_fields" in table_info:
            lines.append("  指标字段:")
            for field_name, field_desc in table_info["metric_fields"].items():
                lines.append(f"    {field_name}: {field_desc}")
        if "fields" in table_info:
            lines.append("  字段:")
            for field_name, field_desc in table_info["fields"].items():
                lines.append(f"    {field_name}: {field_desc}")
    return "\n".join(lines)


def _build_schema_ddl_text() -> str:
    lines = []
    for table_name, table_info in SCHEMA_INFO.items():
        lines.append(f"CREATE TABLE {table_name} (")
        all_fields = {}
        if "identity_fields" in table_info:
            for f in table_info["identity_fields"]:
                all_fields[f] = "VARCHAR/INT"
        if "metric_fields" in table_info:
            for f, desc in table_info["metric_fields"].items():
                all_fields[f] = "DECIMAL"
        if "fields" in table_info:
            for f, desc in table_info["fields"].items():
                all_fields[f] = "VARCHAR"
        field_lines = [f"  {f} {t} -- {desc}" for f, t in all_fields.items()]
        lines.append(",\n".join(field_lines))
        lines.append(");")
        lines.append("")
    return "\n".join(lines)


def _get_company_list(db: Session) -> str:
    stmt = select(
        CompanyBasicInfo.stock_code,
        CompanyBasicInfo.stock_abbr,
        CompanyBasicInfo.company_name,
    )
    results = db.execute(stmt).all()
    lines = []
    for row in results:
        lines.append(f"{row.stock_code} {row.stock_abbr} ({row.company_name})")
    return "\n".join(lines)


def _invoke_llm(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 32768,
    temperature: float = 0.1,
) -> str:
    logger.info("调用LLM: prompt_chars=%d", len(system_prompt) + len(user_prompt))
    try:
        model = get_model.build_chat_model(
            max_tokens=max_tokens, temperature=temperature
        )
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
        response = model.invoke(messages)
    except Exception as exc:
        logger.error("LLM调用失败: error=%s", str(exc))
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "LLM调用失败") from exc

    content = getattr(response, "content", None)
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        text_parts = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                text_parts.append(str(block.get("text", "")))
        return "".join(text_parts).strip()
    return ""


def _extract_json_from_response(response_text: str) -> dict | None:
    json_match = re.search(r"\{[\s\S]*\}", response_text)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        return None


def process_chat_message(
    session_id: str | None,
    question: str,
    db: Session,
    question_id: str | None = None,
    chart_sequence: int = 1,
) -> ChatResponse:
    if session_id is None:
        session_id = str(uuid.uuid4())
        chat_session = ChatSession(id=session_id, status=0, context_slots={})
        db.add(chat_session)
        db.flush()
        logger.info("创建新会话: session_id=%s", session_id)
    else:
        chat_session = db.get(ChatSession, session_id)
        if chat_session is None:
            raise ServiceException(
                ErrorCode.DATA_NOT_FOUND, f"会话不存在: {session_id}"
            )

    user_message = ChatMessage(
        session_id=session_id,
        role="user",
        content=question,
    )
    db.add(user_message)
    db.flush()

    context_slots = chat_session.context_slots or {}

    resolved_question = _resolve_coreference(question, context_slots)
    if resolved_question != question:
        logger.info("指代消解: '%s' -> '%s'", question, resolved_question)

    intent = _parse_intent(resolved_question, context_slots, db)
    intent = _repair_intent_from_question(intent)

    intent = _merge_context(session_id, intent, context_slots)

    if intent.is_unsupported():
        unsupported_keyword = _detect_unsupported(resolved_question)
        hint = UNSUPPORTED_METRIC_HINTS.get(
            unsupported_keyword, "当前数据源不支持该查询"
        )
        unsupported_msg = f"抱歉，{hint}。请尝试其他问题或换一种表述方式。"
        assistant_message = ChatMessage(
            session_id=session_id,
            role="assistant",
            content=unsupported_msg,
            intent_result=intent.model_dump(),
        )
        db.add(assistant_message)
        db.flush()

        chat_session.context_slots = intent.model_dump()
        db.commit()

        return ChatResponse(
            session_id=session_id,
            answer=AnswerContent(content=unsupported_msg),
            need_clarification=False,
            sql=None,
        )

    if intent.is_partial_support():
        all_unsupported = _detect_all_unsupported_keywords(resolved_question)
        intent.unsupported_keywords = all_unsupported
        logger.info("部分支持场景，检测到不支持关键词: %s", all_unsupported)

    need_business_clarification, business_clarification_msg = (
        _handle_business_definition_clarification(resolved_question, intent)
    )
    # 部分支持场景下，即使检测到unsupported关键词，如果有可执行的metric则跳过澄清
    if need_business_clarification and intent.is_partial_support() and intent.metric:
        logger.info("部分支持场景，跳过unsupported澄清，继续执行可支持的查询")
        need_business_clarification = False
        business_clarification_msg = ""

    if need_business_clarification:
        assistant_message = ChatMessage(
            session_id=session_id,
            role="assistant",
            content=business_clarification_msg,
            intent_result=intent.model_dump(),
        )
        db.add(assistant_message)
        db.flush()

        chat_session.context_slots = intent.model_dump()
        db.commit()

        return ChatResponse(
            session_id=session_id,
            answer=AnswerContent(content=business_clarification_msg),
            need_clarification=True,
            sql=None,
        )

    missing_slots = _check_missing_slots(intent)
    if missing_slots:
        clarification = _generate_clarification(missing_slots, intent)
        assistant_message = ChatMessage(
            session_id=session_id,
            role="assistant",
            content=clarification,
            intent_result=intent.model_dump(),
        )
        db.add(assistant_message)
        db.flush()

        chat_session.context_slots = intent.model_dump()
        db.commit()

        return ChatResponse(
            session_id=session_id,
            answer=AnswerContent(content=clarification),
            need_clarification=True,
            sql=None,
        )

    sql = _normalize_sql_for_question(_generate_sql(intent, db), intent)
    is_valid, validate_msg = _validate_sql(sql)
    if not is_valid:
        logger.warning("SQL校验失败: %s, sql=%s", validate_msg, sql)
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR, f"生成的SQL不安全: {validate_msg}"
        )

    query_result, result_companies = _execute_query(sql, db)

    first_metric = intent.get_first_metric()
    metric_field = first_metric.get("field", "") if first_metric else ""
    is_prestored_derived = metric_field in PRESTORED_DERIVED_FIELDS
    if is_prestored_derived and intent.derived_metric_type:
        has_metric_values = _has_non_null_measure_values(query_result, metric_field)

        if not has_metric_values:
            logger.info(
                "预存派生字段 %s 查询结果缺少有效指标值，fallback到动态计算",
                metric_field,
            )
            template_sql = _generate_derived_metric_sql(
                intent, intent.derived_metric_type
            )
            if template_sql:
                normalized_template_sql = _normalize_sql_for_question(
                    template_sql, intent
                )
                is_valid_fallback, validate_msg_fallback = _validate_sql(
                    normalized_template_sql
                )
                if is_valid_fallback:
                    logger.info(
                        "使用派生指标模板fallback SQL: %s",
                        intent.derived_metric_type.value,
                    )
                    sql = normalized_template_sql
                    query_result, result_companies = _execute_query(sql, db)
            else:
                logger.info("派生指标模板返回None，让LLM重新生成SQL")
                fallback_intent = intent.model_dump()
                fallback_intent["derived_metric_type"] = (
                    intent.derived_metric_type.value
                    if intent.derived_metric_type
                    else "无"
                )
                config = _get_chat_config()
                sql_config = config.get("sql_generate", {})
                schema_ddl = _build_schema_ddl_text()
                system_prompt = sql_config.get("system_prompt", "").replace(
                    "{schema_ddl}", schema_ddl
                )
                user_prompt = sql_config.get("user_prompt_template", "").replace(
                    "{intent_json}", json.dumps(fallback_intent, ensure_ascii=False)
                ).replace(
                    "{derived_metric_type}",
                    intent.derived_metric_type.value if intent.derived_metric_type else "无",
                )
                response_text = _invoke_llm(
                    system_prompt, user_prompt, max_tokens=2048, temperature=0.0
                )
                fallback_sql = _extract_sql_from_response(response_text)
                if fallback_sql:
                    normalized_fallback_sql = _normalize_sql_for_question(
                        fallback_sql, intent
                    )
                    is_valid_fallback, validate_msg_fallback = _validate_sql(
                        normalized_fallback_sql
                    )
                    if is_valid_fallback:
                        logger.info("LLM重新生成fallback SQL成功")
                        sql = normalized_fallback_sql
                        query_result, result_companies = _execute_query(sql, db)

    from app.services import visualization as services_visualization

    chart_question_id = question_id or session_id[:8]
    chart_path, chart_type = services_visualization.generate_chart(
        data=query_result,
        intent=intent,
        question_id=chart_question_id,
        sequence=chart_sequence,
    )

    answer_text = _build_answer(question, query_result, intent)

    image_list = [_convert_path_to_url(chart_path)] if chart_path else []

    assistant_message = ChatMessage(
        session_id=session_id,
        role="assistant",
        content=answer_text,
        intent_result=intent.model_dump(),
        sql_query=sql,
        chart_paths=image_list,
    )
    db.add(assistant_message)
    db.flush()

    context_slots_to_save = intent.model_dump()
    if result_companies and len(result_companies) > 1:
        context_slots_to_save["last_result_companies"] = result_companies
        logger.info("保存上一轮筛选结果公司集合: %d家公司", len(result_companies))

    chat_session.context_slots = context_slots_to_save
    db.commit()

    return ChatResponse(
        session_id=session_id,
        answer=AnswerContent(content=answer_text, image=image_list),
        need_clarification=False,
        sql=sql,
        chart_type=chart_type,
    )


def _parse_intent(question: str, context_slots: dict, db: Session) -> IntentResult:
    config = _get_chat_config()
    intent_config = config.get("intent_parse", {})

    schema_info = _build_schema_info_text()
    company_list = _get_company_list(db)

    current_company = context_slots.get("company", "无")
    if isinstance(current_company, list):
        current_company = "、".join(
            [c.get("value", "") for c in current_company if isinstance(c, dict)]
        )
    elif isinstance(current_company, dict):
        current_company = current_company.get("value", "无")
    current_metric = context_slots.get("metric", "无")
    current_time = context_slots.get("time_range", "无")

    system_prompt = intent_config.get("system_prompt", "").replace(
        "{schema_info}", schema_info
    ).replace("{company_list}", company_list)
    user_prompt = intent_config.get("user_prompt_template", "").replace(
        "{question}", question
    ).replace("{current_company}", str(current_company)).replace(
        "{current_metric}", str(current_metric)
    ).replace("{current_time}", str(current_time))

    response_text = _invoke_llm(
        system_prompt, user_prompt, max_tokens=32768, temperature=0.0
    )
    logger.info("意图解析结果: %s", response_text[:500])

    parsed = _extract_json_from_response(response_text)
    if parsed is None:
        logger.warning("意图解析返回非JSON，使用默认值")
        return IntentResult(
            confidence=0.0, missing_slots=["company", "metric", "time_range"]
        )

    try:
        query_type = None
        if parsed.get("query_type"):
            try:
                query_type = QueryType(parsed["query_type"])
            except ValueError:
                query_type = QueryType.SINGLE_VALUE

        company_data = parsed.get("company")
        if company_data is not None:
            if isinstance(company_data, list):
                valid_companies = [
                    c for c in company_data if isinstance(c, dict) and c.get("value")
                ]
                if not valid_companies:
                    company_data = None
                elif len(valid_companies) == 1:
                    company_data = valid_companies[0]
                else:
                    company_data = valid_companies
            elif not isinstance(company_data, dict):
                company_data = None

        metric_data = _normalize_metric_payload(parsed.get("metric"))
        if isinstance(metric_data, list) and metric_data:
            first_metric_item = metric_data[0]
            metric_field = (
                first_metric_item.get("field", "")
                if isinstance(first_metric_item, dict)
                else ""
            )
            metric_table = (
                first_metric_item.get("table", "")
                if isinstance(first_metric_item, dict)
                else ""
            )
            has_component_fields = (
                first_metric_item.get("component_fields") is not None
                if isinstance(first_metric_item, dict)
                else False
            )
        elif isinstance(metric_data, dict):
            metric_field = (
                metric_data.get("field", "") if isinstance(metric_data.get("field"), str) else ""
            )
            metric_table = (
                metric_data.get("table", "") if isinstance(metric_data.get("table"), str) else ""
            )
            has_component_fields = metric_data.get("component_fields") is not None
        else:
            metric_field = ""
            metric_table = ""
            has_component_fields = False
        is_cross_table_query = "+" in metric_table or has_component_fields

        detected_derived_type = _detect_derived_metric(question)
        is_prestored_derived = metric_field and metric_field in PRESTORED_DERIVED_FIELDS

        if is_cross_table_query:
            derived_metric_type = None
            logger.info("检测到跨表查询需求，不使用派生指标模板，让LLM生成SQL")
        elif is_prestored_derived:
            derived_metric_type = detected_derived_type
            logger.info(
                "指标字段 %s 为数据库预存派生字段，将先查表再动态计算", metric_field
            )
        else:
            derived_metric_type = detected_derived_type
        capability = _classify_query_capability(
            question, metric_data, derived_metric_type
        )

        return IntentResult(
            company=company_data,
            metric=metric_data,
            time_range=_normalize_time_range(parsed.get("time_range")),
            ranking_time_range=_normalize_time_range(parsed.get("ranking_time_range")),
            calculation_time_range=_normalize_time_range(
                parsed.get("calculation_time_range")
            ),
            query_type=query_type,
            capability=capability,
            derived_metric_type=derived_metric_type,
            confidence=float(parsed.get("confidence", 0.0)),
            missing_slots=[],
            question=question,
        )
    except Exception as exc:
        logger.warning("意图解析结果构造失败: %s", exc)
        derived_metric_type = _detect_derived_metric(question)
        capability = _classify_query_capability(question, None, derived_metric_type)
        return IntentResult(
            capability=capability,
            derived_metric_type=derived_metric_type,
            confidence=0.0,
            missing_slots=["company", "metric", "time_range"],
            question=question,
        )


def _check_missing_slots(intent: IntentResult) -> list[str]:
    """检查槽位缺失情况，根据查询类型动态判断必需槽位"""
    missing = []
    question = intent.question or ""
    has_company = intent.company is not None and (
        (isinstance(intent.company, dict) and intent.company.get("value"))
        or (isinstance(intent.company, list) and len(intent.company) > 0)
    )
    has_result_companies = intent.has_last_result_companies()
    is_collection_reference = _references_collection_result(question)
    is_aggregation_question = _is_aggregation_collection_question(question)

    if not has_company and has_result_companies and is_collection_reference:
        has_company = True

    if is_collection_reference and not has_result_companies and not has_company:
        return ["last_result_companies"]

    # 派生指标场景：不强制要求company，优先检查metric和time_range
    if intent.derived_metric_type:
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range:
            missing.append("time_range")
        return missing

    # 集合查询场景：ranking、comparison、聚合问题不要求company槽位
    is_collection_query = intent.query_type in [QueryType.RANKING, QueryType.COMPARISON]
    if is_aggregation_question:
        is_collection_query = True

    if intent.query_type == QueryType.SINGLE_VALUE:
        if not is_collection_query and not has_company:
            missing.append("company")
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range:
            missing.append("time_range")
    elif intent.query_type == QueryType.TREND:
        if not is_collection_query and not has_company:
            missing.append("company")
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range:
            missing.append("time_range_for_trend")
    elif intent.query_type == QueryType.COMPARISON:
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range:
            missing.append("time_range")
    elif intent.query_type == QueryType.RANKING:
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range:
            missing.append("time_range")
    elif intent.query_type == QueryType.CONTINUITY:
        # 连续性查询需要：metric、time_range（或continuity_config中的时间范围）
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range and not intent.continuity_config:
            missing.append("time_range")
        continuity_cfg = intent.continuity_config or {}
        if not continuity_cfg.get("period_count"):
            # 如果LLM没有解析出连续期数，尝试从问题中提取
            if not _contains_continuity_keyword(intent.question or ""):
                missing.append("period_count")
    else:
        if not is_collection_query and not has_company:
            missing.append("company")
        if not intent.metric:
            missing.append("metric")
        if not intent.time_range:
            missing.append("time_range")
    return missing


def _generate_clarification(missing_slots: list[str], intent: IntentResult) -> str:
    config = _get_chat_config()
    templates = config.get("clarification", {}).get("templates", {})

    if len(missing_slots) == 1:
        slot = missing_slots[0]
        template = templates.get(slot, "请提供更多信息。")
        if slot == "last_result_companies":
            return template
        return _enrich_clarification(template, intent)

    parts = []
    for slot in missing_slots:
        template = templates.get(slot, "请提供{slot}信息。")
        if slot == "last_result_companies":
            parts.append(template)
            continue
        parts.append(_enrich_clarification(template, intent))
    return " ".join(parts)


def _enrich_clarification(template: str, intent: IntentResult) -> str:
    context_parts = []

    if intent.company is not None:
        if isinstance(intent.company, list):
            values = [
                c.get("value", "")
                for c in intent.company
                if isinstance(c, dict) and c.get("value")
            ]
            if values:
                context_parts.append(f"关于{'、'.join(values)}")
        elif isinstance(intent.company, dict) and intent.company.get("value"):
            context_parts.append(f"关于{intent.company['value']}")

    first_metric = intent.get_first_metric()
    if first_metric and first_metric.get("display_name"):
        context_parts.append(f"的{first_metric['display_name']}")
    if (
        intent.time_range
        and isinstance(intent.time_range, dict)
        and intent.time_range.get("report_year")
    ):
        context_parts.append(f"（{intent.time_range['report_year']}年）")

    if context_parts:
        return "".join(context_parts) + "，" + template

    return template


def _generate_sql(intent: IntentResult, db: Session) -> str:
    first_metric = intent.get_first_metric()
    metric_field = first_metric.get("field", "") if first_metric else ""
    is_prestored_derived = metric_field in PRESTORED_DERIVED_FIELDS

    multi_metric_topn_sql = _generate_multi_metric_topn_intersection_sql(intent)
    if multi_metric_topn_sql:
        logger.info("使用多指标TopN交集模板生成SQL")
        return multi_metric_topn_sql

    cross_table_topn_ratio_sql = _generate_cross_table_topn_ratio_sql(intent)
    if cross_table_topn_ratio_sql:
        logger.info("使用跨表TopN占比模板生成SQL")
        return cross_table_topn_ratio_sql

    if intent.query_type == QueryType.CONTINUITY:
        continuity_sql = _generate_continuity_sql(intent)
        if continuity_sql:
            logger.info("使用连续性查询模板生成SQL")
            return continuity_sql

    if (
        intent.derived_metric_type
        and intent.is_derived_query()
        and not is_prestored_derived
        and intent.capability != QueryCapability.AGGREGATION
    ):
        template_sql = _generate_derived_metric_sql(intent, intent.derived_metric_type)
        if template_sql:
            logger.info("使用派生指标模板生成SQL: %s", intent.derived_metric_type.value)
            return template_sql

    config = _get_chat_config()
    sql_config = config.get("sql_generate", {})

    schema_ddl = _build_schema_ddl_text()
    intent_json = json.dumps(intent.model_dump(), ensure_ascii=False)
    if is_prestored_derived:
        derived_metric_type_str = "无（预存字段，先查表）"
    else:
        derived_metric_type_str = (
            intent.derived_metric_type.value if intent.derived_metric_type else "无"
        )

    system_prompt = sql_config.get("system_prompt", "").replace(
        "{schema_ddl}", schema_ddl
    )
    user_prompt = sql_config.get("user_prompt_template", "").replace(
        "{intent_json}", intent_json
    ).replace("{derived_metric_type}", derived_metric_type_str)

    response_text = _invoke_llm(
        system_prompt, user_prompt, max_tokens=2048, temperature=0.0
    )
    logger.info("SQL生成结果: %s", response_text[:500])

    sql = _extract_sql_from_response(response_text)
    if not sql:
        raise ServiceException(ErrorCode.AI_SERVICE_ERROR, "LLM未能生成有效的SQL语句")
    return sql


def _extract_sql_from_response(response_text: str) -> str | None:
    code_block_match = re.search(
        r"```(?:sql)?\s*([\s\S]*?)```", response_text, re.IGNORECASE
    )
    cleaned_text = (
        code_block_match.group(1).strip()
        if code_block_match
        else response_text.strip()
    )

    sql_match = re.search(
        r"((?:WITH|SELECT)\b[\s\S]*?)(?:;|$)", cleaned_text, re.IGNORECASE
    )
    if sql_match:
        return sql_match.group(1).strip().rstrip(";")

    if re.match(r"(?i)^(WITH|SELECT)\b", cleaned_text):
        return cleaned_text.rstrip(";")

    return None


def _is_tcm_contest_universe_question(question: str) -> bool:
    return any(keyword in question for keyword in TCM_CONTEST_UNIVERSE_KEYWORDS)


def _extract_declared_cte_names(sql: str) -> set[str]:
    stripped_sql = sql.strip()
    with_match = re.match(r"(?is)^WITH(?:\s+RECURSIVE)?\s+", stripped_sql)
    if not with_match:
        return set()

    cte_names: set[str] = set()
    cursor = with_match.end()
    sql_length = len(stripped_sql)

    while cursor < sql_length:
        while cursor < sql_length and stripped_sql[cursor].isspace():
            cursor += 1

        cte_match = re.match(
            r"(?is)(\w+)\s*(?:\([^)]*\)\s*)?AS\s*\(",
            stripped_sql[cursor:],
        )
        if not cte_match:
            break

        cte_names.add(cte_match.group(1).lower())
        cursor += cte_match.end()

        depth = 1
        in_single_quote = False
        in_double_quote = False
        while cursor < sql_length and depth > 0:
            current_char = stripped_sql[cursor]
            next_char = stripped_sql[cursor + 1] if cursor + 1 < sql_length else ""

            if in_single_quote:
                if current_char == "'" and next_char == "'":
                    cursor += 2
                    continue
                if current_char == "'":
                    in_single_quote = False
            elif in_double_quote:
                if current_char == '"' and next_char == '"':
                    cursor += 2
                    continue
                if current_char == '"':
                    in_double_quote = False
            else:
                if current_char == "'":
                    in_single_quote = True
                elif current_char == '"':
                    in_double_quote = True
                elif current_char == "(":
                    depth += 1
                elif current_char == ")":
                    depth -= 1

            cursor += 1

        while cursor < sql_length and stripped_sql[cursor].isspace():
            cursor += 1

        if cursor < sql_length and stripped_sql[cursor] == ",":
            cursor += 1
            continue

        break

    return cte_names


def _normalize_sql_for_mysql_compatibility(sql: str) -> str:
    normalized_sql = sql.strip()

    distinct_topn_pattern = re.compile(
        r"""
        SELECT\s+DISTINCT\s+(?P<selected_col>\w+)\s+
        FROM\s+(?P<table_name>\w+)\s*
        (?P<where_clause>[\s\S]*?)
        ORDER\s+BY\s+(?P<order_expr>[\w\.]+)\s+(?P<direction>ASC|DESC)\s+
        LIMIT\s+(?P<limit>\d+)
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    def _rewrite_distinct_topn(match: re.Match[str]) -> str:
        selected_col = match.group("selected_col")
        table_name = match.group("table_name")
        where_clause = match.group("where_clause").rstrip()
        order_expr = match.group("order_expr")
        direction = match.group("direction")
        limit = match.group("limit")

        if re.search(r"\bGROUP\s+BY\b", where_clause, re.IGNORECASE):
            return match.group(0)

        if order_expr.split(".")[-1].lower() == selected_col.lower():
            return match.group(0)

        rewritten_where_clause = f"{where_clause}\n        GROUP BY {selected_col}"
        return (
            f"SELECT {selected_col}\n"
            f"            FROM {table_name}{rewritten_where_clause}\n"
            f"            ORDER BY MAX({order_expr}) {direction}\n"
            f"            LIMIT {limit}"
        )

    rewritten_sql = distinct_topn_pattern.sub(_rewrite_distinct_topn, normalized_sql)
    if rewritten_sql != normalized_sql:
        logger.info("已重写 DISTINCT + ORDER BY TopN 子查询，兼容 MySQL 执行规则")

    return rewritten_sql


def _normalize_sql_for_question(sql: str, intent: IntentResult) -> str:
    question = intent.question or ""
    normalized_sql = _normalize_sql_for_mysql_compatibility(sql.strip())

    if not question or not _is_tcm_contest_universe_question(question):
        return normalized_sql

    original_sql = normalized_sql
    for pattern in TCM_CONTEST_INDUSTRY_SQL_PATTERNS:
        normalized_sql = re.sub(
            rf"(?i)\b(WHERE|AND)\s+{pattern}",
            lambda match: f"{match.group(1)} 1=1",
            normalized_sql,
        )

    if normalized_sql != original_sql:
        logger.info("已中和中药样本问题中的行业过滤条件")

    return normalized_sql


def _ensure_non_empty_qa_pairs(
    question_json_str: str,
    qa_pairs: list[dict],
) -> list[dict]:
    if qa_pairs:
        return qa_pairs

    try:
        rounds = json.loads(question_json_str)
    except (json.JSONDecodeError, TypeError):
        rounds = [{"Q": question_json_str}]

    first_question = ""
    if isinstance(rounds, list) and rounds:
        first_round = rounds[0]
        first_question = (
            first_round.get("Q", "")
            if isinstance(first_round, dict)
            else str(first_round)
        )

    fallback_question = first_question.strip() or str(question_json_str)
    return [
        {
            "Q": fallback_question,
            "A": {
                "content": "回答生成失败：未生成任何有效轮次结果，请重新执行该题。"
            },
        }
    ]


def _validate_sql(sql: str) -> tuple[bool, str]:
    stripped_sql = sql.strip()
    sql_upper = stripped_sql.upper()

    for keyword in FORBIDDEN_KEYWORDS:
        pattern = r"\b" + keyword + r"\b"
        if re.search(pattern, sql_upper):
            return False, f"SQL包含禁止关键字: {keyword}"

    if not re.match(r"(?i)^(WITH|SELECT)\b", stripped_sql):
        return False, "SQL必须以SELECT或WITH开头"

    allowed_table_names = {table_name.lower() for table_name in ALLOWED_TABLES}
    allowed_table_names.update(_extract_declared_cte_names(stripped_sql))

    found_tables = re.findall(r"(?i)\b(?:FROM|JOIN)\s+(\w+)", stripped_sql)
    for table_name in found_tables:
        if table_name.lower() not in allowed_table_names:
            return False, f"SQL引用了不允许的表: {table_name}"

    raw_columns = _extract_select_columns(stripped_sql)
    bare_refs = _extract_column_refs_from_select(raw_columns)
    allowed_column_names = ALLOWED_COLUMN_NAMES | _extract_declared_column_aliases(
        stripped_sql
    )
    for ref in bare_refs:
        if ref.lower() not in allowed_column_names:
            logger.warning("SQL列名可能无效: '%s' (不在schema白名单中)", ref)
            return False, f"SQL列名不在schema白名单中: {ref}"

    if "LIMIT" not in sql_upper:
        sql = stripped_sql.rstrip(";") + " LIMIT 1000"

    return True, ""


def _execute_query(sql: str, db: Session) -> tuple[list[dict], list[dict]]:
    """执行SQL查询，返回(查询结果, 公司列表)"""
    try:
        result = db.execute(text(sql))
        columns = list(result.keys())
        rows = result.fetchall()
        data = [dict(zip(columns, row)) for row in rows]
        data = _normalize_abnormal_unit_rows(data)
        data = _apply_post_normalization_sql_adjustments(data, sql)

        companies = []
        company_columns = ["stock_abbr", "stock_code", "company_name", "company"]
        for row in data:
            company_info = None
            for col in company_columns:
                if col in row and row[col]:
                    company_info = {"value": str(row[col]), "type": col}
                    break
            if company_info and company_info not in companies:
                companies.append(company_info)

        return data, companies
    except Exception as exc:
        logger.error("SQL执行失败: sql=%s error=%s", sql, str(exc))
        raise ServiceException(
            ErrorCode.AI_SERVICE_ERROR, f"SQL查询执行失败: {str(exc)}"
        ) from exc


def _to_decimal_value(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, Decimal):
        return value

    if isinstance(value, int):
        return Decimal(value)

    if isinstance(value, float):
        return Decimal(str(value))

    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if not cleaned or cleaned.endswith("%"):
            return None
        try:
            return Decimal(cleaned)
        except InvalidOperation:
            return None

    return None


def _restore_numeric_type(original_value: Any, normalized_value: Decimal) -> Any:
    quantized_value = normalized_value.quantize(Decimal("0.01"))

    if isinstance(original_value, Decimal):
        return quantized_value

    if isinstance(original_value, int):
        if quantized_value == quantized_value.to_integral_value():
            return int(quantized_value)
        return float(quantized_value)

    if isinstance(original_value, float):
        return float(quantized_value)

    if isinstance(original_value, str):
        return format(quantized_value, "f")

    return quantized_value


def _median_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None

    sorted_values = sorted(values)
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2 == 1:
        return sorted_values[mid]

    return (sorted_values[mid - 1] + sorted_values[mid]) / Decimal("2")


def _is_ten_thousand_unit_result_column(column: str) -> bool:
    normalized_column = _normalize_result_column_name(column)
    return (
        normalized_column in TEN_THOUSAND_UNIT_COLUMN_NAMES
        or "万元" in str(column)
    )


def _normalize_abnormal_unit_rows(rows: list[dict]) -> list[dict]:
    """仅对标注为万元的异常大值做元->万元统一。"""
    if not rows:
        return rows

    normalized_rows = [dict(row) for row in rows]
    all_columns: list[str] = []
    seen_columns: set[str] = set()
    for row in normalized_rows:
        for column in row.keys():
            if column not in seen_columns:
                seen_columns.add(column)
                all_columns.append(column)

    candidate_columns = [
        column
        for column in all_columns
        if _is_ten_thousand_unit_result_column(column)
    ]

    for column in candidate_columns:
        column_values = [_to_decimal_value(row.get(column)) for row in normalized_rows]
        non_zero_values = [
            abs(value)
            for value in column_values
            if value is not None and value != 0
        ]
        if not non_zero_values:
            continue

        for index, decimal_value in enumerate(column_values):
            if decimal_value is None:
                continue

            abs_value = abs(decimal_value)
            if abs_value < ABNORMAL_TEN_THOUSAND_UNIT_THRESHOLD:
                continue

            peer_values = [
                abs(value)
                for peer_index, value in enumerate(column_values)
                if peer_index != index and value is not None and value != 0
            ]
            peer_median = _median_decimal(peer_values)
            if (
                peer_median is not None
                and peer_median > 0
                and abs_value < peer_median * ABNORMAL_TEN_THOUSAND_UNIT_RATIO
            ):
                continue

            normalized_value = decimal_value / Decimal("10000")
            normalized_rows[index][column] = _restore_numeric_type(
                rows[index].get(column),
                normalized_value,
            )
            logger.warning(
                "检测到疑似元/万元混用，已自动按万元统一: column=%s raw=%s normalized=%s",
                column,
                decimal_value,
                normalized_rows[index][column],
            )

    return normalized_rows


def _extract_where_clause(sql: str) -> str:
    match = re.search(
        r"(?is)\bWHERE\b(?P<where>.*?)(?=\bORDER\s+BY\b|\bGROUP\s+BY\b|\bLIMIT\b|$)",
        sql,
    )
    return match.group("where") if match else ""


def _extract_order_by_clause(sql: str) -> str:
    match = re.search(
        r"(?is)\bORDER\s+BY\b(?P<order>.*?)(?=\bLIMIT\b|$)",
        sql,
    )
    return match.group("order") if match else ""


def _apply_post_normalization_sql_adjustments(rows: list[dict], sql: str) -> list[dict]:
    """对归一化后的结果，重应用万元字段的简单数值过滤与排序。"""
    if not rows:
        return rows

    adjusted_rows = list(rows)
    result_columns = {str(column) for row in adjusted_rows for column in row.keys()}

    where_clause = _extract_where_clause(sql)
    if where_clause:
        numeric_predicates = re.findall(
            r"(?i)(?:\b\w+\.)?(\w+)\s*(>=|<=|>|<|=)\s*(-?\d+(?:\.\d+)?)",
            where_clause,
        )
        for column, operator, raw_threshold in numeric_predicates:
            if column not in result_columns or not _is_ten_thousand_unit_result_column(column):
                continue

            threshold = Decimal(raw_threshold)

            def _matches(row: dict) -> bool:
                value = _to_decimal_value(row.get(column))
                if value is None:
                    return False
                if operator == ">":
                    return value > threshold
                if operator == ">=":
                    return value >= threshold
                if operator == "<":
                    return value < threshold
                if operator == "<=":
                    return value <= threshold
                return value == threshold

            adjusted_rows = [row for row in adjusted_rows if _matches(row)]

    if not adjusted_rows:
        return adjusted_rows

    order_clause = _extract_order_by_clause(sql)
    if order_clause:
        order_match = re.match(
            r"(?is)\s*(?:\b\w+\.)?(\w+)(?:\s+(ASC|DESC))?",
            order_clause.strip(),
        )
        if order_match:
            order_column = order_match.group(1)
            direction = (order_match.group(2) or "ASC").upper()
            if order_column in result_columns and _is_ten_thousand_unit_result_column(order_column):
                adjusted_rows = sorted(
                    adjusted_rows,
                    key=lambda row: _to_decimal_value(row.get(order_column))
                    if _to_decimal_value(row.get(order_column)) is not None
                    else Decimal("-Infinity"),
                    reverse=direction == "DESC",
                )

    return adjusted_rows


def _build_multi_metric_topn_intersection_answer(
    question: str,
    query_result: list[dict],
    intent: IntentResult,
) -> str | None:
    if not _is_multi_metric_topn_intersection_question(intent) or not query_result:
        return None

    metrics = intent.get_metric_list()
    topn_limit = _extract_topn_limit(question)
    if topn_limit is None:
        return None
    time_range = intent.time_range or {}
    report_year = time_range.get("report_year")
    report_period = time_range.get("report_period")
    period_display_map = {
        "Q1": "第一季度",
        "HY": "半年度",
        "Q3": "第三季度",
        "FY": "年度",
    }
    period_display = (
        f"{report_year}年{period_display_map.get(report_period, report_period)}"
        if report_year and report_period
        else "目标报告期"
    )

    headers = ["股票代码", "股票简称"] + [
        str(metric.get("display_name") or metric.get("field") or f"指标{index + 1}")
        for index, metric in enumerate(metrics)
    ]
    metric_fields = [str(metric.get("field") or "") for metric in metrics]

    def _format_cell(value: Any) -> str:
        if isinstance(value, Decimal):
            return f"{value:,.2f}"
        if isinstance(value, float):
            return f"{value:,.2f}"
        if isinstance(value, int):
            return str(value)
        return str(value) if value is not None else "-"

    lines = [
        "## 分析结果",
        "",
        (
            f"根据查询结果，{period_display}同时满足所给指标均排名前{topn_limit}的公司"
            f"共有 **{len(query_result)} 家**。"
        ),
        "",
        "|" + "|".join(headers) + "|",
        "|" + "|".join(["---"] * len(headers)) + "|",
    ]

    for row in query_result:
        cells = [
            _format_cell(row.get("stock_code")),
            _format_cell(row.get("stock_abbr")),
        ] + [_format_cell(row.get(field)) for field in metric_fields]
        lines.append("| " + " | ".join(cells) + " |")

    lines.extend(
        [
            "",
            "### 说明",
            "",
            (
                f"- “均排名前{topn_limit}”表示先分别对各指标做排名，再取交集；"
                f"因此最终结果少于 {topn_limit} 家是正常情况，不代表查询不完整。"
            ),
            "- 表中金额单位按题目要求统一为万元。",
        ]
    )

    return "\n".join(lines)


def _format_time_range_label(time_range: dict | None) -> str:
    if not isinstance(time_range, dict):
        return "目标报告期"

    report_year = time_range.get("report_year")
    report_period = time_range.get("report_period")
    period_display_map = {
        "Q1": "第一季度",
        "HY": "半年度",
        "Q3": "第三季度",
        "FY": "年度",
    }

    if isinstance(report_year, int) and isinstance(report_period, str):
        return f"{report_year}年{period_display_map.get(report_period, report_period)}"
    if isinstance(report_year, int):
        return f"{report_year}年"
    return "目标报告期"


def _build_cross_table_topn_ratio_answer(
    question: str,
    query_result: list[dict],
    intent: IntentResult,
) -> str | None:
    if not _is_cross_table_topn_ratio_question(intent.question or question):
        return None

    ranking_label = _format_time_range_label(intent.ranking_time_range or intent.time_range)
    calculation_label = _format_time_range_label(
        intent.calculation_time_range or intent.time_range
    )
    topn_limit = _extract_topn_limit(question) or len(query_result)

    if not query_result:
        return (
            "## 分析结果\n\n"
            f"未检索到{ranking_label}未分配利润排名前{topn_limit}的公司数据，当前无法继续计算比例。"
        )

    valid_rows = [
        row
        for row in query_result
        if row.get("net_profit") is not None and row.get("ratio_percent") is not None
    ]

    def _format_cell(value: Any) -> str:
        if isinstance(value, Decimal):
            return f"{value:,.2f}"
        if isinstance(value, float):
            return f"{value:,.2f}"
        if isinstance(value, int):
            return str(value)
        if value is None:
            return "-"
        return str(value)

    lines = ["## 分析结果", ""]
    if valid_rows:
        lines.append(
            f"{ranking_label}未分配利润排名前{topn_limit}的公司中，"
            f"共 **{len(valid_rows)} 家** 能够匹配到{calculation_label}净利润数据并计算占比。"
        )
    else:
        lines.append(
            f"{ranking_label}未分配利润排名前{topn_limit}的公司已识别，"
            f"但缺少{calculation_label}净利润数据，当前无法计算“净利润占未分配利润比例”。"
        )
    lines.extend(
        [
            "",
            "| 股票代码 | 股票简称 | 未分配利润（万元） | 净利润（万元） | 比例（%） |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )

    for row in query_result:
        lines.append(
            "| "
            + " | ".join(
                [
                    _format_cell(row.get("stock_code")),
                    _format_cell(row.get("stock_abbr")),
                    _format_cell(row.get("equity_unappropriated_profit")),
                    _format_cell(row.get("net_profit")),
                    _format_cell(row.get("ratio_percent")),
                ]
            )
            + " |"
        )

    if not valid_rows:
        lines.extend(
            [
                "",
                "### 说明",
                "",
                f"- 排名口径使用 {ranking_label} 的未分配利润。",
                f"- 计算口径需要 {calculation_label} 的净利润；当前该口径在结果中未匹配到有效数据。",
                "- 因此不能将查空误判为未分配利润缺失。",
            ]
        )

    return "\n".join(lines)


def _build_answer(question: str, query_result: list[dict], intent: IntentResult) -> str:
    structured_answer = _build_multi_metric_topn_intersection_answer(
        question, query_result, intent
    )
    if structured_answer:
        logger.info("使用多指标TopN交集模板构建回答")
        return structured_answer

    structured_answer = _build_cross_table_topn_ratio_answer(
        question, query_result, intent
    )
    if structured_answer:
        logger.info("使用跨表TopN占比模板构建回答")
        return structured_answer

    config = _get_chat_config()
    answer_config = config.get("answer_build", {})

    intent_json = json.dumps(intent.model_dump(), ensure_ascii=False)
    query_result_str = json.dumps(query_result, ensure_ascii=False, default=str)
    derived_metric_type_str = (
        intent.derived_metric_type.value if intent.derived_metric_type else "无"
    )

    # 处理部分支持场景：在user_prompt中添加说明
    partial_support_note = ""
    if intent.is_partial_support() and intent.unsupported_keywords:
        unsupported_list = "、".join(intent.unsupported_keywords)
        partial_support_note = (
            f"\n\n【重要提示】用户问题中包含数据库不支持的指标/内容：{unsupported_list}。"
            f"请在回答中明确说明这些指标无法查询，并返回其他可支持的指标查询结果。"
        )

    system_prompt = answer_config.get("system_prompt", "")
    user_prompt = (
        answer_config.get("user_prompt_template", "")
        .replace("{question}", question)
        .replace("{query_result}", query_result_str)
        .replace("{intent_json}", intent_json)
        .replace("{derived_metric_type}", derived_metric_type_str)
        + partial_support_note
    )

    response_text = _invoke_llm(
        system_prompt, user_prompt, max_tokens=32768, temperature=0.3
    )
    logger.info("回答构建完成: length=%d", len(response_text))
    return response_text


COREFERENCE_PATTERNS = [
    r"它[的]?",
    r"他[的]?",
    r"该公司[的]?",
    r"这家公司[的]?",
    r"那家公司[的]?",
    r"这个公司[的]?",
    r"那个公司[的]?",
    r"本公司[的]?",
    r"上述公司[的]?",
    r"该企业[的]?",
    r"这家企业[的]?",
]

COLLECTION_COREFERENCE_PATTERNS = [
    (r"其中", "其中"),
    (r"这些公司", "这些公司"),
    (r"那些公司", "那些公司"),
    (r"上述公司", "上述公司"),
    (r"这几家公司", "这几家公司"),
    (r"那几家公司", "那几家公司"),
    (r"筛选出的公司", "筛选出的公司"),
    (r"筛选出来的公司", "筛选出来的公司"),
    (r"这些企业", "这些企业"),
    (r"那些企业", "那些企业"),
    (r"上述企业", "上述企业"),
    (r"这几家企业", "这几家企业"),
    (r"那几家企业", "那几家企业"),
    (r"这[0-9一二三四五六七八九十两几]+家公司", "这N家公司"),
    (r"前[0-9一二三四五六七八九十两]+家公司", "前N家公司"),
    (r"上述[0-9一二三四五六七八九十两几]*家公司", "上述N家公司"),
    (r"这[0-9一二三四五六七八九十两几]+家企业", "这N家企业"),
    (r"前[0-9一二三四五六七八九十两]+家企业", "前N家企业"),
    (r"上述[0-9一二三四五六七八九十两几]*家企业", "上述N家企业"),
    (r"这[0-9一二三四五六七八九十两几]+家上市公司", "这N家上市公司"),
    (r"前[0-9一二三四五六七八九十两]+家上市公司", "前N家上市公司"),
]


def _resolve_coreference(question: str, context_slots: dict) -> str:
    if not context_slots:
        return question

    company_info = context_slots.get("company")
    last_result_companies = context_slots.get("last_result_companies")

    resolved = question

    if (
        last_result_companies
        and isinstance(last_result_companies, list)
        and len(last_result_companies) > 0
    ):
        values = [
            c.get("value", "")
            for c in last_result_companies
            if isinstance(c, dict) and c.get("value")
        ]
        if values:
            company_value = "、".join(values)
            for pattern, _ in COLLECTION_COREFERENCE_PATTERNS:
                if re.search(pattern, question):
                    resolved = re.sub(pattern, f"{company_value}中", resolved)
                    return resolved

    if company_info:
        company_value = None
        if isinstance(company_info, list):
            values = [
                c.get("value", "")
                for c in company_info
                if isinstance(c, dict) and c.get("value")
            ]
            company_value = "、".join(values) if values else None
        elif isinstance(company_info, dict):
            company_value = company_info.get("value", "")

        if company_value:
            for pattern in COREFERENCE_PATTERNS:
                resolved = re.sub(pattern, company_value, resolved)

    return resolved


def _merge_context(
    session_id: str, new_intent: IntentResult, context_slots: dict
) -> IntentResult:
    if not context_slots:
        return new_intent

    def _is_valid_company(company: dict | list[dict] | None) -> bool:
        if company is None:
            return False
        if isinstance(company, list):
            return any(isinstance(c, dict) and c.get("value") for c in company)
        if isinstance(company, dict):
            return bool(company.get("value"))
        return False

    def _get_company_value(company: dict | list[dict] | None) -> str | None:
        if company is None:
            return None
        if isinstance(company, list):
            values = [
                c.get("value")
                for c in company
                if isinstance(c, dict) and c.get("value")
            ]
            return "、".join(values) if values else None
        if isinstance(company, dict):
            return company.get("value")
        return None

    merged_company = new_intent.company
    if not _is_valid_company(merged_company):
        context_company = context_slots.get("company")
        if _is_valid_company(context_company):
            merged_company = context_company

    merged_metric = _normalize_metric_payload(new_intent.metric) or _normalize_metric_payload(
        context_slots.get("metric")
    )
    merged_time_range = new_intent.time_range or context_slots.get("time_range")
    merged_ranking_time_range = (
        new_intent.ranking_time_range or context_slots.get("ranking_time_range")
    )
    merged_calculation_time_range = (
        new_intent.calculation_time_range or context_slots.get("calculation_time_range")
    )
    merged_query_type = new_intent.query_type

    if not merged_query_type and context_slots.get("query_type"):
        try:
            merged_query_type = QueryType(context_slots["query_type"])
        except (ValueError, KeyError):
            pass

    def _metric_has_field(m):
        if isinstance(m, dict):
            return bool(m.get("field"))
        if isinstance(m, list) and m:
            first = m[0]
            return isinstance(first, dict) and bool(first.get("field"))
        return False

    if (
        merged_metric
        and not _metric_has_field(merged_metric)
        and context_slots.get("metric")
    ):
        merged_metric = context_slots["metric"]

    if merged_time_range and isinstance(merged_time_range, dict):
        if not merged_time_range.get("report_year") and context_slots.get("time_range"):
            merged_time_range = context_slots["time_range"]

    merged_capability = new_intent.capability
    if not merged_capability and context_slots.get("capability"):
        try:
            merged_capability = QueryCapability(context_slots["capability"])
        except (ValueError, KeyError):
            pass

    merged_derived_metric_type = new_intent.derived_metric_type
    if not merged_derived_metric_type and context_slots.get("derived_metric_type"):
        try:
            merged_derived_metric_type = DerivedMetricType(
                context_slots["derived_metric_type"]
            )
        except (ValueError, KeyError):
            pass

    merged_last_result_companies = new_intent.last_result_companies
    if not merged_last_result_companies and context_slots.get("last_result_companies"):
        merged_last_result_companies = context_slots.get("last_result_companies")

    return IntentResult(
        company=merged_company,
        metric=merged_metric,
        time_range=merged_time_range,
        ranking_time_range=merged_ranking_time_range,
        calculation_time_range=merged_calculation_time_range,
        query_type=merged_query_type,
        capability=merged_capability,
        derived_metric_type=merged_derived_metric_type,
        last_result_companies=merged_last_result_companies,
        confidence=new_intent.confidence,
        missing_slots=new_intent.missing_slots,
        question=new_intent.question,
    )


def _resolve_company(company_text: str, db: Session) -> dict | None:
    stmt = select(
        CompanyBasicInfo.stock_code,
        CompanyBasicInfo.stock_abbr,
        CompanyBasicInfo.company_name,
    )
    results = db.execute(stmt).all()

    for row in results:
        if company_text in (row.stock_code, row.stock_abbr, row.company_name):
            return {
                "value": row.stock_abbr,
                "type": "stock_abbr",
                "stock_code": row.stock_code,
            }

    for row in results:
        if company_text in row.company_name or company_text in row.stock_abbr:
            return {
                "value": row.stock_abbr,
                "type": "stock_abbr",
                "stock_code": row.stock_code,
            }

    return None


def _resolve_time_expression(time_text: str) -> dict | None:
    year_match = re.search(r"(\d{4})", time_text)
    year = int(year_match.group(1)) if year_match else None

    period = None
    for alias, code in PERIOD_ALIAS_MAP.items():
        if alias in time_text:
            period = code
            break

    is_range = any(
        kw in time_text for kw in ["近几年", "近三年", "近五年", "变化趋势", "趋势"]
    )

    if year is None and period is None:
        return None

    result = {"is_range": is_range}
    if year is not None:
        result["report_year"] = year
    if period is not None:
        result["report_period"] = period
    return result


def _resolve_metric(metric_text: str) -> dict | None:
    return METRIC_ALIAS_MAP.get(metric_text)


def get_chat_sessions(
    db: Session, page: int = 1, page_size: int = 10
) -> PaginatedResponse:
    query = db.query(ChatSession).filter(ChatSession.status == 0)
    total = query.count()
    offset = (page - 1) * page_size
    records = (
        query.order_by(ChatSession.updated_at.desc())
        .offset(offset)
        .limit(page_size)
        .all()
    )

    items = [
        ChatSessionResponse(
            id=r.id,
            name=r.name,
            status=r.status,
            created_at=r.created_at,
            updated_at=r.updated_at,
        )
        for r in records
    ]

    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    pagination = PaginationInfo(
        page=page, page_size=page_size, total=total, total_pages=total_pages
    )
    return PaginatedResponse(lists=items, pagination=pagination)


def get_chat_history(session_id: str, db: Session) -> list[ChatMessageResponse]:
    chat_session = db.get(ChatSession, session_id)
    if chat_session is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"会话不存在: {session_id}")

    stmt = (
        select(ChatMessage)
        .where(ChatMessage.session_id == session_id)
        .order_by(ChatMessage.created_at.asc())
    )
    messages = db.execute(stmt).scalars().all()

    return [
        ChatMessageResponse(
            id=m.id,
            role=m.role,
            content=m.content,
            sql=m.sql_query,
            image=[
                _convert_path_to_url(p) if p.startswith("/") or ":" in p else p
                for p in (m.chart_paths or [])
            ],
            created_at=m.created_at,
        )
        for m in messages
    ]


def close_chat_session(session_id: str, db: Session) -> bool:
    chat_session = db.get(ChatSession, session_id)
    if chat_session is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"会话不存在: {session_id}")

    chat_session.status = 1
    db.commit()
    logger.info("会话已关闭: session_id=%s", session_id)
    return True


def delete_chat_session(session_id: str, db: Session) -> bool:
    import os

    chat_session = db.get(ChatSession, session_id)
    if chat_session is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"会话不存在: {session_id}")

    stmt = select(ChatMessage).where(ChatMessage.session_id == session_id)
    messages = db.execute(stmt).scalars().all()

    chart_dir = os.path.join(os.getcwd(), "result")
    for m in messages:
        if m.chart_paths:
            for chart_url in m.chart_paths:
                try:
                    filename = os.path.basename(chart_url)
                    file_path = os.path.join(chart_dir, filename)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        logger.info("图表已删除: %s", file_path)
                except Exception as e:
                    logger.warning(
                        "删除图表失败: chart_url=%s error=%s", chart_url, str(e)
                    )
        db.delete(m)

    db.delete(chat_session)
    db.commit()
    logger.info("会话已删除: session_id=%s", session_id)
    return True


def rename_chat_session(session_id: str, name: str, db: Session) -> bool:
    chat_session = db.get(ChatSession, session_id)
    if chat_session is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"会话不存在: {session_id}")

    chat_session.name = name
    db.commit()
    logger.info("会话已重命名: session_id=%s, name=%s", session_id, name)
    return True


def export_result_2(questions: list[dict], db: Session) -> str:
    import os

    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)

    all_results = []

    for idx, item in enumerate(questions):
        question_id = item.get("id", f"B{idx + 1:03d}")
        question_json_str = item.get("question", "[]")

        try:
            rounds = json.loads(question_json_str)
        except (json.JSONDecodeError, TypeError):
            rounds = [{"Q": question_json_str}]

        if not isinstance(rounds, list):
            rounds = [{"Q": str(rounds)}]

        session_id = None
        qa_pairs = []
        all_sqls = []
        all_chart_types = []
        chart_sequence = 1

        for round_idx, round_item in enumerate(rounds):
            q_text = (
                round_item.get("Q", "")
                if isinstance(round_item, dict)
                else str(round_item)
            )
            if not q_text.strip():
                continue

            try:
                response = process_chat_message(
                    session_id=session_id,
                    question=q_text,
                    db=db,
                    question_id=question_id,
                    chart_sequence=chart_sequence,
                )
                session_id = response.session_id

                image_paths = []
                if response.answer and response.answer.image:
                    for img in response.answer.image:
                        filename = (
                            os.path.basename(img) if "/" in img or "\\" in img else img
                        )
                        image_paths.append(f"./result/{filename}")
                        chart_sequence += 1

                answer_content = response.answer.content if response.answer and response.answer.content else ""
                if not answer_content:
                    answer_content = "回答内容为空"

                answer_data = {
                    "Q": q_text,
                    "A": {
                        "content": answer_content,
                    },
                }
                if image_paths:
                    answer_data["A"]["image"] = image_paths
                    if response.chart_type:
                        chart_type_map = {
                            "line": "折线图",
                            "bar": "柱状图",
                            "pie": "饼图",
                            "horizontal_bar": "水平柱状图",
                            "grouped_bar": "分组柱状图",
                            "radar": "雷达图",
                            "histogram": "直方图",
                            "scatter": "散点图",
                            "box": "箱线图",
                        }
                        all_chart_types.append(
                            chart_type_map.get(response.chart_type, "图表")
                        )

                if response.sql:
                    all_sqls.append(response.sql)

                qa_pairs.append(answer_data)

                if response.need_clarification:
                    continue

            except Exception as exc:
                logger.error(
                    "批量问答失败: question_id=%s round=%d error=%s",
                    question_id,
                    round_idx,
                    str(exc),
                )
                error_msg = f"回答生成失败: {str(exc)}" if exc else "回答生成失败: 未知错误"
                qa_pairs.append(
                    {
                        "Q": q_text,
                        "A": {"content": error_msg},
                    }
                )

        qa_pairs = _ensure_non_empty_qa_pairs(question_json_str, qa_pairs)
        sql_query = "\n\n".join(all_sqls) if all_sqls else ""
        chart_type = "、".join(all_chart_types) if all_chart_types else "无"

        result_item = {
            "id": question_id,
            "question": question_json_str,
            "sql": sql_query,
            "chart_type": chart_type,
            "answer": qa_pairs,
        }
        all_results.append(result_item)

        ws = wb.create_sheet(title=question_id)
        ws.append(["编号", "问题", "SQL查询语句", "图形格式", "回答"])
        ws.append(
            [
                question_id,
                json.dumps(rounds, ensure_ascii=False),
                sql_query,
                chart_type,
                json.dumps(qa_pairs, ensure_ascii=False),
            ]
        )

        for col in ws.columns:
            max_length = 0
            for cell in col:
                try:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
                except Exception:
                    pass
            adjusted_width = min(max_length + 2, 60)
            ws.column_dimensions[col[0].column_letter].width = adjusted_width

    result_dir = os.path.join(os.getcwd(), "result")
    os.makedirs(result_dir, exist_ok=True)
    result_path = os.path.join(result_dir, "result_2.xlsx")
    wb.save(result_path)
    logger.info("result_2.xlsx 已生成: %s, 共 %d 个问题", result_path, len(questions))

    json_path = os.path.join(result_dir, "result_2.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    logger.info("result_2.json 已生成: %s", json_path)

    return result_path
