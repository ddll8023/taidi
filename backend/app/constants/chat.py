"""Chat 服务常量定义"""

from app.schemas import chat as schemas_chat

# ========== 常量==========

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

VALID_REPORT_PERIODS = frozenset(PERIOD_ALIAS_MAP.values())

DERIVED_METRIC_KEYWORDS = {
    schemas_chat.DerivedMetricType.YOY_GROWTH: [
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
    schemas_chat.DerivedMetricType.QOQ_GROWTH: [
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
    schemas_chat.DerivedMetricType.CAGR: [
        "复合增长率",
        "年均复合增长率",
        "CAGR",
        "年化增长率",
        "年均增长率",
        "复合增速",
        "年均复合增速",
    ],
    schemas_chat.DerivedMetricType.RATIO: [
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
    schemas_chat.DerivedMetricType.INDUSTRY_AVG: [
        "行业均值",
        "行业平均",
        "行业平均水平",
        "平均数",
        "全行业平均",
        "行业均值水平",
        "行业平均值",
    ],
    schemas_chat.DerivedMetricType.MEDIAN: [
        "中位数",
        "中位值",
        "中间值",
    ],
    schemas_chat.DerivedMetricType.CORRELATION: [
        "相关性",
        "相关系数",
        "关联度",
        "相关关系",
    ],
    schemas_chat.DerivedMetricType.DIFFERENCE: [
        "差值",
        "差额",
        "差异",
        "相差",
        "差",
        "波动",
        "变化幅度",
        "变动幅度",
    ],
    schemas_chat.DerivedMetricType.PERCENTAGE: [
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
