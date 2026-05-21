from enum import Enum


class ReportTypeEnum(str, Enum):
    """报告类型枚举"""

    REPORT = "REPORT"
    SUMMARY = "SUMMARY"


class ReportPeriodEnum(str, Enum):
    """报告期间枚举"""

    Q1 = "Q1"
    Q2 = "Q2"
    Q3 = "Q3"
    Q4 = "Q4"
    HY = "HY"
    FY = "FY"


class ExchangeEnum(str, Enum):
    """上市交易所枚举"""

    SH = "SH"
    SZ = "SZ"
    BJ = "BJ"
    DEFAULT = "NONE"


class ParseStatusEnum(int, Enum):
    """解析状态枚举"""

    PENDING = 0
    SUCCESS = 1
    FAIL = 2
    PARSING = 3


class ImportStatusEnum(int, Enum):
    """导入状态枚举"""

    PENDING = 0
    SUCCESS = 1
    FAIL = 2


FILE_COLUMN_MAPPING_DICT: dict[str, str] = {
    # 序号	股票代码	A股简称	公司名称	英文名称	所属证监会行业	上市交易所	证券类别	注册区域	注册资本	雇员人数	管理人员人数
    "source_row_no": "序号",
    "stock_code": "股票代码",
    "stock_abbr": "A股简称",
    "company_name": "公司名称",
    "english_name": "英文名称",
    "csrc_industry": "所属证监会行业",
    "listed_exchange": "上市交易所",
    "security_category": "证券类别",
    "registered_region": "注册区域",
    "registered_capital_raw": "注册资本",
    "employee_count": "雇员人数",
    "management_count": "管理人员人数",
}

EXCHANGE_ALIAS_MAP: dict[str, ExchangeEnum] = {
    "SH": ExchangeEnum.SH,  # 上海
    "SSE": ExchangeEnum.SH,  # 上海证券交易所英文缩写
    "上海证券交易所": ExchangeEnum.SH,
    "上交所": ExchangeEnum.SH,
    "SZ": ExchangeEnum.SZ,  # 深圳
    "SZSE": ExchangeEnum.SZ,  # 深圳证券交易所英文缩写
    "深圳证券交易所": ExchangeEnum.SZ,
    "深交所": ExchangeEnum.SZ,
    "BJ": ExchangeEnum.BJ,  # 北京
    "BSE": ExchangeEnum.BJ,  # 北京证券交易所英文缩写
    "北京证券交易所": ExchangeEnum.BJ,
    "北交所": ExchangeEnum.BJ,
}


# 报告标签 → (报告期间, 报告类型, 标准标签) 映射
REPORT_LABEL_TO_META: dict[str, tuple[ReportPeriodEnum, ReportTypeEnum, str]] = {
    "一季度报告": (ReportPeriodEnum.Q1, ReportTypeEnum.REPORT, "一季度报告"),
    "第一季度报告": (ReportPeriodEnum.Q1, ReportTypeEnum.REPORT, "一季度报告"),
    "二季度报告": (ReportPeriodEnum.Q2, ReportTypeEnum.REPORT, "二季度报告"),
    "第二季度报告": (ReportPeriodEnum.Q2, ReportTypeEnum.REPORT, "二季度报告"),
    "三季度报告": (ReportPeriodEnum.Q3, ReportTypeEnum.REPORT, "三季度报告"),
    "第三季度报告": (ReportPeriodEnum.Q3, ReportTypeEnum.REPORT, "三季度报告"),
    "四季度报告": (ReportPeriodEnum.Q4, ReportTypeEnum.REPORT, "四季度报告"),
    "第四季度报告": (ReportPeriodEnum.Q4, ReportTypeEnum.REPORT, "四季度报告"),
    "半年度报告": (ReportPeriodEnum.HY, ReportTypeEnum.REPORT, "半年度报告"),
    "半年度报告摘要": (
        ReportPeriodEnum.HY,
        ReportTypeEnum.SUMMARY,
        "半年度报告摘要",
    ),
    "年度报告": (ReportPeriodEnum.FY, ReportTypeEnum.REPORT, "年度报告"),
    "年度报告摘要": (
        ReportPeriodEnum.FY,
        ReportTypeEnum.SUMMARY,
        "年度报告摘要",
    ),
}

# 交易所 → 上市交易所全称
DERIVED_LISTED_EXCHANGE_MAP: dict[ExchangeEnum, str] = {
    ExchangeEnum.SH: "上海证券交易所",
    ExchangeEnum.SZ: "深圳证券交易所",
    ExchangeEnum.BJ: "北京证券交易所",
}


PERIOD_SORT_KEY_MAP: dict[ReportPeriodEnum, int] = {
    ReportPeriodEnum.Q1: 1,
    ReportPeriodEnum.Q2: 2,
    ReportPeriodEnum.Q3: 3,
    ReportPeriodEnum.Q4: 4,
    ReportPeriodEnum.HY: 5,
    ReportPeriodEnum.FY: 6,
}
