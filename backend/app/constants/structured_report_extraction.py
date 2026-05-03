"""结构化报告抽取服务常量定义"""

from typing import Any

CORE_TABLE_NAME = "core_performance_indicators_sheet"
TABLE_ORDER = (
    CORE_TABLE_NAME,
    "balance_sheet",
    "cash_flow_sheet",
    "income_sheet",
)
MAX_CONCURRENT_TABLES = 4
REPORT_PERIOD_DISPLAY = {
    "Q1": "一季度",
    "HY": "半年度",
    "Q3": "三季度",
    "FY": "年度",
}
SECTION_SPECS: dict[str, Any] = {
    CORE_TABLE_NAME: {
        "strong_keywords": (
            "主要会计数据和财务指标",
            "主要财务数据",
            "主要财务指标",
            "会计数据和财务指标",
            "主要会计数据",
        ),
        "weak_keywords": (
            "财务指标",
            "会计数据",
            "每股收益",
            "净资产收益率",
        ),
        "markers": (
            "单位：元",
            "币种：人民币",
            "营业收入",
            "净利润",
            "每股净资产",
            "经营现金流量",
        ),
        "window_config_key": "core_window_pages",
        "default_window_pages": 5,
        "primary_context_label": "主要财务数据页段",
    },
    "balance_sheet": {
        "strong_keywords": ("合并资产负债表", "1、合并资产负债表"),
        "weak_keywords": ("资产负债表",),
        "markers": ("编制单位", "单位：元", "项目", "期末余额", "流动资产"),
        "window_config_key": "statement_window_pages",
        "default_window_pages": 6,
        "primary_context_label": "合并资产负债表页段",
    },
    "cash_flow_sheet": {
        "strong_keywords": ("合并现金流量表", "5、合并现金流量表"),
        "weak_keywords": ("现金流量表",),
        "markers": (
            "编制单位",
            "单位：元",
            "项目",
            "经营活动产生的现金流量净额",
            "现金及现金等价物净增加额",
        ),
        "window_config_key": "statement_window_pages",
        "default_window_pages": 6,
        "primary_context_label": "合并现金流量表页段",
    },
    "income_sheet": {
        "strong_keywords": ("合并利润表", "3、合并利润表"),
        "weak_keywords": ("利润表",),
        "markers": ("单位：元", "项目", "营业总收入", "营业利润", "利润总额"),
        "window_config_key": "statement_window_pages",
        "default_window_pages": 6,
        "primary_context_label": "合并利润表页段",
    },
}
SHORT_PDF_THRESHOLD = 10
MISSING_ANCHOR_THRESHOLD = 2
SUMMARY_KEYWORDS = ("摘要", "年报摘要", "年度报告摘要")
