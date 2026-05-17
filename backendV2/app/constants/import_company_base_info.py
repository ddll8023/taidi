FILE_COLUMN_MAPPING_DICT = {
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

EXCHANGE_ALIAS_MAP = {
    "SH": "SH",  # 上海
    "SSE": "SH",  # 上海证券交易所英文缩写
    "上海证券交易所": "SH",
    "上交所": "SH",
    "SZ": "SZ",  # 深圳
    "SZSE": "SZ",  # 深圳证券交易所英文缩写
    "深圳证券交易所": "SZ",
    "深交所": "SZ",
    "BJ": "BJ",  # 北京
    "BSE": "BJ",  # 北京证券交易所英文缩写
    "北京证券交易所": "BJ",
    "北交所": "BJ",
}
