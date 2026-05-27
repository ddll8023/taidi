"""知识库管理常量"""

STOCK_RESEARCH_REPORT_COLUMN_MAP: dict[str, str] = {
    "title": "title",
    "stock_abbr": "stockName",
    "stock_code": "stockCode",
    "org_code": "orgCode",
    "org_name": "orgName",
    "publish_date": "publishDate",
    "predict_next_two_year_eps": "predictNextTwoYearEps",
    "predict_next_two_year_pe": "predictNextTwoYearPe",
    "predict_next_year_eps": "predictNextYearEps",
    "predict_next_year_pe": "predictNextYearPe",
    "predict_this_year_eps": "predictThisYearEps",
    "predict_this_year_pe": "predictThisYearPe",
    "predict_last_year_eps": "predictLastYearEps",
    "predict_last_year_pe": "predictLastYearPe",
    "industry_name": "indvInduName",
    "em_rating_name": "emRatingName",
    "last_em_rating_name": "lastEmRatingName",
    "indv_is_new": "indvIsNew",
    "researcher": "researcher",
    "new_listing_date": "newListingDate",
    "new_purchase_date": "newPurchaseDate",
    "new_issue_price": "newIssuePrice",
    "new_pe_issue_a": "newPeIssueA",
    "indv_aim_price_t": "indvAimPriceT",
    "indv_aim_price_l": "indvAimPriceL",
    "s_rating_name": "sRatingName",
    "s_rating_code": "sRatingCode",
    "market": "market",
}

INDUSTRY_REPORT_COLUMN_MAP: dict[str, str] = {
    "title": "title",
    "org_code": "orgCode",
    "org_name": "orgName",
    "org_S_Name": "orgSName",
    "publish_date": "publishDate",
    "industry_name": "industryName",
    "em_rating_name": "emRatingName",
    "last_em_rating_name": "lastEmRatingName",
    "researcher": "researcher",
    "s_rating_name": "sRatingName",
    "s_rating_code": "sRatingCode",
}

DOC_TYPE_RESEARCH_REPORT = "RESEARCH_REPORT"
DOC_TYPE_INDUSTRY_REPORT = "INDUSTRY_REPORT"

# DashScope 单次批量 Embedding 最大条数
EMBEDDING_BATCH_SIZE = 25

# 可重试的异常类型（网络错误、SSL 错误等）
RETRYABLE_EXCEPTIONS = (
    ConnectionError,
    OSError,
    TimeoutError,
)
