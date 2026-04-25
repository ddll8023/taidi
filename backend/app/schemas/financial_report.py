from datetime import date, datetime
from enum import IntEnum

from pydantic import BaseModel, ConfigDict


# ========== 辅助类（Support）==========  # 内部 Enum、通用对象类


class ParseStatus(IntEnum):
    PENDING = 0
    SUCCESS = 1
    FAILED = 2
    PROCESSING = 3


class ReviewStatus(IntEnum):
    PENDING = 0
    APPROVED = 1
    REJECTED = 2


class ValidateStatus(IntEnum):
    PENDING = 0
    PASSED = 1
    FAILED = 2


class ImportStatus(IntEnum):
    PENDING = 0
    SUCCESS = 1
    FAILED = 2


class VectorStatus(IntEnum):
    PENDING = 0
    PROCESSING = 1
    SUCCESS = 2
    FAILED = 3
    SKIPPED = 4


# ========== 请求类（Request）==========  # 入参校验


# ========== 响应类（Response）==========  # 返回数据结构


class FinancialReportResponse(BaseModel):
    id: int
    stock_code: str
    stock_abbr: str
    report_year: int
    report_period: str
    report_type: str
    report_label: str
    exchange: str
    report_title: str
    report_date: date | None = None
    period_sort_key: int = 0
    source_priority: int = 0
    source_file_name: str
    storage_path: str
    structured_json_path: str | None = None
    parse_status: int = 0
    review_status: int = 0
    validate_status: int = 0
    validate_message: str | None = None
    import_status: int = 0
    vector_status: int = 0
    vector_model: str | None = None
    vector_dim: int | None = None
    vector_version: str | None = None
    vector_error_message: str | None = None
    vectorized_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)
