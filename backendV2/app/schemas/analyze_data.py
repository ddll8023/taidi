from datetime import date

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


# ========== 请求类（Request）==========


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