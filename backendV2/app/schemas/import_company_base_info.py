"""导入公司基本信息响应 Schema"""
from pydantic import BaseModel, Field, ConfigDict


# ========== 响应类（Response）==========


class ImportCompanyBaseInfoResponse(BaseModel):
    """导入公司基本信息响应"""

    total: int = Field(description="总条数", default=0)
    inserted: int = Field(description="插入条数", default=0)
    updated: int = Field(description="更新条数", default=0)

    model_config = ConfigDict(from_attributes=True)
