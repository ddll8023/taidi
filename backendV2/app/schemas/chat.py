from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime


class IdentifyIntentResultItem(BaseModel):
    """识别意图结果项"""

    companys: list[dict] | None = Field(
        default_factory=list, description="识别到的公司"
    )
    metrics: list[dict] | None = Field(default_factory=list, description="识别到的指标")
    time_range: dict | None = Field(default_factory=dict, description="识别到的时间")
    query_type: str | None = Field(None, description="查询类型")
    confidence: float = Field(0.0, description="置信度")
    continuity_config: dict | None = Field(
        default_factory=dict, description="连续性配置"
    )


class StartChatRequest(BaseModel):
    """开始聊天请求"""

    session_id: str | None = Field(None, description="会话ID(新对话时可选)")
    question: str = Field(..., description="用户问题")


class GetChatListRequest(BaseModel):
    """获取聊天列表请求"""

    page: int = Field(1, description="页码")
    page_size: int = Field(10, description="每页数量")


class GetChatListResponse(BaseModel):
    """获取聊天列表响应"""

    model_config = ConfigDict(from_attributes=True)

    id: str = Field(..., description="会话ID")
    session_name: str = Field(..., description="会话名称")
    status: int = Field(..., description="会话状态")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
