from pydantic import BaseModel, Field, ConfigDict


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
