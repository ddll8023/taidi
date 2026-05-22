from pydantic import BaseModel, Field, ConfigDict


class AnswerContentItem(BaseModel):
    """回答内容"""

    content: str = Field(..., description="回答文本(Markdown 格式)")
    image: list[str] | None = Field(
        default_factory=list, description="图表图片 URL 列表"
    )  # 图表图片 URL 列表

    model_config = ConfigDict(from_attributes=True)


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


class ChatResponse(BaseModel):
    """聊天响应"""

    session_id: str = Field(..., description="会话ID")
    answer: AnswerContentItem = Field(..., description="回答内容")  # 内部的嵌套模型
    sql: str | None = Field(None, description="SQL 语句")
    chart_type: str | None = Field(None, description="图表类型")

    model_config = ConfigDict(from_attributes=True)
