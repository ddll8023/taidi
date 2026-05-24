from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime


# ========== 辅助类（Support）==========


class IdentifyIntentResultItem(BaseModel):
    """识别意图结果项"""

    companys: list[dict] | None = Field(
        default_factory=list, description="识别到的公司"
    )
    metrics: list[dict] | None = Field(default_factory=list, description="识别到的指标")
    time_range: list[dict] | None = Field(
        default_factory=list, description="识别到的时间列表，单期查询为单元素列表"
    )
    query_type: str | None = Field(None, description="查询类型")
    confidence: float = Field(0.0, description="置信度")
    continuity_config: dict | None = Field(
        default_factory=dict, description="连续性配置"
    )

    model_config = ConfigDict(from_attributes=True)


class ChatMessageItem(BaseModel):
    """聊天消息项"""

    id: int = Field(..., description="消息ID")
    query: str | None = Field(None, description="用户查询")
    answer: str | None = Field(None, description="回答")
    sql_query: str | None = Field(None, description="生成的SQL")
    sql_result: list | None = Field(None, description="SQL执行结果")
    intent_result: dict | None = Field(None, description="意图解析结果")
    created_at: datetime = Field(..., description="创建时间")

    model_config = ConfigDict(from_attributes=True)


# ========== 请求类（Request）==========


class StartChatRequest(BaseModel):
    """开始聊天请求"""

    session_id: str | None = Field(None, description="会话ID(新对话时可选)")
    question: str = Field(..., description="用户问题")


class GetChatListRequest(BaseModel):
    """获取聊天列表请求"""

    page: int = Field(1, description="页码")
    page_size: int = Field(10, description="每页数量")


class GetChatDetailRequest(BaseModel):
    """获取聊天详情请求"""

    session_id: str = Field(..., description="会话ID")


class DeleteChatSessionRequest(BaseModel):
    """删除聊天会话请求"""

    session_id: str = Field(..., description="会话ID")


# ========== 响应类（Response）==========


class GetChatListResponse(BaseModel):
    """获取聊天列表响应"""

    id: str = Field(..., description="会话ID")
    session_name: str = Field(..., description="会话名称")
    status: int = Field(..., description="会话状态")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")

    model_config = ConfigDict(from_attributes=True)


class GetChatDetailResponse(BaseModel):
    """获取聊天详情响应"""

    id: str = Field(..., description="会话ID")
    session_name: str = Field(..., description="会话名称")
    status: int = Field(..., description="会话状态")
    messages: list[ChatMessageItem] = Field(
        default_factory=list, description="消息列表"
    )
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")

    model_config = ConfigDict(from_attributes=True)


class DeleteChatSessionResponse(BaseModel):
    """删除聊天会话响应"""

    session_id: str = Field(..., description="会话ID")
    deleted: bool = Field(..., description="是否删除成功")
