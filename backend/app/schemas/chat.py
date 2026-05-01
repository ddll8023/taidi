from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field

# ========== 辅助类（Support）==========


class QueryType(str, Enum):
    SINGLE_VALUE = "single_value"
    TREND = "trend"
    COMPARISON = "comparison"
    RANKING = "ranking"
    CONTINUITY = "continuity"  # 连续N期满足某条件的查询


class QueryCapability(str, Enum):
    DIRECT_FIELD = "direct_field"
    DERIVED_METRIC = "derived_metric"
    CROSS_TABLE = "cross_table"
    AGGREGATION = "aggregation"
    UNSUPPORTED = "unsupported"
    PARTIAL_SUPPORT = (
        "partial_support"  # 部分支持：问题中有不支持的部分，但其他部分可执行
    )


class DerivedMetricType(str, Enum):
    YOY_GROWTH = "yoy_growth"
    QOQ_GROWTH = "qoq_growth"
    CAGR = "cagr"
    RATIO = "ratio"
    INDUSTRY_AVG = "industry_avg"
    MEDIAN = "median"
    CORRELATION = "correlation"
    DIFFERENCE = "difference"
    PERCENTAGE = "percentage"


class IntentResult(BaseModel):
    company: dict | list[dict] | None = Field(
        None, description="公司信息，单公司为dict含value/type，多公司为list[dict]"
    )
    metric: dict | list[dict] | None = Field(
        None,
        description="指标信息，单指标为dict含field/table/display_name，多指标为list[dict]",
    )
    time_range: dict | None = Field(
        None, description="时间范围，含report_year/report_period/is_range"
    )
    ranking_time_range: dict | None = Field(
        None,
        description="排序口径时间范围，适用于先筛选TopN再计算其他指标的场景",
    )
    calculation_time_range: dict | None = Field(
        None,
        description="计算口径时间范围，适用于分子分母或排序口径不同的场景",
    )
    query_type: QueryType | None = Field(None, description="查询类型")
    capability: QueryCapability | None = Field(None, description="查询能力分类")
    derived_metric_type: DerivedMetricType | None = Field(
        None, description="派生指标类型"
    )
    continuity_config: dict | None = Field(
        None,
        description="连续性查询配置，包含period_count/condition/start_period/end_period",
    )
    last_result_companies: list[dict] | None = Field(
        None, description="上一轮查询结果中的公司集合"
    )
    confidence: float = Field(0.0, ge=0.0, le=1.0, description="置信度")
    missing_slots: list[str] = Field(default_factory=list, description="缺失的槽位列表")
    question: str | None = Field(None, description="原始问题文本，用于图表类型检测")
    unsupported_keywords: list[str] = Field(
        default_factory=list,
        description="问题中包含的不支持关键词列表（用于部分支持场景）",
    )

    model_config = ConfigDict(from_attributes=True)

    def is_multi_company(self) -> bool:
        """判断是否为多公司查询"""
        return isinstance(self.company, list) and len(self.company) > 1

    def get_company_list(self) -> list[dict]:
        """获取公司列表（统一返回列表格式）"""
        if self.company is None:
            return []
        if isinstance(self.company, list):
            return self.company
        return [self.company]

    def get_first_company(self) -> dict | None:
        """获取第一个公司信息"""
        companies = self.get_company_list()
        return companies[0] if companies else None

    def get_metric_list(self) -> list[dict]:
        """获取指标列表（统一返回列表格式）"""
        if self.metric is None:
            return []
        if isinstance(self.metric, list):
            return self.metric
        return [self.metric]

    def get_first_metric(self) -> dict | None:
        """获取第一个指标信息"""
        metrics = self.get_metric_list()
        return metrics[0] if metrics else None

    def is_multi_metric(self) -> bool:
        """判断是否为多指标查询"""
        return isinstance(self.metric, list) and len(self.metric) > 1

    def is_derived_query(self) -> bool:
        """判断是否为派生指标查询"""
        return self.capability == QueryCapability.DERIVED_METRIC

    def is_unsupported(self) -> bool:
        """判断是否为不可答查询"""
        return self.capability == QueryCapability.UNSUPPORTED

    def is_partial_support(self) -> bool:
        """判断是否为部分支持查询"""
        return self.capability == QueryCapability.PARTIAL_SUPPORT

    def has_last_result_companies(self) -> bool:
        """判断是否有上一轮筛选结果的公司集合"""
        return bool(self.last_result_companies and len(self.last_result_companies) > 0)


# ========== 请求类（Request）==========


class ChatRequest(BaseModel):
    session_id: str | None = Field(None, description="会话ID，新会话不传")
    question: str = Field(..., min_length=1, max_length=500, description="用户问题")


class ChatExportRequest(BaseModel):
    questions: list[dict] = Field(..., description="待回答问题列表")


class ChatRenameRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="会话名称")


# ========== 响应类（Response）==========


class AnswerContent(BaseModel):
    content: str = Field(..., description="回答文本内容")
    image: list[str] = Field(default_factory=list, description="图表路径列表")

    model_config = ConfigDict(from_attributes=True)


class ChatResponse(BaseModel):
    session_id: str = Field(..., description="会话ID")
    answer: AnswerContent = Field(..., description="回答内容")
    need_clarification: bool = Field(False, description="是否需要澄清")
    sql: str | None = Field(None, description="生成的SQL语句")
    chart_type: str | None = Field(
        None,
        description="图表类型: line/bar/pie/horizontal_bar/grouped_bar/radar/histogram/scatter/box",
    )

    model_config = ConfigDict(from_attributes=True)


class ChatSessionResponse(BaseModel):
    id: str = Field(..., description="会话UUID")
    name: str | None = Field(None, description="会话名称")
    status: int = Field(0, description="状态：0活跃 1已关闭")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")

    model_config = ConfigDict(from_attributes=True)


class ChatMessageResponse(BaseModel):
    id: int = Field(..., description="消息ID")
    role: str = Field(..., description="角色：user/assistant")
    content: str = Field(..., description="消息内容")
    sql: str | None = Field(None, description="生成的SQL")
    image: list[str] = Field(default_factory=list, description="图表路径列表")
    created_at: datetime = Field(..., description="创建时间")

    model_config = ConfigDict(from_attributes=True)


class ChatExportResponse(BaseModel):
    file_path: str = Field(..., description="导出文件路径")

    model_config = ConfigDict(from_attributes=True)
