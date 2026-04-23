from datetime import datetime
from enum import Enum, IntEnum

from pydantic import BaseModel, ConfigDict, Field


class StepType(str, Enum):
    SQL_QUERY = "sql_query"
    DERIVE_METRIC = "derive_metric"
    RETRIEVE_EVIDENCE = "retrieve_evidence"
    AGGREGATE = "aggregate"
    VERIFY = "verify"
    COMPOSE_ANSWER = "compose_answer"


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class TaskStep(BaseModel):
    step_id: str = Field(..., description="步骤唯一标识，如 s1, s2")
    step_type: StepType = Field(..., description="步骤类型")
    goal: str = Field(..., description="步骤目标描述")
    depends_on: list[str] = Field(default_factory=list, description="依赖的步骤ID列表")
    params: dict = Field(default_factory=dict, description="步骤参数")
    priority: int = Field(0, description="执行优先级，数字越小越先执行")

    model_config = ConfigDict(from_attributes=True)


class ExecutionPlan(BaseModel):
    question: str = Field(..., description="原始用户问题")
    steps: list[TaskStep] = Field(default_factory=list, description="子任务步骤列表")
    context: dict = Field(default_factory=dict, description="规划上下文（公司、时间等）")
    created_at: datetime | None = Field(None, description="计划创建时间")

    model_config = ConfigDict(from_attributes=True)

    def get_step(self, step_id: str) -> TaskStep | None:
        for step in self.steps:
            if step.step_id == step_id:
                return step
        return None

    def get_steps_by_type(self, step_type: StepType) -> list[TaskStep]:
        return [s for s in self.steps if s.step_type == step_type]

    def get_ordered_steps(self) -> list[TaskStep]:
        return sorted(self.steps, key=lambda s: s.priority)


class StepResult(BaseModel):
    step_id: str = Field(..., description="对应的步骤ID")
    step_type: StepType = Field(..., description="步骤类型")
    status: StepStatus = Field(..., description="执行状态")
    output: dict = Field(default_factory=dict, description="执行输出结果")
    error_message: str | None = Field(None, description="错误信息")
    execution_time_ms: int | None = Field(None, description="执行耗时（毫秒）")

    model_config = ConfigDict(from_attributes=True)


class ExecutionTrace(BaseModel):
    plan: ExecutionPlan = Field(..., description="执行计划")
    results: list[StepResult] = Field(default_factory=list, description="各步骤执行结果")
    final_answer: str | None = Field(None, description="最终答案")
    references: list[dict] = Field(default_factory=list, description="引用来源列表")
    started_at: datetime | None = Field(None, description="开始时间")
    finished_at: datetime | None = Field(None, description="结束时间")

    model_config = ConfigDict(from_attributes=True)

    def get_result(self, step_id: str) -> StepResult | None:
        for result in self.results:
            if result.step_id == step_id:
                return result
        return None

    def is_step_ready(self, step_id: str) -> bool:
        step = self.plan.get_step(step_id)
        if step is None:
            return False
        for dep_id in step.depends_on:
            dep_result = self.get_result(dep_id)
            if dep_result is None or dep_result.status != StepStatus.COMPLETED:
                return False
        return True


class Reference(BaseModel):
    paper_path: str | None = Field(None, description="文档路径")
    text: str = Field(..., description="支撑结论的摘要证据")
    page_no: int | None = Field(None, description="页码")
    paper_image: str | None = Field(None, description="图表或页图路径")

    model_config = ConfigDict(from_attributes=True)


class Task3AnswerContent(BaseModel):
    content: str = Field(..., description="回答文本内容")
    image: list[str] = Field(default_factory=list, description="图表路径列表")
    references: list[Reference] = Field(default_factory=list, description="引用来源列表")

    model_config = ConfigDict(from_attributes=True)


class Task3Response(BaseModel):
    question_id: str | None = Field(None, description="问题编号")
    answer: Task3AnswerContent = Field(..., description="回答内容")
    sql: str | None = Field(None, description="生成的SQL语句")
    chart_type: str | None = Field(None, description="图表类型")
    execution_trace: ExecutionTrace | None = Field(None, description="执行轨迹")

    model_config = ConfigDict(from_attributes=True)


class Task3ExportRequest(BaseModel):
    questions: list[dict] = Field(..., description="待回答问题列表")


class Task3PlanRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000, description="用户问题")
    context: dict = Field(default_factory=dict, description="上下文信息")


class Task3PlanResponse(BaseModel):
    plan: ExecutionPlan = Field(..., description="生成的执行计划")
    reasoning: str | None = Field(None, description="规划推理过程")

    model_config = ConfigDict(from_attributes=True)


# ─────────────────────────────────────────────────────────────────────────────
# 工作台相关 Schema
# ─────────────────────────────────────────────────────────────────────────────
class Task3ImportStatus(IntEnum):
    NOT_IMPORTED = 0
    IMPORTING = 1
    IMPORTED = 2
    IMPORT_FAILED = 3


class Task3QuestionStatus(IntEnum):
    PENDING = 0
    ANSWERING = 1
    ANSWERED = 2
    FAILED = 3


class Task3WorkspaceResponse(BaseModel):
    id: int = Field(..., description="工作台ID")
    source_file_name: str | None = Field(None, description="附件6源文件名")
    source_file_path: str | None = Field(None, description="附件6源文件路径")
    import_status: int = Field(0, description="导入状态：0未导入 1导入中 2已导入 3导入失败")
    total_questions: int = Field(0, description="题目总数")
    answered_count: int = Field(0, description="已回答数量")
    failed_count: int = Field(0, description="失败数量")
    pending_count: int = Field(0, description="待处理数量")
    last_export_path: str | None = Field(None, description="最近导出文件路径")
    last_exported_at: datetime | None = Field(None, description="最近导出时间")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")

    model_config = ConfigDict(from_attributes=True)


class Task3QuestionItemResponse(BaseModel):
    id: int = Field(..., description="题目ID")
    workspace_id: int = Field(..., description="关联工作台ID")
    question_code: str = Field(..., description="题目编号")
    question_type: str | None = Field(None, description="问题类型")
    question_raw_json: str | None = Field(None, description="原始问题JSON字符串")
    status: int = Field(0, description="状态：0待处理 1回答中 2已完成 3失败")
    answer_json: list | None = Field(None, description="回答JSON数组")
    sql_text: str | None = Field(None, description="生成的SQL语句")
    chart_type: str | None = Field(None, description="图表类型")
    image_paths_json: list | None = Field(None, description="图表文件路径列表")
    execution_plan: dict | None = Field(None, description="执行计划对象")
    verification: dict | None = Field(None, description="校验结果对象")
    retrieval_summary: dict | None = Field(None, description="知识库检索摘要")
    last_error: str | None = Field(None, description="最后一次错误信息")
    answered_at: datetime | None = Field(None, description="回答完成时间")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")

    model_config = ConfigDict(from_attributes=True)


class Task3ImportResponse(BaseModel):
    workspace_id: int = Field(..., description="工作台ID")
    source_file_name: str = Field(..., description="源文件名")
    total_questions: int = Field(..., description="解析出的题目总数")
    message: str = Field(..., description="导入结果消息")

    model_config = ConfigDict(from_attributes=True)


class Task3QuestionListResponse(BaseModel):
    items: list[Task3QuestionItemResponse] = Field(default_factory=list, description="题目列表")
    total: int = Field(0, description="总数")
    pending_count: int = Field(0, description="待处理数量")
    answered_count: int = Field(0, description="已回答数量")
    failed_count: int = Field(0, description="失败数量")

    model_config = ConfigDict(from_attributes=True)
