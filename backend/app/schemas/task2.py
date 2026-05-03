from datetime import datetime
from enum import IntEnum

from pydantic import BaseModel, ConfigDict, Field


# ========== 辅助类（Support）==========

class ImportStatus(IntEnum):
    NOT_IMPORTED = 0
    IMPORTING = 1
    IMPORTED = 2
    IMPORT_FAILED = 3


class QuestionStatus(IntEnum):
    PENDING = 0
    ANSWERING = 1
    ANSWERED = 2
    FAILED = 3


# ========== 响应类（Response）==========

class Task2WorkspaceResponse(BaseModel):
    id: int = Field(..., description="工作台ID")
    source_file_name: str | None = Field(None, description="附件4源文件名")
    source_file_path: str | None = Field(None, description="附件4源文件路径")
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


class Task2QuestionItemResponse(BaseModel):
    id: int = Field(..., description="题目ID")
    workspace_id: int = Field(..., description="关联工作台ID")
    question_code: str = Field(..., description="题目编号")
    question_type: str | None = Field(None, description="问题类型")
    question_raw_json: str | None = Field(None, description="原始问题JSON字符串")
    rounds_json: list | None = Field(None, description="解析后的多轮问题数组")
    status: int = Field(0, description="状态：0待处理 1回答中 2已完成 3失败")
    session_id: str | None = Field(None, description="关联的会话ID")
    answer_json: list | None = Field(None, description="回答JSON数组")
    sql_text: str | None = Field(None, description="生成的SQL语句")
    chart_type: str | None = Field(None, description="图表类型")
    image_paths_json: list | None = Field(None, description="图表文件路径列表")
    last_error: str | None = Field(None, description="最后一次错误信息")
    answered_at: datetime | None = Field(None, description="回答完成时间")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")

    model_config = ConfigDict(from_attributes=True)


class Task2ImportResponse(BaseModel):
    workspace_id: int = Field(..., description="工作台ID")
    source_file_name: str = Field(..., description="源文件名")
    total_questions: int = Field(..., description="解析出的题目总数")
    message: str = Field(..., description="导入结果消息")

    model_config = ConfigDict(from_attributes=True)


class Task2QuestionListResponse(BaseModel):
    items: list[Task2QuestionItemResponse] = Field(default_factory=list, description="题目列表")
    total: int = Field(0, description="总数")
    pending_count: int = Field(0, description="待处理数量")
    answered_count: int = Field(0, description="已回答数量")
    failed_count: int = Field(0, description="失败数量")

    model_config = ConfigDict(from_attributes=True)


class Task2QuestionStatsResponse(BaseModel):
    """任务二题目状态统计"""
    total: int = 0
    pending: int = 0
    answered: int = 0
    failed: int = 0

    model_config = ConfigDict(from_attributes=True)


class Task2ExportResultResponse(BaseModel):
    """任务二导出结果响应"""
    xlsx_path: str = Field(..., description="导出 Excel 文件路径")
    json_path: str = Field(..., description="导出 JSON 文件路径")
    total_questions: int = Field(..., description="题目总数")
    answered_count: int = Field(0, description="已回答数量")
    failed_count: int = Field(0, description="失败数量")
    exported_at: str = Field(..., description="导出时间")

    model_config = ConfigDict(from_attributes=True)


class Task2LatestExportInfoResponse(BaseModel):
    """任务二最近导出信息响应"""
    xlsx_path: str = Field(..., description="最近导出文件路径")
    exported_at: str | None = Field(None, description="最近导出时间")

    model_config = ConfigDict(from_attributes=True)


class AnswerResultResponse(BaseModel):
    """单题回答结果"""
    question_id: int
    question_code: str
    status: int
    answer_json: list | None = None
    sql_text: str | None = None
    chart_type: str | None = None
    image_paths: list | None = None

    model_config = ConfigDict(from_attributes=True)


class DeleteAnswerResponse(BaseModel):
    """删除回答结果"""
    question_id: int
    question_code: str
    status: int
    message: str = "回答已删除"

    model_config = ConfigDict(from_attributes=True)


class BatchAnswerResultItem(BaseModel):
    """批量回答单项结果"""
    question_code: str
    status: str
    result: AnswerResultResponse | None = None
    error: str | None = None

    model_config = ConfigDict(from_attributes=True)


class BatchAnswerResponse(BaseModel):
    """批量回答结果"""
    total: int
    processed: int
    success: int
    failed: int
    message: str | None = None
    results: list[BatchAnswerResultItem] | None = None

    model_config = ConfigDict(from_attributes=True)
