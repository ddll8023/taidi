from datetime import datetime
from enum import IntEnum

from pydantic import BaseModel, ConfigDict, Field


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
