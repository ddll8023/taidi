"""结构化报告抽取服务数据模型"""

from typing import Any

from pydantic import BaseModel, ConfigDict


# ========== 辅助类（Support）==========  # 内部 Enum、通用对象类


class StructuredExtractionArtifact(BaseModel):
    payload: dict[str, list[dict[str, Any]]]
    structured_json_path: str
    trace: dict[str, Any]
    use_full_pdf: bool = False

    model_config = ConfigDict(frozen=True)


class PdfPageText(BaseModel):
    page_number: int
    text: str

    model_config = ConfigDict(frozen=True)


class TableExtractionContext(BaseModel):
    table_name: str
    page_numbers: tuple[int, ...]
    context_text: str
    source_mode: str
    anchor_page: int | None
    used_core_supplement: bool

    model_config = ConfigDict(frozen=True)


class TableExtractionResult(BaseModel):
    table_name: str
    records: list[dict[str, Any]]
    page_numbers: tuple[int, ...]
    source_mode: str
    stop_reason: str | None
    skipped: bool
    used_core_supplement: bool

    model_config = ConfigDict(frozen=True)
