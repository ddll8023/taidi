"""任务三附件导入与工作台查询服务。"""

import json
import os
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.db.database import commit_or_rollback
from app.models.task3_question_item import Task3QuestionItem
from app.models.task3_workspace import Task3Workspace
from app.schemas.common import ErrorCode
from app.schemas.task3 import (
    Task3ImportResponse,
    Task3ImportStatus,
    Task3QuestionItemResponse,
    Task3QuestionStatsResponse,
    Task3WorkspaceResponse,
)
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)

MAIN_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL_NS = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}
OFFICE_REL_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"

UPLOAD_DIR = os.path.join(os.getcwd(), "uploads", "fujian6", "current")


# ========== 公共入口函数 ==========


def get_workspace_info(db: Session):
    """获取最新的任务三工作台记录。"""
    stmt = select(Task3Workspace).order_by(Task3Workspace.id.desc()).limit(1)
    workspace = db.execute(stmt).scalar_one_or_none()
    if workspace is None:
        return None
    return Task3WorkspaceResponse.model_validate(workspace)


def get_question_list(
    db: Session,
    workspace_id: int,
    status: int | None = None,
):
    """获取工作台题目列表。"""
    stmt = select(Task3QuestionItem).where(
        Task3QuestionItem.workspace_id == workspace_id
    )
    if status is not None:
        stmt = stmt.where(Task3QuestionItem.status == status)
    stmt = stmt.order_by(Task3QuestionItem.question_code)
    questions = list(db.execute(stmt).scalars().all())
    for question in questions:
        normalize_question_item(question)
    return [Task3QuestionItemResponse.model_validate(q) for q in questions]


def get_question_detail(db: Session, question_id: int):
    """获取单个题目的详情对象。"""
    question = db.get(Task3QuestionItem, question_id)
    if question is None:
        return None
    normalize_question_item(question)
    return Task3QuestionItemResponse.model_validate(question)


def normalize_question_item(question: Task3QuestionItem):
    """规范化题目对象中的结构化字段。"""
    if isinstance(question.execution_plan, list):
        question.execution_plan = {"rounds": question.execution_plan}
    return question


def get_question_stats(
    db: Session, workspace_id: int
):
    """统计工作台题目数量与状态分布。"""
    all_items = get_question_list(db, workspace_id)
    total = len(all_items)
    pending = sum(1 for q in all_items if q.status == 0)
    answered = sum(1 for q in all_items if q.status == 2)
    failed = sum(1 for q in all_items if q.status == 3)
    return Task3QuestionStatsResponse(
        total=total,
        pending=pending,
        answered=answered,
        failed=failed,
    )


def get_or_create_workspace(db: Session):
    """获取工作台，不存在时自动创建。"""
    workspace = _get_or_create_workspace_entity(db)
    return Task3WorkspaceResponse.model_validate(workspace)


def import_fujian6(
    file_path: str, original_filename: str, db: Session
):
    """导入附件6并刷新任务三工作台数据。"""
    workspace = _get_or_create_workspace_entity(db)

    stmt = select(Task3QuestionItem).where(
        Task3QuestionItem.workspace_id == workspace.id
    )
    existing_questions = db.execute(stmt).scalars().all()
    if existing_questions:
        logger.info("工作台已有数据，将清空旧数据后重新导入")
        db.execute(delete(Task3QuestionItem).where(Task3QuestionItem.workspace_id == workspace.id))

    workspace.import_status = Task3ImportStatus.IMPORTING
    db.flush()

    try:
        questions = _parse_fujian6_file(file_path)
        logger.info("从附件6解析出 %d 个问题", len(questions))

        os.makedirs(UPLOAD_DIR, exist_ok=True)
        saved_path = os.path.join(UPLOAD_DIR, "附件6.xlsx")

        import shutil
        shutil.copy2(file_path, saved_path)

        workspace.source_file_name = original_filename
        workspace.source_file_path = saved_path

        for q in questions:
            rounds_json: list | None = None
            try:
                parsed = json.loads(q["question"])
                if isinstance(parsed, list):
                    rounds_json = parsed
                else:
                    rounds_json = [{"Q": str(parsed)}]
            except (json.JSONDecodeError, TypeError):
                rounds_json = [{"Q": q["question"]}]

            question_item = Task3QuestionItem(
                workspace_id=workspace.id,
                question_code=q["id"],
                question_type=q.get("type", ""),
                question_raw_json=q["question"],
                status=0,
            )
            db.add(question_item)

        workspace.total_questions = len(questions)
        workspace.pending_count = len(questions)
        workspace.answered_count = 0
        workspace.failed_count = 0
        workspace.import_status = Task3ImportStatus.IMPORTED
        commit_or_rollback(db)

        logger.info("附件6导入完成: workspace_id=%d total=%d", workspace.id, len(questions))

        return Task3ImportResponse(
            workspace_id=workspace.id,
            source_file_name=original_filename,
            total_questions=len(questions),
            message=f"成功导入 {len(questions)} 个问题",
        )

    except Exception as e:
        workspace.import_status = Task3ImportStatus.IMPORT_FAILED
        commit_or_rollback(db)
        logger.error("导入附件6失败: %s", str(e), exc_info=True)
        raise


"""辅助函数"""


def _column_ref_to_index(cell_ref: str):
    """将 Excel 列标转换为从零开始的索引。"""
    letters = "".join(char for char in cell_ref if char.isalpha())
    if not letters:
        return 0
    index = 0
    for char in letters:
        index = index * 26 + (ord(char.upper()) - 64)
    return index - 1


def _load_shared_strings(archive: zipfile.ZipFile):
    """读取共享字符串表。"""
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    values: list[str] = []
    for shared_string in root.findall("a:si", MAIN_NS):
        text_parts: list[str] = []
        for node in shared_string.iter():
            if node.tag == "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t":
                text_parts.append(node.text or "")
        values.append("".join(text_parts))
    return values


def _get_all_sheet_names(archive: zipfile.ZipFile):
    """获取工作簿中的全部工作表名称。"""
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    sheets = workbook.find("a:sheets", MAIN_NS)
    if sheets is None:
        return []
    return [sheet.attrib.get("name", "") for sheet in sheets]


def _get_sheet_target(archive: zipfile.ZipFile, sheet_name: str):
    """根据工作表名称定位其 XML 文件路径。"""
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    rel_map = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in relationships.findall("r:Relationship", REL_NS)
    }
    sheets = workbook.find("a:sheets", MAIN_NS)
    for sheet in sheets:
        if sheet.attrib.get("name") != sheet_name:
            continue
        relation_id = sheet.attrib.get(OFFICE_REL_NS)
        if not relation_id or relation_id not in rel_map:
            break
        target = rel_map[relation_id]
        return target if target.startswith("xl/") else f"xl/{target}"
    raise ServiceException(ErrorCode.PARAM_ERROR, f"未找到工作表：{sheet_name}")


def _read_sheet_rows(archive: zipfile.ZipFile, sheet_target: str):
    """读取指定工作表的原始行数据。"""
    shared_strings = _load_shared_strings(archive)
    root = ET.fromstring(archive.read(sheet_target))
    sheet_data = root.find("a:sheetData", MAIN_NS)
    if sheet_data is None:
        return []
    rows: list[list[str]] = []
    for row in sheet_data.findall("a:row", MAIN_NS):
        cell_values: dict[int, str] = {}
        for cell in row.findall("a:c", MAIN_NS):
            index = _column_ref_to_index(cell.attrib.get("r", ""))
            cell_type = cell.attrib.get("t")
            value = ""
            if cell_type == "inlineStr":
                text_node = cell.find("a:is/a:t", MAIN_NS)
                value = "" if text_node is None else (text_node.text or "")
            else:
                raw_node = cell.find("a:v", MAIN_NS)
                if raw_node is not None and raw_node.text is not None:
                    raw_value = raw_node.text
                    if cell_type == "s" and raw_value.isdigit():
                        shared_index = int(raw_value)
                        if 0 <= shared_index < len(shared_strings):
                            value = shared_strings[shared_index]
                        else:
                            value = raw_value
                    else:
                        value = raw_value
            cell_values[index] = value
        max_index = max(cell_values.keys(), default=-1)
        rows.append([cell_values.get(i, "") for i in range(max_index + 1)])
    return rows


def _parse_fujian6_file(file_path: str):
    """解析附件6并提取题目数据。"""
    source_path = Path(file_path)
    if not source_path.exists():
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"附件6文件不存在：{file_path}")

    questions = []
    with zipfile.ZipFile(source_path) as archive:
        sheet_names = _get_all_sheet_names(archive)
        for sheet_name in sheet_names:
            sheet_target = _get_sheet_target(archive, sheet_name)
            rows = _read_sheet_rows(archive, sheet_target)
            if not rows:
                continue

            headers = [h.strip() for h in rows[0]]
            col_map: dict[str, int] = {}
            for idx, header in enumerate(headers):
                if header == "编号":
                    col_map["id"] = idx
                elif header == "问题类型":
                    col_map["type"] = idx
                elif header == "问题":
                    col_map["question"] = idx

            for row in rows[1:]:
                if not any(str(cell).strip() for cell in row):
                    continue
                padded_row = row + [""] * max(0, len(headers) - len(row))
                q_id = str(padded_row[col_map.get("id", 0)]).strip()
                q_type = str(padded_row[col_map.get("type", 1)]).strip()
                q_text = str(padded_row[col_map.get("question", 2)]).strip()
                if q_id:
                    questions.append({"id": q_id, "type": q_type, "question": q_text})

    return questions


def _get_or_create_workspace_entity(db: Session):
    """获取工作台ORM实体，不存在时自动创建（仅供内部使用）。"""
    stmt = select(Task3Workspace).order_by(Task3Workspace.id.desc()).limit(1)
    workspace = db.execute(stmt).scalar_one_or_none()
    if workspace is None:
        workspace = Task3Workspace(
            import_status=Task3ImportStatus.NOT_IMPORTED,
            total_questions=0,
            answered_count=0,
            failed_count=0,
            pending_count=0,
        )
        db.add(workspace)
        db.flush()
        logger.info("创建新任务三工作台: id=%d", workspace.id)
    return workspace
