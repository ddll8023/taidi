"""任务二附件导入与工作台查询服务"""
import json
import os
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models.chat_message import ChatMessage
from app.models.chat_session import ChatSession
from app.models.task2_question_item import Task2QuestionItem
from app.models.task2_workspace import Task2Workspace
from app.schemas.common import ErrorCode
from app.schemas.task2 import (
    ImportStatus,
    QuestionStatus,
    Task2QuestionItemResponse,
    Task2WorkspaceResponse,
)
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)

MAIN_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL_NS = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}
OFFICE_REL_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"

UPLOAD_DIR = os.path.join(os.getcwd(), "uploads", "fujian4", "current")


# ========== 公共入口函数 ==========

def import_fujian4(file_path: str, original_filename: str, db: Session) -> dict:
    """导入附件4文件，解析题目并初始化任务二工作台。"""
    return _import_fujian4(
        file_path=file_path,
        original_filename=original_filename,
        db=db,
    )


def get_workspace_info(db: Session) -> Task2WorkspaceResponse | None:
    """查询最近一次任务二工作台概览。"""
    workspace = _get_workspace_entity(db)
    if workspace is None:
        return None
    return Task2WorkspaceResponse.model_validate(workspace)


def get_question_list(
    db: Session,
    workspace_id: int,
    status: int | None = None,
) -> list[Task2QuestionItemResponse]:
    """查询任务二工作台题目列表。"""
    return [
        Task2QuestionItemResponse.model_validate(item)
        for item in _get_question_entities(db=db, workspace_id=workspace_id, status=status)
    ]


def get_question_detail(question_id: int, db: Session) -> Task2QuestionItemResponse:
    """查询任务二单题详情。"""
    question = db.get(Task2QuestionItem, question_id)
    if question is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "题目不存在")
    return Task2QuestionItemResponse.model_validate(question)


def get_question_stats(db: Session, workspace_id: int) -> dict:
    """统计任务二工作台题目状态数量。"""
    questions = _get_question_entities(db=db, workspace_id=workspace_id)
    total = len(questions)
    pending = sum(1 for q in questions if q.status == QuestionStatus.PENDING)
    answered = sum(1 for q in questions if q.status == QuestionStatus.ANSWERED)
    failed = sum(1 for q in questions if q.status == QuestionStatus.FAILED)

    return {
        "total": total,
        "pending": pending,
        "answered": answered,
        "failed": failed,
    }


"""辅助函数"""


def _commit_or_raise(db: Session) -> None:
    """提交当前事务，失败时回滚并转换为业务异常。"""
    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "操作失败") from exc


def _column_ref_to_index(cell_ref: str) -> int:
    letters = "".join(char for char in cell_ref if char.isalpha())
    if not letters:
        return 0
    index = 0
    for char in letters:
        index = index * 26 + (ord(char.upper()) - 64)
    return index - 1


def _load_shared_strings(archive: zipfile.ZipFile) -> list[str]:
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


def _get_all_sheet_names(archive: zipfile.ZipFile) -> list[str]:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    sheets = workbook.find("a:sheets", MAIN_NS)
    if sheets is None:
        return []
    return [sheet.attrib.get("name", "") for sheet in sheets]


def _get_sheet_target(archive: zipfile.ZipFile, sheet_name: str) -> str:
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


def _read_sheet_rows(archive: zipfile.ZipFile, sheet_target: str) -> list[list[str]]:
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


def _parse_fujian4_file(file_path: str) -> list[dict]:
    source_path = Path(file_path)
    if not source_path.exists():
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "附件4文件不存在")

    questions = []
    with zipfile.ZipFile(source_path) as archive:
        sheet_names = _get_all_sheet_names(archive)
        for sheet_name in sheet_names:
            sheet_target = _get_sheet_target(archive, sheet_name)
            rows = _read_sheet_rows(archive, sheet_target)
            if not rows:
                continue

            headers = [h.strip() for h in rows[0]]
            col_map = {}
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


def _delete_old_workspace_data(db: Session, workspace_id: int):
    stmt = select(Task2QuestionItem).where(Task2QuestionItem.workspace_id == workspace_id)
    questions = db.execute(stmt).scalars().all()

    chart_dir = os.path.join(os.getcwd(), "result")
    for q in questions:
        if q.session_id:
            _delete_session_and_charts(db, q.session_id, chart_dir)
        if q.image_paths_json:
            for img_path in q.image_paths_json:
                _delete_chart_file(img_path, chart_dir)

    db.execute(delete(Task2QuestionItem).where(Task2QuestionItem.workspace_id == workspace_id))
    logger.info("已删除工作台 %d 的旧题目数据", workspace_id)


def _delete_session_and_charts(db: Session, session_id: str, chart_dir: str):
    stmt = select(ChatMessage).where(ChatMessage.session_id == session_id)
    messages = db.execute(stmt).scalars().all()

    for m in messages:
        if m.chart_paths:
            for chart_url in m.chart_paths:
                _delete_chart_file(chart_url, chart_dir)
        db.delete(m)

    session = db.get(ChatSession, session_id)
    if session:
        db.delete(session)
    logger.info("已删除会话 %s 及其图表", session_id)


def _delete_chart_file(chart_path: str, chart_dir: str):
    try:
        if chart_path.startswith("/api/v1/"):
            filename = chart_path.split("/")[-1]
        elif "/" in chart_path or "\\" in chart_path:
            filename = os.path.basename(chart_path)
        else:
            filename = chart_path

        file_path = os.path.join(chart_dir, filename)
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info("已删除图表文件: %s", file_path)
    except Exception as e:
        logger.warning("删除图表文件失败: path=%s error=%s", chart_path, str(e))


def _get_or_create_workspace(db: Session) -> Task2Workspace:
    stmt = select(Task2Workspace).order_by(Task2Workspace.id.desc()).limit(1)
    workspace = db.execute(stmt).scalar_one_or_none()

    if workspace is None:
        workspace = Task2Workspace(
            import_status=ImportStatus.NOT_IMPORTED,
            total_questions=0,
            answered_count=0,
            failed_count=0,
            pending_count=0,
        )
        db.add(workspace)
        db.flush()
        logger.info("创建新工作台: id=%d", workspace.id)

    return workspace


def _import_fujian4(file_path: str, original_filename: str, db: Session) -> dict:
    workspace = _get_or_create_workspace(db)

    if workspace.import_status == ImportStatus.IMPORTED and workspace.total_questions > 0:
        logger.info("工作台已有数据，将清空旧数据后重新导入")
        _delete_old_workspace_data(db, workspace.id)

    workspace.import_status = ImportStatus.IMPORTING
    db.flush()

    try:
        questions = _parse_fujian4_file(file_path)
        logger.info("从附件4解析出 %d 个问题", len(questions))

        os.makedirs(UPLOAD_DIR, exist_ok=True)
        saved_filename = f"附件4.xlsx"
        saved_path = os.path.join(UPLOAD_DIR, saved_filename)

        import shutil
        shutil.copy2(file_path, saved_path)

        workspace.source_file_name = original_filename
        workspace.source_file_path = saved_path

        for q in questions:
            rounds_json = None
            try:
                parsed = json.loads(q["question"])
                if isinstance(parsed, list):
                    rounds_json = parsed
                else:
                    rounds_json = [{"Q": str(parsed)}]
            except (json.JSONDecodeError, TypeError):
                rounds_json = [{"Q": q["question"]}]

            question_item = Task2QuestionItem(
                workspace_id=workspace.id,
                question_code=q["id"],
                question_type=q.get("type", ""),
                question_raw_json=q["question"],
                rounds_json=rounds_json,
                status=QuestionStatus.PENDING,
            )
            db.add(question_item)

        workspace.total_questions = len(questions)
        workspace.pending_count = len(questions)
        workspace.answered_count = 0
        workspace.failed_count = 0
        workspace.import_status = ImportStatus.IMPORTED
        _commit_or_raise(db)

        logger.info("附件4导入完成: workspace_id=%d total=%d", workspace.id, len(questions))

        return {
            "workspace_id": workspace.id,
            "source_file_name": original_filename,
            "total_questions": len(questions),
            "message": f"成功导入 {len(questions)} 个问题",
        }

    except ServiceException as e:
        workspace.import_status = ImportStatus.IMPORT_FAILED
        _commit_or_raise(db)
        logger.error("附件4导入失败: %s", e.message)
        raise
    except Exception as e:
        workspace.import_status = ImportStatus.IMPORT_FAILED
        _commit_or_raise(db)
        logger.error("附件4导入失败: %s", str(e))
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "导入失败") from e


def _get_workspace_entity(db: Session) -> Task2Workspace | None:
    stmt = select(Task2Workspace).order_by(Task2Workspace.id.desc()).limit(1)
    return db.execute(stmt).scalar_one_or_none()


def _get_question_entities(
    db: Session,
    workspace_id: int,
    status: int | None = None,
) -> list[Task2QuestionItem]:
    stmt = select(Task2QuestionItem).where(Task2QuestionItem.workspace_id == workspace_id)
    if status is not None:
        stmt = stmt.where(Task2QuestionItem.status == status)
    stmt = stmt.order_by(Task2QuestionItem.question_code)
    return list(db.execute(stmt).scalars().all())
