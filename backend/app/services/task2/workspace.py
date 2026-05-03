"""任务二附件导入与工作台查询服务"""
import json
import os
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

from fastapi import UploadFile
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models.task2_question_item import Task2QuestionItem
from app.models.task2_workspace import Task2Workspace
from app.schemas.common import ErrorCode
from app.schemas import task2 as schemas_task2
from app.utils.exception import ServiceException
from app.db.database import commit_or_rollback
from app.services.task2.helpers import _delete_chart_file, _delete_session_and_charts
from app.constants import task2 as constants_task2
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)

UPLOAD_DIR = os.path.join(os.getcwd(), "uploads", "fujian4", "current")


# ========== 公共入口函数 ==========

def import_fujian4(file_path: str, original_filename: str, db: Session):
    """导入附件4文件，解析题目并初始化任务二工作台。"""
    return _import_fujian4(
        file_path=file_path,
        original_filename=original_filename,
        db=db,
    )


def get_workspace_info(db: Session):
    """查询最近一次任务二工作台概览。"""
    workspace = _get_workspace_entity(db)
    if workspace is None:
        return None
    return schemas_task2.Task2WorkspaceResponse.model_validate(workspace)


def get_question_list(
    db: Session,
    workspace_id: int,
    status: int | None = None,
):
    """查询任务二工作台题目列表。"""
    return [
        schemas_task2.Task2QuestionItemResponse.model_validate(item)
        for item in _get_question_entities(db=db, workspace_id=workspace_id, status=status)
    ]


def get_question_detail(question_id: int, db: Session):
    """查询任务二单题详情。"""
    question = db.get(Task2QuestionItem, question_id)
    if question is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "题目不存在")
    return schemas_task2.Task2QuestionItemResponse.model_validate(question)


def get_question_stats(db: Session, workspace_id: int):
    """统计任务二工作台题目状态数量。"""
    questions = _get_question_entities(db=db, workspace_id=workspace_id)
    total = len(questions)
    pending = sum(1 for q in questions if q.status == schemas_task2.QuestionStatus.PENDING)
    answered = sum(1 for q in questions if q.status == schemas_task2.QuestionStatus.ANSWERED)
    failed = sum(1 for q in questions if q.status == schemas_task2.QuestionStatus.FAILED)

    return schemas_task2.Task2QuestionStatsResponse(
        total=total,
        pending=pending,
        answered=answered,
        failed=failed,
    )


def get_workspace_or_raise(db: Session):
    """获取工作台，不存在时抛异常。"""
    workspace = get_workspace_info(db)
    if workspace is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "工作台不存在，请先导入附件4")
    return workspace


def get_question_list_response(db: Session, status: int | None = None):
    """查询题目列表并组装完整响应（含工作台校验和统计）。"""
    workspace = get_workspace_info(db)
    if workspace is None:
        return schemas_task2.Task2QuestionListResponse()

    questions = get_question_list(db=db, workspace_id=workspace.id, status=status)
    stats = get_question_stats(db, workspace.id)

    return schemas_task2.Task2QuestionListResponse(
        items=questions,
        total=stats.total,
        pending_count=stats.pending,
        answered_count=stats.answered,
        failed_count=stats.failed,
    )


async def import_fujian4_from_upload(db: Session, file: UploadFile):
    """接收上传文件，校验格式后导入附件4。"""
    if not file.filename or not file.filename.endswith(".xlsx"):
        raise ServiceException(ErrorCode.PARAM_ERROR, "请上传xlsx格式的附件4文件")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
        content = await file.read()
        tmp_file.write(content)
        tmp_path = tmp_file.name

    try:
        result = import_fujian4(
            file_path=tmp_path,
            original_filename=file.filename,
            db=db,
        )
        return result
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


"""辅助函数"""


def _column_ref_to_index(cell_ref: str):
    """将 Excel 列字母引用（如 A、AB）转换为零基数字索引。"""
    letters = "".join(char for char in cell_ref if char.isalpha())
    if not letters:
        return 0
    index = 0
    for char in letters:
        index = index * 26 + (ord(char.upper()) - 64)
    return index - 1


def _load_shared_strings(archive: zipfile.ZipFile):
    """从 xlsx 归档中加载共享字符串表。"""
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    values: list[str] = []
    for shared_string in root.findall("a:si", constants_task2.MAIN_NS):
        text_parts: list[str] = []
        for node in shared_string.iter():
            if node.tag == "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t":
                text_parts.append(node.text or "")
        values.append("".join(text_parts))
    return values


def _get_all_sheet_names(archive: zipfile.ZipFile):
    """获取 xlsx 工作簿中所有工作表名称。"""
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    sheets = workbook.find("a:sheets", constants_task2.MAIN_NS)
    if sheets is None:
        return []
    return [sheet.attrib.get("name", "") for sheet in sheets]


def _get_sheet_target(archive: zipfile.ZipFile, sheet_name: str):
    """根据工作表名称获取其在 xlsx 归档中的文件路径。"""
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    rel_map = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in relationships.findall("r:Relationship", constants_task2.REL_NS)
    }
    sheets = workbook.find("a:sheets", constants_task2.MAIN_NS)
    for sheet in sheets:
        if sheet.attrib.get("name") != sheet_name:
            continue
        relation_id = sheet.attrib.get(constants_task2.OFFICE_REL_NS)
        if not relation_id or relation_id not in rel_map:
            break
        target = rel_map[relation_id]
        return target if target.startswith("xl/") else f"xl/{target}"
    raise ServiceException(ErrorCode.PARAM_ERROR, f"未找到工作表：{sheet_name}")


def _read_sheet_rows(archive: zipfile.ZipFile, sheet_target: str):
    """读取指定工作表中的所有行数据，返回二维列表。"""
    shared_strings = _load_shared_strings(archive)
    root = ET.fromstring(archive.read(sheet_target))
    sheet_data = root.find("a:sheetData", constants_task2.MAIN_NS)
    if sheet_data is None:
        return []
    rows: list[list[str]] = []
    for row in sheet_data.findall("a:row", constants_task2.MAIN_NS):
        cell_values: dict[int, str] = {}
        for cell in row.findall("a:c", constants_task2.MAIN_NS):
            index = _column_ref_to_index(cell.attrib.get("r", ""))
            cell_type = cell.attrib.get("t")
            value = ""
            if cell_type == "inlineStr":
                text_node = cell.find("a:is/a:t", constants_task2.MAIN_NS)
                value = "" if text_node is None else (text_node.text or "")
            else:
                raw_node = cell.find("a:v", constants_task2.MAIN_NS)
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


def _parse_fujian4_file(file_path: str):
    """解析附件4 xlsx 文件，提取问题列表。"""
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
    """删除工作台关联的旧题目数据、会话及图表文件。"""
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
    logger.info(f"已删除工作台 {workspace_id} 的旧题目数据")


def _get_or_create_workspace(db: Session):
    """获取或创建任务二工作台实体。"""
    stmt = select(Task2Workspace).order_by(Task2Workspace.id.desc()).limit(1)
    workspace = db.execute(stmt).scalar_one_or_none()

    if workspace is None:
        workspace = Task2Workspace(
            import_status=schemas_task2.ImportStatus.NOT_IMPORTED,
            total_questions=0,
            answered_count=0,
            failed_count=0,
            pending_count=0,
        )
        db.add(workspace)
        db.flush()
        logger.info(f"创建新工作台: id={workspace.id}")

    return workspace


def _import_fujian4(file_path: str, original_filename: str, db: Session):
    workspace = _get_or_create_workspace(db)

    if workspace.import_status == schemas_task2.ImportStatus.IMPORTED and workspace.total_questions > 0:
        logger.info("工作台已有数据，将清空旧数据后重新导入")
        _delete_old_workspace_data(db, workspace.id)

    workspace.import_status = schemas_task2.ImportStatus.IMPORTING
    db.flush()

    try:
        questions = _parse_fujian4_file(file_path)
        logger.info(f"从附件4解析出 {len(questions)} 个问题")

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
                status=schemas_task2.QuestionStatus.PENDING,
            )
            db.add(question_item)

        workspace.total_questions = len(questions)
        workspace.pending_count = len(questions)
        workspace.answered_count = 0
        workspace.failed_count = 0
        workspace.import_status = schemas_task2.ImportStatus.IMPORTED
        commit_or_rollback(db)

        logger.info(f"附件4导入完成: workspace_id={workspace.id} total={len(questions)}")

        return schemas_task2.Task2ImportResponse(
            workspace_id=workspace.id,
            source_file_name=original_filename,
            total_questions=len(questions),
            message=f"成功导入 {len(questions)} 个问题",
        )

    except ServiceException as e:
        workspace.import_status = schemas_task2.ImportStatus.IMPORT_FAILED
        commit_or_rollback(db)
        logger.error(f"附件4导入失败: {e.message}")
        raise
    except Exception as e:
        workspace.import_status = schemas_task2.ImportStatus.IMPORT_FAILED
        commit_or_rollback(db)
        logger.error(f"附件4导入失败: {str(e)}")
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "导入失败") from e


def _get_workspace_entity(db: Session):
    """查询最近一条工作台记录。"""
    stmt = select(Task2Workspace).order_by(Task2Workspace.id.desc()).limit(1)
    return db.execute(stmt).scalar_one_or_none()


def _get_question_entities(
    db: Session,
    workspace_id: int,
    status: int | None = None,
):
    """查询指定工作台的题目实体列表。"""
    stmt = select(Task2QuestionItem).where(Task2QuestionItem.workspace_id == workspace_id)
    if status is not None:
        stmt = stmt.where(Task2QuestionItem.status == status)
    stmt = stmt.order_by(Task2QuestionItem.question_code)
    return list(db.execute(stmt).scalars().all())
