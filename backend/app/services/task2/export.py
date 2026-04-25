"""任务二结果导出服务"""
import json
import os
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.task2_question_item import Task2QuestionItem
from app.models.task2_workspace import Task2Workspace
from app.schemas.common import ErrorCode
from app.schemas.task2 import QuestionStatus
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)

RESULT_DIR = os.path.join(os.getcwd(), "result")


# ========== 公共入口函数 ==========

def export_result_2(db: Session):
    """导出任务二结果文件并记录最近导出信息。"""
    return _export_result_2(db=db)


"""辅助函数"""


def _commit_or_raise(db: Session):
    """提交当前事务，失败时回滚并转换为业务异常。"""
    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "操作失败") from exc


def _ensure_non_empty_qa_pairs(question_json_str: str, qa_pairs: list[dict]):
    if qa_pairs:
        return qa_pairs

    try:
        rounds = json.loads(question_json_str)
    except (json.JSONDecodeError, TypeError):
        rounds = [{"Q": question_json_str}]

    first_question = ""
    if isinstance(rounds, list) and rounds:
        first_round = rounds[0]
        first_question = (
            first_round.get("Q", "")
            if isinstance(first_round, dict)
            else str(first_round)
        )

    fallback_question = first_question.strip() or str(question_json_str)
    return [
        {
            "Q": fallback_question,
            "A": {
                "content": "回答生成失败：未生成任何有效轮次结果，请重新执行该题。"
            },
        }
    ]


def _export_result_2(db: Session):
    stmt = select(Task2Workspace).order_by(Task2Workspace.id.desc()).limit(1)
    workspace = db.execute(stmt).scalar_one_or_none()

    if workspace is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "工作台不存在，请先导入附件4")

    stmt = select(Task2QuestionItem).where(
        Task2QuestionItem.workspace_id == workspace.id
    ).order_by(Task2QuestionItem.question_code)
    questions = list(db.execute(stmt).scalars().all())

    if not questions:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "没有可导出的题目")

    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)

    ws = wb.create_sheet(title="结果汇总")
    ws.append(["编号", "问题", "SQL查询语句", "图形格式", "回答"])

    all_results = []

    for q in questions:
        question_id = q.question_code
        question_json_str = q.question_raw_json or ""
        sql_query = q.sql_text or ""
        chart_type = q.chart_type or "无"
        qa_pairs = _ensure_non_empty_qa_pairs(question_json_str, q.answer_json or [])

        try:
            rounds = json.loads(question_json_str)
        except (json.JSONDecodeError, TypeError):
            rounds = [{"Q": question_json_str}]

        result_item = {
            "id": question_id,
            "type": q.question_type or "",
            "question": question_json_str,
            "sql": sql_query,
            "chart_type": chart_type,
            "answer": qa_pairs,
        }
        all_results.append(result_item)

        ws.append([
            question_id,
            json.dumps(rounds, ensure_ascii=False),
            sql_query,
            chart_type,
            json.dumps(qa_pairs, ensure_ascii=False),
        ])

    for col in ws.columns:
        max_length = 0
        for cell in col:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except Exception:
                pass
        adjusted_width = min(max_length + 2, 60)
        ws.column_dimensions[col[0].column_letter].width = adjusted_width

    os.makedirs(RESULT_DIR, exist_ok=True)
    xlsx_path = os.path.join(RESULT_DIR, "result_2.xlsx")
    wb.save(xlsx_path)
    logger.info("result_2.xlsx 已生成: %s, 共 %d 个问题", xlsx_path, len(questions))

    json_path = os.path.join(RESULT_DIR, "result_2.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    logger.info("result_2.json 已生成: %s", json_path)

    workspace.last_export_path = xlsx_path
    workspace.last_exported_at = datetime.now()
    db.flush()
    _commit_or_raise(db)

    return {
        "xlsx_path": xlsx_path,
        "json_path": json_path,
        "total_questions": len(questions),
        "answered_count": sum(1 for q in questions if q.status == QuestionStatus.ANSWERED),
        "failed_count": sum(1 for q in questions if q.status == QuestionStatus.FAILED),
        "exported_at": datetime.now().isoformat(),
    }


def get_latest_export_info(db: Session):
    """查询最近一次任务二导出结果信息，无导出记录时返回带 message 的默认结构。"""
    stmt = select(Task2Workspace).order_by(Task2Workspace.id.desc()).limit(1)
    workspace = db.execute(stmt).scalar_one_or_none()

    if workspace is None or not workspace.last_export_path:
        return {"message": "暂无导出记录"}

    return {
        "xlsx_path": workspace.last_export_path,
        "exported_at": workspace.last_exported_at.isoformat() if workspace.last_exported_at else None,
    }
