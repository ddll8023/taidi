"""任务二题目回答执行服务"""
import json
import os
import uuid
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.chat_message import ChatMessage
from app.models.chat_session import ChatSession
from app.models.task2_question_item import Task2QuestionItem
from app.models.task2_workspace import Task2Workspace
from app.schemas.common import ErrorCode
from app.schemas.task2 import QuestionStatus
from app.services import chat as chat_service
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)

CHART_DIR = os.path.join(os.getcwd(), "result")


# ========== 公共入口函数 ==========

def answer_single_question(question_id: int, db: Session) -> dict:
    """回答指定任务二题目并保存问答结果。"""
    question = db.get(Task2QuestionItem, question_id)
    if question is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "题目不存在")

    if question.status == QuestionStatus.ANSWERING:
        raise ServiceException(ErrorCode.PARAM_ERROR, "题目正在回答中")

    question.status = QuestionStatus.ANSWERING
    question.last_error = None
    db.flush()

    try:
        rounds = question.rounds_json or []
        if not rounds:
            rounds = [{"Q": question.question_raw_json or ""}]

        session_id = question.session_id
        qa_pairs = []
        all_sqls = []
        all_chart_types = []
        all_image_paths = []
        chart_sequence = 1

        for round_idx, round_item in enumerate(rounds):
            q_text = round_item.get("Q", "") if isinstance(round_item, dict) else str(round_item)
            if not q_text.strip():
                continue

            logger.info("处理题目 %s 轮次 %d: %s", question.question_code, round_idx + 1, q_text[:50])

            try:
                response = chat_service.process_chat_message(
                    session_id=session_id,
                    question=q_text,
                    db=db,
                    question_id=question.question_code,
                    chart_sequence=chart_sequence,
                )
                session_id = response.session_id

                image_paths = []
                if response.answer.image:
                    for img in response.answer.image:
                        filename = os.path.basename(img) if "/" in img or "\\" in img else img
                        relative_path = f"./result/{filename}"
                        image_paths.append(relative_path)
                        all_image_paths.append(relative_path)
                        chart_sequence += 1

                answer_data = {
                    "Q": q_text,
                    "A": {
                        "content": response.answer.content,
                    },
                }
                if image_paths:
                    answer_data["A"]["image"] = image_paths
                    if response.chart_type:
                        chart_type_map = {"line": "折线图", "bar": "柱状图", "pie": "饼图"}
                        all_chart_types.append(chart_type_map.get(response.chart_type, "图表"))

                if response.sql:
                    all_sqls.append(response.sql)

                qa_pairs.append(answer_data)

            except Exception as exc:
                logger.error("题目 %s 轮次 %d 处理失败: %s", question.question_code, round_idx + 1, str(exc))
                qa_pairs.append({
                    "Q": q_text,
                    "A": {"content": f"回答生成失败: {str(exc)}"},
                })
                raise

        question.session_id = session_id
        question.answer_json = qa_pairs
        question.sql_text = "\n\n".join(all_sqls) if all_sqls else None
        question.chart_type = "、".join(all_chart_types) if all_chart_types else "无"
        question.image_paths_json = all_image_paths if all_image_paths else None
        question.status = QuestionStatus.ANSWERED
        question.answered_at = datetime.now()
        db.flush()

        _update_workspace_stats(db, question.workspace_id)
        _commit_or_raise(db)

        logger.info("题目 %s 回答完成", question.question_code)

        return {
            "question_id": question.id,
            "question_code": question.question_code,
            "status": question.status,
            "answer_json": qa_pairs,
            "sql_text": question.sql_text,
            "chart_type": question.chart_type,
            "image_paths": all_image_paths,
        }

    except Exception as exc:
        question.status = QuestionStatus.FAILED
        question.last_error = str(exc)
        db.flush()
        _update_workspace_stats(db, question.workspace_id)
        _commit_or_raise(db)
        logger.error("题目 %s 回答失败: %s", question.question_code, str(exc))
        if isinstance(exc, ServiceException):
            raise
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "回答失败") from exc


def delete_question_answer(question_id: int, db: Session) -> dict:
    """删除指定任务二题目的已生成回答。"""
    question = db.get(Task2QuestionItem, question_id)
    if question is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "题目不存在")

    if question.status == QuestionStatus.ANSWERING:
        raise ServiceException(ErrorCode.PARAM_ERROR, "题目正在回答中，无法删除")

    if question.session_id:
        _delete_session_and_charts(db, question.session_id)

    if question.image_paths_json:
        for img_path in question.image_paths_json:
            _delete_chart_file(img_path)

    question.session_id = None
    question.answer_json = None
    question.sql_text = None
    question.chart_type = None
    question.image_paths_json = None
    question.last_error = None
    question.answered_at = None
    question.status = QuestionStatus.PENDING
    db.flush()

    _update_workspace_stats(db, question.workspace_id)
    _commit_or_raise(db)

    logger.info("题目 %s 的回答已删除", question.question_code)

    return {
        "question_id": question.id,
        "question_code": question.question_code,
        "status": question.status,
        "message": "回答已删除",
    }


def rerun_question(question_id: int, db: Session) -> dict:
    """清理指定题目旧结果后重新回答。"""
    question = db.get(Task2QuestionItem, question_id)
    if question is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "题目不存在")

    if question.status == QuestionStatus.ANSWERING:
        raise ServiceException(ErrorCode.PARAM_ERROR, "题目正在回答中，无法重新回答")

    if question.session_id:
        _delete_session_and_charts(db, question.session_id)

    if question.image_paths_json:
        for img_path in question.image_paths_json:
            _delete_chart_file(img_path)

    question.session_id = None
    question.answer_json = None
    question.sql_text = None
    question.chart_type = None
    question.image_paths_json = None
    question.last_error = None
    question.answered_at = None
    db.flush()

    logger.info("题目 %s 开始重新回答", question.question_code)

    return answer_single_question(question_id, db)


def batch_answer_questions(workspace_id: int, scope: str, db: Session) -> dict:
    """按范围批量回答任务二工作台题目。"""
    return _batch_answer_questions(workspace_id=workspace_id, scope=scope, db=db)


"""辅助函数"""


def _commit_or_raise(db: Session) -> None:
    """提交当前事务，失败时回滚并转换为业务异常。"""
    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "操作失败") from exc


def _delete_session_and_charts(db: Session, session_id: str):
    if not session_id:
        return

    stmt = select(ChatMessage).where(ChatMessage.session_id == session_id)
    messages = db.execute(stmt).scalars().all()

    for m in messages:
        if m.chart_paths:
            for chart_url in m.chart_paths:
                _delete_chart_file(chart_url)
        db.delete(m)

    session = db.get(ChatSession, session_id)
    if session:
        db.delete(session)

    logger.info("已删除会话 %s 及其图表", session_id)


def _delete_chart_file(chart_path: str):
    try:
        if chart_path.startswith("/api/v1/"):
            filename = chart_path.split("/")[-1]
        elif chart_path.startswith("./result/"):
            filename = chart_path.replace("./result/", "")
        elif "/" in chart_path or "\\" in chart_path:
            filename = os.path.basename(chart_path)
        else:
            filename = chart_path

        file_path = os.path.join(CHART_DIR, filename)
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info("已删除图表文件: %s", file_path)
    except Exception as e:
        logger.warning("删除图表文件失败: path=%s error=%s", chart_path, str(e))


def _update_workspace_stats(db: Session, workspace_id: int):
    stmt = select(Task2QuestionItem).where(Task2QuestionItem.workspace_id == workspace_id)
    questions = list(db.execute(stmt).scalars().all())

    total = len(questions)
    pending = sum(1 for q in questions if q.status == QuestionStatus.PENDING)
    answered = sum(1 for q in questions if q.status == QuestionStatus.ANSWERED)
    failed = sum(1 for q in questions if q.status == QuestionStatus.FAILED)

    workspace = db.get(Task2Workspace, workspace_id)
    if workspace:
        workspace.total_questions = total
        workspace.pending_count = pending
        workspace.answered_count = answered
        workspace.failed_count = failed
        db.flush()


def _batch_answer_questions(workspace_id: int, scope: str, db: Session) -> dict:
    workspace = db.get(Task2Workspace, workspace_id)
    if workspace is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "工作台不存在")

    stmt = select(Task2QuestionItem).where(
        Task2QuestionItem.workspace_id == workspace_id
    ).order_by(Task2QuestionItem.question_code)
    all_questions = list(db.execute(stmt).scalars().all())

    if scope == "all":
        questions = [q for q in all_questions if q.status != QuestionStatus.ANSWERED]
    elif scope == "unfinished":
        questions = [q for q in all_questions if q.status == QuestionStatus.PENDING]
    elif scope == "failed":
        questions = [q for q in all_questions if q.status == QuestionStatus.FAILED]
    else:
        questions = [q for q in all_questions if q.status != QuestionStatus.ANSWERED]

    if not questions:
        return {
            "total": len(all_questions),
            "processed": 0,
            "success": 0,
            "failed": 0,
            "message": "没有需要处理的题目",
        }

    logger.info("开始批量回答: workspace_id=%d scope=%s count=%d", workspace_id, scope, len(questions))

    success_count = 0
    failed_count = 0
    results = []

    for idx, question in enumerate(questions):
        logger.info("批量处理进度: %d/%d - %s", idx + 1, len(questions), question.question_code)

        try:
            result = answer_single_question(question.id, db)
            success_count += 1
            results.append({
                "question_code": question.question_code,
                "status": "success",
                "result": result,
            })
        except Exception as exc:
            db.rollback()
            failed_count += 1
            results.append({
                "question_code": question.question_code,
                "status": "failed",
                "error": exc.message if isinstance(exc, ServiceException) else "回答失败",
            })
            logger.error("批量回答失败: %s - %s", question.question_code, str(exc))

    _update_workspace_stats(db, workspace_id)
    db.flush()
    _commit_or_raise(db)

    return {
        "total": len(all_questions),
        "processed": len(questions),
        "success": success_count,
        "failed": failed_count,
        "results": results,
    }
