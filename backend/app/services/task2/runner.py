"""任务二题目回答执行服务"""
import os
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.task2_question_item import Task2QuestionItem
from app.models.task2_workspace import Task2Workspace
from app.schemas.common import ErrorCode
from app.schemas import task2 as schemas_task2
from app.services import chat as chat_service
from app.services.task2.helpers import _delete_chart_file, _delete_session_and_charts
from app.services.task2.workspace import get_workspace_or_raise
from app.utils.exception import ServiceException
from app.db.database import commit_or_rollback
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)

CHART_DIR = os.path.join(os.getcwd(), "result")


# ========== 公共入口函数 ==========

def answer_single_question(question_id: int, db: Session):
    """回答指定任务二题目并保存问答结果。"""
    question = db.get(Task2QuestionItem, question_id)
    if question is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "题目不存在")

    if question.status == schemas_task2.QuestionStatus.ANSWERING:
        raise ServiceException(ErrorCode.PARAM_ERROR, "题目正在回答中")

    question.status = schemas_task2.QuestionStatus.ANSWERING
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

            logger.info(f"处理题目 {question.question_code} 轮次 {round_idx + 1}: {q_text[:50]}")

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

            except ServiceException:
                raise
            except Exception as exc:
                logger.error(f"题目 {question.question_code} 轮次 {round_idx + 1} 处理失败: {exc}")
                qa_pairs.append({
                    "Q": q_text,
                    "A": {"content": "回答生成失败：处理异常，请重新执行该题"},
                })
                raise

        question.session_id = session_id
        question.answer_json = qa_pairs
        question.sql_text = "\n\n".join(all_sqls) if all_sqls else None
        question.chart_type = "、".join(all_chart_types) if all_chart_types else "无"
        question.image_paths_json = all_image_paths if all_image_paths else None
        question.status = schemas_task2.QuestionStatus.ANSWERED
        question.answered_at = datetime.now()
        db.flush()

        _update_workspace_stats(db, question.workspace_id)
        commit_or_rollback(db)

        logger.info(f"题目 {question.question_code} 回答完成")

        return schemas_task2.AnswerResultResponse(
            question_id=question.id,
            question_code=question.question_code,
            status=question.status,
            answer_json=qa_pairs,
            sql_text=question.sql_text,
            chart_type=question.chart_type,
            image_paths=all_image_paths,
        )

    except Exception as exc:
        question.status = schemas_task2.QuestionStatus.FAILED
        question.last_error = exc.message if isinstance(exc, ServiceException) else "系统内部错误"
        db.flush()
        _update_workspace_stats(db, question.workspace_id)
        commit_or_rollback(db)
        logger.error(f"题目 {question.question_code} 回答失败: {exc}")
        if isinstance(exc, ServiceException):
            raise
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "回答失败") from exc


def delete_question_answer(question_id: int, db: Session):
    """删除指定任务二题目的已生成回答。"""
    question = db.get(Task2QuestionItem, question_id)
    if question is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "题目不存在")

    if question.status == schemas_task2.QuestionStatus.ANSWERING:
        raise ServiceException(ErrorCode.PARAM_ERROR, "题目正在回答中，无法删除")

    _reset_question_answer(db, question)

    question.status = schemas_task2.QuestionStatus.PENDING
    db.flush()

    _update_workspace_stats(db, question.workspace_id)
    commit_or_rollback(db)

    logger.info(f"题目 {question.question_code} 的回答已删除")

    return schemas_task2.DeleteAnswerResponse(
        question_id=question.id,
        question_code=question.question_code,
        status=question.status,
    )


def rerun_question(question_id: int, db: Session):
    """清理指定题目旧结果后重新回答。"""
    question = db.get(Task2QuestionItem, question_id)
    if question is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "题目不存在")

    if question.status == schemas_task2.QuestionStatus.ANSWERING:
        raise ServiceException(ErrorCode.PARAM_ERROR, "题目正在回答中，无法重新回答")

    _reset_question_answer(db, question)

    logger.info(f"题目 {question.question_code} 开始重新回答")

    return answer_single_question(question_id, db)


def batch_answer_questions(workspace_id: int, scope: str, db: Session):
    """按范围批量回答任务二工作台题目。"""
    return _batch_answer_questions(workspace_id=workspace_id, scope=scope, db=db)


def batch_answer_with_workspace_check(scope: str, db: Session):
    """校验工作台后批量回答题目。"""
    workspace = get_workspace_or_raise(db)
    return _batch_answer_questions(workspace_id=workspace.id, scope=scope, db=db)


"""辅助函数"""


def _reset_question_answer(db: Session, question: Task2QuestionItem):
    """清理题目的旧会话、图表文件和回答字段。"""
    if question.session_id:
        _delete_session_and_charts(db, question.session_id, CHART_DIR)

    if question.image_paths_json:
        for img_path in question.image_paths_json:
            _delete_chart_file(img_path, CHART_DIR)

    question.session_id = None
    question.answer_json = None
    question.sql_text = None
    question.chart_type = None
    question.image_paths_json = None
    question.last_error = None
    question.answered_at = None


def _update_workspace_stats(db: Session, workspace_id: int):
    stmt = select(Task2QuestionItem).where(Task2QuestionItem.workspace_id == workspace_id)
    questions = list(db.execute(stmt).scalars().all())

    total = len(questions)
    pending = sum(1 for q in questions if q.status == schemas_task2.QuestionStatus.PENDING)
    answered = sum(1 for q in questions if q.status == schemas_task2.QuestionStatus.ANSWERED)
    failed = sum(1 for q in questions if q.status == schemas_task2.QuestionStatus.FAILED)

    workspace = db.get(Task2Workspace, workspace_id)
    if workspace:
        workspace.total_questions = total
        workspace.pending_count = pending
        workspace.answered_count = answered
        workspace.failed_count = failed
        db.flush()


def _batch_answer_questions(workspace_id: int, scope: str, db: Session):
    workspace = db.get(Task2Workspace, workspace_id)
    if workspace is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "工作台不存在")

    stmt = select(Task2QuestionItem).where(
        Task2QuestionItem.workspace_id == workspace_id
    ).order_by(Task2QuestionItem.question_code)
    all_questions = list(db.execute(stmt).scalars().all())

    if scope == "all":
        questions = [q for q in all_questions if q.status != schemas_task2.QuestionStatus.ANSWERED]
    elif scope == "unfinished":
        questions = [q for q in all_questions if q.status == schemas_task2.QuestionStatus.PENDING]
    elif scope == "failed":
        questions = [q for q in all_questions if q.status == schemas_task2.QuestionStatus.FAILED]
    else:
        questions = [q for q in all_questions if q.status != schemas_task2.QuestionStatus.ANSWERED]

    if not questions:
        return schemas_task2.BatchAnswerResponse(
            total=len(all_questions),
            processed=0,
            success=0,
            failed=0,
            message="没有需要处理的题目",
        )

    logger.info(f"开始批量回答: workspace_id={workspace_id} scope={scope} count={len(questions)}")

    success_count = 0
    failed_count = 0
    results = []

    for idx, question in enumerate(questions):
        logger.info(f"批量处理进度: {idx + 1}/{len(questions)} - {question.question_code}")

        try:
            result = answer_single_question(question.id, db)
            success_count += 1
            results.append(schemas_task2.BatchAnswerResultItem(
                question_code=question.question_code,
                status="success",
                result=result,
            ))
        except Exception as exc:
            failed_count += 1
            results.append(schemas_task2.BatchAnswerResultItem(
                question_code=question.question_code,
                status="failed",
                error=exc.message if isinstance(exc, ServiceException) else "回答失败",
            ))
            logger.error(f"批量回答失败: {question.question_code} - {exc}")

    _update_workspace_stats(db, workspace_id)
    db.flush()
    commit_or_rollback(db)

    return schemas_task2.BatchAnswerResponse(
        total=len(all_questions),
        processed=len(questions),
        success=success_count,
        failed=failed_count,
        results=results,
    )
