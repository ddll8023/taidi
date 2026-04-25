"""任务三工作台执行服务。"""

import json
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.task3_question_item import Task3QuestionItem
from app.models.task3_workspace import Task3Workspace
from app.schemas.common import ErrorCode
from app.schemas.task3 import (
    StepStatus,
    StepType,
    Task3BatchAnswerResponse,
    Task3QuestionActionResponse,
)
from app.services.task3.planner import process_task3_question
from app.services.task3.verifier import verify_execution_trace
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


"""辅助函数"""


def _to_jsonable(value):
    """将复杂对象递归转换为 JSON 可序列化结构。"""
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(item) for item in value]
    return value


def _build_failure_message(verification: dict | None, fallback: str = "任务三回答未通过校验"):
    """根据校验结果拼接失败原因文本。"""
    if not verification:
        return fallback

    errors = verification.get("errors") or []
    warnings = verification.get("warnings") or []
    messages = [str(item) for item in [*errors, *warnings] if item]
    if not messages:
        return fallback
    return "; ".join(messages)[:2000]


def _parse_question_rounds(question_json_str: str):
    """解析题目原始 JSON 为轮次列表。"""
    try:
        parsed = json.loads(question_json_str)
    except (json.JSONDecodeError, TypeError):
        parsed = [{"Q": question_json_str}]

    if not isinstance(parsed, list):
        parsed = [{"Q": str(parsed)}]

    rounds = []
    for item in parsed:
        if isinstance(item, dict):
            q_text = str(item.get("Q", "")).strip()
        else:
            q_text = str(item).strip()
        if q_text:
            rounds.append({"Q": q_text})

    if not rounds:
        rounds.append({"Q": str(question_json_str or "").strip()})
    return rounds


def _build_standalone_question(q_text: str, previous_rounds: list[dict]):
    """将当前追问与历史轮次拼接为独立问题。"""
    if not previous_rounds:
        return q_text

    history_parts = []
    for idx, item in enumerate(previous_rounds, start=1):
        question_text = str(item.get("Q", "")).strip()
        answer_text = str(item.get("A", "")).strip()
        if len(answer_text) > 300:
            answer_text = answer_text[:300] + "..."
        history_parts.append(
            f"第{idx}轮问题：{question_text}\n第{idx}轮回答：{answer_text}"
        )

    history_text = "\n\n".join(history_parts)
    return (
        "请结合以下多轮对话上下文，补全当前追问中的省略信息后回答。\n"
        f"历史上下文：\n{history_text}\n\n"
        f"当前问题：{q_text}"
    )


def _build_reference_json(ref):
    """将引用对象规范化为输出字典。"""
    if isinstance(ref, dict):
        return {
            "paper_path": ref.get("paper_path"),
            "text": ref.get("text") or "",
            "paper_image": ref.get("paper_image"),
        }
    return {
        "paper_path": ref.paper_path if hasattr(ref, "paper_path") else None,
        "text": ref.text if hasattr(ref, "text") else "",
        "paper_image": ref.paper_image if hasattr(ref, "paper_image") else None,
    }


def _build_answer_item(q_text: str, response):
    """构造单轮回答的输出对象。"""
    references = []
    for ref in response.answer.references:
        ref_json = _build_reference_json(ref)
        if ref_json.get("text") or ref_json.get("paper_path"):
            references.append(ref_json)

    answer_payload = {"content": response.answer.content}
    if references:
        answer_payload["references"] = references

    return {
        "Q": q_text,
        "A": answer_payload,
    }


def _build_retrieval_summary(trace):
    """从执行轨迹中提取检索摘要。"""
    retrieve_steps = [
        r for r in trace.results
        if r.step_type == StepType.RETRIEVE_EVIDENCE and r.status == StepStatus.COMPLETED
    ]
    if not retrieve_steps:
        return None

    total_hits = sum(
        r.output.get("evidence_count", 0)
        for r in retrieve_steps
    )

    doc_types = set()
    stock_filters = set()
    for r in retrieve_steps:
        evidence_list = r.output.get("evidence", [])
        for ev in evidence_list:
            if isinstance(ev, dict):
                if ev.get("doc_type"):
                    doc_types.add(ev["doc_type"])
                if ev.get("stock_code"):
                    stock_filters.add(ev["stock_code"])

    return {
        "triggered": True,
        "hit_count": total_hits,
        "doc_types": list(doc_types) if doc_types else None,
        "stock_filter": list(stock_filters)[0] if len(stock_filters) == 1 else (list(stock_filters) if stock_filters else None),
        "generated_references": len(trace.references) > 0,
    }


# ========== 公共入口函数 ==========

def answer_single_question(question_id: int, db: Session):
    """回答指定题目并持久化结果。"""
    question = db.get(Task3QuestionItem, question_id)
    if question is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "题目不存在")

    question.status = 1  # 回答中
    db.flush()

    try:
        rounds = _parse_question_rounds(question.question_raw_json or "")
        answer_json = []
        all_sqls = []
        verifications = []
        retrieval_summaries = []
        execution_plans = []
        previous_rounds = []

        for round_idx, round_item in enumerate(rounds):
            q_text = round_item.get("Q", "")
            standalone_question = _build_standalone_question(q_text, previous_rounds)
            context = {
                "question_id": question.question_code,
                "previous_rounds": previous_rounds,
                "previous_sqls": all_sqls,
                "original_question": q_text,
                "standalone_question": standalone_question,
            }

            response = process_task3_question(
                question=standalone_question,
                db=db,
                context=context,
            )

            trace = response.execution_trace
            if trace and trace.plan:
                execution_plans.append(trace.plan)
            verification = verify_execution_trace(db, trace)
            verifications.append(verification)

            retrieval_summary = _build_retrieval_summary(trace)
            if retrieval_summary:
                retrieval_summaries.append(retrieval_summary)

            for result in trace.results:
                if result.step_type == StepType.SQL_QUERY and result.status == StepStatus.COMPLETED:
                    sql = result.output.get("sql")
                    if sql:
                        all_sqls.append(sql)

            answer_item = _build_answer_item(q_text, response)
            answer_json.append(answer_item)

            previous_rounds.append({
                "Q": q_text,
                "A": answer_item["A"].get("content", ""),
            })

        verification = {
            "passed": bool(verifications) and all(item.passed for item in verifications),
            "rounds": verifications,
        }
        retrieval_summary = {
            "triggered": any(item.get("triggered") for item in retrieval_summaries),
            "rounds": retrieval_summaries,
        } if retrieval_summaries else None

        question.answer_json = _to_jsonable(answer_json)
        question.sql_text = "\n\n".join(all_sqls) if all_sqls else None
        question.execution_plan = (
            {"rounds": _to_jsonable(execution_plans)}
            if execution_plans
            else None
        )
        question.verification = _to_jsonable(verification)
        question.retrieval_summary = _to_jsonable(retrieval_summary)
        if answer_json and verification.get("passed", False):
            question.status = 2  # 已完成
            question.last_error = None
        else:
            question.status = 3  # 失败
            question.last_error = _build_failure_message(verification)
        question.answered_at = datetime.now()

        _sync_workspace_stats(db, question.workspace_id)
        db.commit()
        db.refresh(question)

        return Task3QuestionActionResponse(
            id=question.id,
            status=question.status,
            answered_at=question.answered_at,
        )

    except Exception as e:
        logger.error("回答题目失败: question_id=%s, error=%s", question_id, str(e), exc_info=True)
        db.rollback()
        failed_question = db.get(Task3QuestionItem, question_id)
        if failed_question is not None:
            failed_question.status = 3  # 失败
            failed_question.last_error = str(e)
            failed_question.answered_at = datetime.now()
            try:
                _sync_workspace_stats(db, failed_question.workspace_id)
                db.commit()
            except Exception:
                logger.error("保存题目失败状态失败: question_id=%s", question_id, exc_info=True)
                db.rollback()
        raise


def delete_question_answer(question_id: int, db: Session):
    """删除指定题目的回答结果。"""
    question = db.get(Task3QuestionItem, question_id)
    if question is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "题目不存在")

    question.answer_json = None
    question.sql_text = None
    question.execution_plan = None
    question.verification = None
    question.retrieval_summary = None
    question.last_error = None
    question.answered_at = None
    question.status = 0  # 待处理
    _sync_workspace_stats(db, question.workspace_id)
    db.commit()

    return Task3QuestionActionResponse(
        id=question_id,
        status=question.status,
        answered_at=question.answered_at,
    )


def rerun_question(question_id: int, db: Session):
    """清空旧结果并重新回答指定题目。"""
    question = db.get(Task3QuestionItem, question_id)
    if question is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "题目不存在")

    question.answer_json = None
    question.sql_text = None
    question.execution_plan = None
    question.verification = None
    question.retrieval_summary = None
    question.last_error = None
    question.answered_at = None
    db.commit()

    return answer_single_question(question_id, db)


def _sync_workspace_stats(db: Session, workspace_id: int):
    """同步工作台题目统计信息。"""
    workspace = db.get(Task3Workspace, workspace_id)
    if workspace is None:
        return

    stmt = select(Task3QuestionItem).where(Task3QuestionItem.workspace_id == workspace_id)
    questions = db.execute(stmt).scalars().all()

    workspace.total_questions = len(questions)
    workspace.answered_count = sum(1 for q in questions if q.status == 2)
    workspace.failed_count = sum(1 for q in questions if q.status == 3)
    workspace.pending_count = sum(1 for q in questions if q.status == 0)
    db.flush()


def batch_answer_questions(
    workspace_id: int,
    scope: str,
    db: Session,
):
    """按范围批量回答工作台中的题目。"""
    stmt = select(Task3QuestionItem).where(Task3QuestionItem.workspace_id == workspace_id)
    if scope == "unfinished":
        stmt = stmt.where(Task3QuestionItem.status.in_([0, 3]))
    elif scope == "failed":
        stmt = stmt.where(Task3QuestionItem.status == 3)
    # scope == "all": no filter

    questions = db.execute(stmt).scalars().all()

    success_count = 0
    failed_count = 0

    for q in questions:
        try:
            result = answer_single_question(q.id, db)
            if result.status == 2:
                success_count += 1
            else:
                failed_count += 1
        except Exception as e:
            failed_count += 1
            logger.warning("批量回答中题目 %s 失败: %s", q.question_code, str(e))

    _sync_workspace_stats(db, workspace_id)
    db.commit()

    return Task3BatchAnswerResponse(success=success_count, failed=failed_count)


def batch_answer_with_workspace_check(scope: str, db: Session):
    """校验工作台后批量回答题目。"""
    from app.services.task3.importer import get_workspace_or_raise
    workspace = get_workspace_or_raise(db)
    return batch_answer_questions(workspace_id=workspace.id, scope=scope, db=db)
