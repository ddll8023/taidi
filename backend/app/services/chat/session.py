"""会话管理：查询、关闭、删除、重命名、导出"""
import json
import math
import os

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.database import commit_or_rollback
from app.models import chat_message as models_chat_message
from app.models import chat_session as models_chat_session
from app.schemas import chat as schemas_chat
from app.schemas.common import ErrorCode, PaginatedResponse, PaginationInfo
from app.services.chat.message import process_chat_message
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
logger = setup_logger(__name__)


# ========== 公共入口函数 ==========


def get_chat_sessions(
    db: Session, page: int = 1, page_size: int = 10
):
    """查询会话列表"""
    base_stmt = select(models_chat_session.ChatSession).where(
        models_chat_session.ChatSession.status == 0
    )
    total = db.scalar(select(func.count()).select_from(base_stmt.subquery())) or 0
    offset = (page - 1) * page_size
    records = db.scalars(
        base_stmt.order_by(models_chat_session.ChatSession.created_at.desc())
        .offset(offset)
        .limit(page_size)
    ).all()

    items = [
        schemas_chat.ChatSessionResponse.model_validate(r)
        for r in records
    ]

    total_pages = math.ceil(total / page_size) if total > 0 else 0
    pagination = PaginationInfo(
        page=page, page_size=page_size, total=total, total_pages=total_pages
    )
    return PaginatedResponse(lists=items, pagination=pagination)


def get_chat_history(
    session_id: str, db: Session
):
    """查询会话消息历史"""
    chat_session = db.get(models_chat_session.ChatSession, session_id)
    if chat_session is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "会话不存在")

    stmt = (
        select(models_chat_message.ChatMessage)
        .where(models_chat_message.ChatMessage.session_id == session_id)
        .order_by(models_chat_message.ChatMessage.created_at.asc())
    )
    messages = db.execute(stmt).scalars().all()

    return [
        schemas_chat.ChatMessageResponse(
            id=m.id,
            role=m.role,
            content=m.content,
            sql=m.sql_query,
            image=[
                f"/api/v1/chat/images/{os.path.basename(p)}" if p.startswith("/") or ":" in p else p
                for p in (m.chart_paths or [])
            ],
            created_at=m.created_at,
        )
        for m in messages
    ]


def close_chat_session(session_id: str, db: Session):
    """关闭会话"""
    chat_session = db.get(models_chat_session.ChatSession, session_id)
    if chat_session is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "会话不存在")

    chat_session.status = 1
    commit_or_rollback(db)
    logger.info(f"会话已关闭: session_id={session_id}")
    return schemas_chat.ChatSessionResponse.model_validate(chat_session)


def delete_chat_session(session_id: str, db: Session):
    """删除会话及其消息"""
    chat_session = db.get(models_chat_session.ChatSession, session_id)
    if chat_session is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "会话不存在")

    stmt = select(models_chat_message.ChatMessage).where(
        models_chat_message.ChatMessage.session_id == session_id
    )
    messages = db.execute(stmt).scalars().all()

    chart_dir = os.path.join(os.getcwd(), "result")
    for m in messages:
        if m.chart_paths:
            for chart_url in m.chart_paths:
                try:
                    filename = os.path.basename(chart_url)
                    file_path = os.path.join(chart_dir, filename)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        logger.info(f"图表已删除: {file_path}")
                except Exception as e:
                    logger.warning(f"删除图表失败: chart_url={chart_url} error={e}")
        db.delete(m)

    db.delete(chat_session)
    commit_or_rollback(db)
    logger.info(f"会话已删除: session_id={session_id}")
    return schemas_chat.ChatSessionResponse.model_validate(chat_session)


def rename_chat_session(session_id: str, name: str, db: Session):
    """重命名会话"""
    chat_session = db.get(models_chat_session.ChatSession, session_id)
    if chat_session is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, "会话不存在")

    chat_session.name = name
    commit_or_rollback(db)
    logger.info(f"会话已重命名: session_id={session_id}, name={name}")
    return schemas_chat.ChatSessionResponse.model_validate(chat_session)


def export_chat_results(questions: list[dict], db: Session):
    """批量执行问答并导出结果"""
    from openpyxl import Workbook

    wb = Workbook()
    wb.remove(wb.active)

    all_results = []

    for idx, item in enumerate(questions):
        question_id = item.get("id", f"B{idx + 1:03d}")
        question_json_str = item.get("question", "[]")

        try:
            rounds = json.loads(question_json_str)
        except (json.JSONDecodeError, TypeError):
            rounds = [{"Q": question_json_str}]

        if not isinstance(rounds, list):
            rounds = [{"Q": str(rounds)}]

        session_id = None
        qa_pairs = []
        all_sqls = []
        all_chart_types = []
        chart_sequence = 1

        for round_idx, round_item in enumerate(rounds):
            q_text = (
                round_item.get("Q", "")
                if isinstance(round_item, dict)
                else str(round_item)
            )
            if not q_text.strip():
                continue

            try:
                response = process_chat_message(
                    session_id=session_id,
                    question=q_text,
                    db=db,
                    question_id=question_id,
                    chart_sequence=chart_sequence,
                )
                session_id = response.session_id

                image_paths = []
                if response.answer and response.answer.image:
                    for img in response.answer.image:
                        filename = (
                            os.path.basename(img) if "/" in img or "\\" in img else img
                        )
                        image_paths.append(f"./result/{filename}")
                        chart_sequence += 1

                answer_content = (
                    response.answer.content
                    if response.answer and response.answer.content
                    else ""
                )
                if not answer_content:
                    answer_content = "回答内容为空"

                answer_data = {
                    "Q": q_text,
                    "A": {
                        "content": answer_content,
                    },
                }
                if image_paths:
                    answer_data["A"]["image"] = image_paths
                    if response.chart_type:
                        chart_type_map = {
                            "line": "折线图",
                            "bar": "柱状图",
                            "pie": "饼图",
                            "horizontal_bar": "水平柱状图",
                            "grouped_bar": "分组柱状图",
                            "radar": "雷达图",
                            "histogram": "直方图",
                            "scatter": "散点图",
                            "box": "箱线图",
                        }
                        all_chart_types.append(
                            chart_type_map.get(response.chart_type, "图表")
                        )

                if response.sql:
                    all_sqls.append(response.sql)

                qa_pairs.append(answer_data)

                if response.need_clarification:
                    continue

            except Exception as exc:
                logger.error(
                    f"批量问答失败: question_id={question_id} round={round_idx} error={exc}",
                    exc_info=True,
                )
                qa_pairs.append(
                    {
                        "Q": q_text,
                        "A": {"content": "回答生成失败"},
                    }
                )

        qa_pairs = _ensure_non_empty_qa_pairs(question_json_str, qa_pairs)
        sql_query = "\n\n".join(all_sqls) if all_sqls else ""
        chart_type = "、".join(all_chart_types) if all_chart_types else "无"

        result_item = {
            "id": question_id,
            "question": question_json_str,
            "sql": sql_query,
            "chart_type": chart_type,
            "answer": qa_pairs,
        }
        all_results.append(result_item)

        ws = wb.create_sheet(title=question_id)
        ws.append(["编号", "问题", "SQL查询语句", "图形格式", "回答"])
        ws.append(
            [
                question_id,
                json.dumps(rounds, ensure_ascii=False),
                sql_query,
                chart_type,
                json.dumps(qa_pairs, ensure_ascii=False),
            ]
        )

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

    result_dir = os.path.join(os.getcwd(), "result")
    os.makedirs(result_dir, exist_ok=True)
    result_path = os.path.join(result_dir, "result_2.xlsx")
    wb.save(result_path)
    logger.info(f"result_2.xlsx 已生成: {result_path}, 共 {len(questions)} 个问题")

    json_path = os.path.join(result_dir, "result_2.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    logger.info(f"result_2.json 已生成: {json_path}")

    return schemas_chat.ChatExportResponse(file_path=result_path)


"""辅助函数"""


def _ensure_non_empty_qa_pairs(
    question_json_str: str,
    qa_pairs: list[dict],
):
    """确保 QA 对列表不为空，空时生成兜底条目"""
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
            "A": {"content": "回答生成失败：未生成任何有效轮次结果，请重新执行该题。"},
        }
    ]
