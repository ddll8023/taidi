"""会话管理：增删改查、关闭、重命名、导出"""
import json
import os

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.constants import chat as constants_chat
from app.db.database import commit_or_rollback
from app.models import chat_message as models_chat_message
from app.models import chat_session as models_chat_session
from app.schemas import chat as schemas_chat
from app.schemas.common import ErrorCode, PaginatedResponse, PaginationInfo
from app.utils.exception import ServiceException
from app.utils.logger_config import setup_logger
from app.services.chat.context import _convert_path_to_url

logger = setup_logger(__name__)


"""辅助函数"""


def get_chat_sessions(
    db: Session, page: int = 1, page_size: int = 10
):
    base_stmt = select(models_chat_session.ChatSession).where(
        models_chat_session.ChatSession.status == 0
    )
    total = db.scalar(select(func.count()).select_from(base_stmt.subquery())) or 0
    offset = (page - 1) * page_size
    records = db.scalars(
        base_stmt.order_by(models_chat_session.ChatSession.updated_at.desc())
        .offset(offset)
        .limit(page_size)
    ).all()

    items = [
        schemas_chat.ChatSessionResponse(
            id=r.id,
            name=r.name,
            status=r.status,
            created_at=r.created_at,
            updated_at=r.updated_at,
        )
        for r in records
    ]

    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    pagination = PaginationInfo(
        page=page, page_size=page_size, total=total, total_pages=total_pages
    )
    return PaginatedResponse(lists=items, pagination=pagination)


def get_chat_history(
    session_id: str, db: Session
):
    chat_session = db.get(models_chat_session.ChatSession, session_id)
    if chat_session is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"会话不存在: {session_id}")

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
                _convert_path_to_url(p) if p.startswith("/") or ":" in p else p
                for p in (m.chart_paths or [])
            ],
            created_at=m.created_at,
        )
        for m in messages
    ]


def close_chat_session(session_id: str, db: Session):
    chat_session = db.get(models_chat_session.ChatSession, session_id)
    if chat_session is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"会话不存在: {session_id}")

    chat_session.status = 1
    commit_or_rollback(db)
    logger.info("会话已关闭: session_id=%s", session_id)
    return True


def delete_chat_session(session_id: str, db: Session):
    import os

    chat_session = db.get(models_chat_session.ChatSession, session_id)
    if chat_session is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"会话不存在: {session_id}")

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
                        logger.info("图表已删除: %s", file_path)
                except Exception as e:
                    logger.warning(
                        "删除图表失败: chart_url=%s error=%s", chart_url, str(e)
                    )
        db.delete(m)

    db.delete(chat_session)
    commit_or_rollback(db)
    logger.info("会话已删除: session_id=%s", session_id)
    return True


def rename_chat_session(session_id: str, name: str, db: Session):
    chat_session = db.get(models_chat_session.ChatSession, session_id)
    if chat_session is None:
        raise ServiceException(ErrorCode.DATA_NOT_FOUND, f"会话不存在: {session_id}")

    chat_session.name = name
    commit_or_rollback(db)
    logger.info("会话已重命名: session_id=%s, name=%s", session_id, name)
    return True


def export_result_2(questions: list[dict], db: Session):
    import os

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
                from app.services.chat.message import process_chat_message

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
                    "批量问答失败: question_id=%s round=%d error=%s",
                    question_id,
                    round_idx,
                    str(exc),
                )
                error_msg = (
                    f"回答生成失败: {str(exc)}" if exc else "回答生成失败: 未知错误"
                )
                qa_pairs.append(
                    {
                        "Q": q_text,
                        "A": {"content": error_msg},
                    }
                )

        from app.services.chat.message import _ensure_non_empty_qa_pairs as _ensure_qa

        qa_pairs = _ensure_qa(question_json_str, qa_pairs)
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
    logger.info("result_2.xlsx 已生成: %s, 共 %d 个问题", result_path, len(questions))

    json_path = os.path.join(result_dir, "result_2.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    logger.info("result_2.json 已生成: %s", json_path)

    return result_path
