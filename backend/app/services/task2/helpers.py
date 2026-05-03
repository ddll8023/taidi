"""任务二跨文件共享辅助函数"""
import os

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.chat_message import ChatMessage
from app.models.chat_session import ChatSession
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


def _delete_chart_file(chart_path: str, chart_dir: str):
    """删除指定路径的图表文件。"""
    try:
        if chart_path.startswith("/api/v1/"):
            filename = chart_path.split("/")[-1]
        elif chart_path.startswith("./result/"):
            filename = chart_path.replace("./result/", "")
        elif "/" in chart_path or "\\" in chart_path:
            filename = os.path.basename(chart_path)
        else:
            filename = chart_path

        file_path = os.path.join(chart_dir, filename)
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"已删除图表文件: {file_path}")
    except Exception as e:
        logger.warning(f"删除图表文件失败: path={chart_path} error={str(e)}")


def _delete_session_and_charts(db: Session, session_id: str, chart_dir: str):
    """删除会话及其关联的图表消息和文件。"""
    if not session_id:
        return

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

    logger.info(f"已删除会话 {session_id} 及其图表")
