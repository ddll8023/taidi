from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase
from app.core.config import settings
from app.schemas.common import ErrorCode
from app.utils.exception import ServiceException

# 创建数据库引擎
engine = create_engine(
    settings.DATABASE_URL, pool_pre_ping=True, pool_size=10, max_overflow=20
)

# 创建会话工厂
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# 模型基类
class Base(DeclarativeBase):
    pass


# 数据库会话依赖
def get_db():
    with SessionLocal() as db:
        try:
            yield db
        except Exception as exc:
            db.rollback()
            raise ServiceException(ErrorCode.INTERNAL_ERROR, "操作失败") from exc


def get_background_db_session():
    """
    获取后台任务数据库会话
    用于 BackgroundTasks.add_task() 场景
    """
    return SessionLocal()


def commit_or_rollback(db: Session):
    """提交当前事务，失败时回滚并转换为业务异常。"""
    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        raise ServiceException(ErrorCode.INTERNAL_ERROR, "操作失败") from exc
