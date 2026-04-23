"""数据初始化脚本，单独运行"""

import sys, os

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from app.db.database import SessionLocal, engine, Base
import app.models  # noqa: F401

import bcrypt


def init():
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    init()
