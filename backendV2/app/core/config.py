import os
from functools import lru_cache

from pydantic_settings import BaseSettings
from app.utils.file import ROOT_DIR
from app.core.prompt_config import PromptConfig, prompt_config


class Settings(BaseSettings):
    # 数据库配置
    MYSQL_HOST: str = "127.0.0.1"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = ""
    MYSQL_PASSWORD: str = ""
    MYSQL_DATABASE: str = "financial_report"

    @property
    def DATABASE_URL(self) -> str:
        return (
            f"mysql+pymysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}"
            f"@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.MYSQL_DATABASE}?charset=utf8mb4"
        )

    # 上传配置
    UPLOAD_DIR: str = "uploads"

    @property
    def FINCANCIAL_REPORT_UPLOAD_DIR(self):
        path = os.path.join(ROOT_DIR, self.UPLOAD_DIR, "financial_report")
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def RESEARCH_REPORT_UPLOAD_DIR(self):
        path = os.path.join(ROOT_DIR, self.UPLOAD_DIR, "research_report")
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def json_UPLOAD_DIR(self) -> str:
        """JSON文件保存目录 = financial_report目录下的json子目录"""
        path = os.path.join(ROOT_DIR, self.FINCANCIAL_REPORT_UPLOAD_DIR, "json")
        os.makedirs(path, exist_ok=True)
        return path

    # 模型配置
    CHAT_PROVIDER: str = ""
    CHAT_BASE_URL: str = ""
    CHAT_MODEL: str = ""
    CHAT_API_KEY: str = ""
    EMBEDDING_MODEL: str = ""
    EMBEDDING_DIM: int = 1024
    EMBEDDING_API_KEY: str = ""

    # 提示词配置
    PROMPT_CONFIG: PromptConfig = prompt_config

    # RAG配置
    CHROMA_PERSIST_DIR: str = "chroma_data"
    CHROMA_KB_COLLECTION: str = "knowledge_chunk_embedding"
    CHUNK_SIZE: int = 1600
    CHUNK_OVERLAP: int = 220
    CHUNK_SEPARATORS: list[str] = ["\n\n", "\n", "。", ". ", " ", ""]

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache()
def get_settings():
    return Settings()


settings = get_settings()
