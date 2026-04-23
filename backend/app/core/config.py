import os
from functools import lru_cache

from pydantic_settings import BaseSettings

from app.core.prompt_config import PromptConfig, prompt_config


class Settings(BaseSettings):
    MYSQL_HOST: str = "localhost"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "root"
    MYSQL_PASSWORD: str = ""
    MYSQL_DATABASE: str = "financial_report"

    @property
    def DATABASE_URL(self) -> str:
        return (
            f"mysql+pymysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}"
            f"@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.MYSQL_DATABASE}?charset=utf8mb4"
        )

    UPLOAD_DIR: str = "uploads"
    fujian2_DIR: str = "fujian2"
    fujian5_DIR: str = "fujian5"
    JSON_SUBDIR: str = "json"

    @property
    def fujian2_UPLOAD_DIR(self) -> str:
        path = os.path.join(self.UPLOAD_DIR, self.fujian2_DIR)
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def fujian5_UPLOAD_DIR(self) -> str:
        path = os.path.join(self.UPLOAD_DIR, self.fujian5_DIR)
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def json_UPLOAD_DIR(self) -> str:
        """JSON文件保存目录 = fujian2目录下的json子目录"""
        path = os.path.join(self.fujian2_UPLOAD_DIR, self.JSON_SUBDIR)
        os.makedirs(path, exist_ok=True)
        return path

    MINIMAX_BASE_URL: str = "https://api.minimaxi.com/anthropic"
    AUDIO_MODEL: str = "qwen3-asr-flash-filetrans"
    AUDIO_MODEL_TIMEOUT: int = 600
    CHAT_MODEL: str = "MiniMax-M2.7"
    EMBEDDING_MODEL: str = "text-embedding-v2"
    EMBEDDING_DIM: int = 1024
    EMBEDDING_API_KEY: str = ""
    CHAT_API_KEY: str = ""
    PROMPT_CONFIG: PromptConfig = prompt_config

    MILVUS_URI: str = "http://127.0.0.1:19530"
    MILVUS_COLLECTION: str = "financial_report_embedding"
    MILVUS_KB_COLLECTION: str = "knowledge_chunk_embedding"
    CHUNK_SIZE: int = 1600
    CHUNK_OVERLAP: int = 160

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
