import logging

from functools import lru_cache

from langchain_anthropic import ChatAnthropic
from langchain_community.embeddings import DashScopeEmbeddings

from app.core.config import settings

logger = logging.getLogger(__name__)


class ModelFactory:
    def __init__(self):
        self.default_chat_max_tokens = 32768
        self.default_chat_temperature = 0.1

        logger.info(
            "[ModelFactory] 初始化 Embedding 模型: model=%s, key长度=%d, key前4位=%s",
            settings.EMBEDDING_MODEL,
            len(settings.EMBEDDING_API_KEY) if settings.EMBEDDING_API_KEY else 0,
            settings.EMBEDDING_API_KEY[:4] if settings.EMBEDDING_API_KEY else "EMPTY",
        )

        if not settings.EMBEDDING_API_KEY:
            logger.error("[ModelFactory] EMBEDDING_API_KEY 为空，请检查环境变量配置")

        self.embedding_model = DashScopeEmbeddings(
            model=settings.EMBEDDING_MODEL,
            dashscope_api_key=settings.EMBEDDING_API_KEY,
        )
        logger.info("[ModelFactory] DashScopeEmbeddings 初始化完成")

        self.chat_model = self.build_chat_model()

    def build_chat_model(
        self,
        *,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> ChatAnthropic:
        return ChatAnthropic(
            model=settings.CHAT_MODEL,
            anthropic_api_key=settings.CHAT_API_KEY,
            anthropic_api_url=settings.MINIMAX_BASE_URL,
            max_tokens=max_tokens or self.default_chat_max_tokens,
            temperature=(
                self.default_chat_temperature if temperature is None else temperature
            ),
        )


@lru_cache
def get_model_factory():
    return ModelFactory()


get_model = get_model_factory()
