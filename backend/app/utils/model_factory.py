import logging

from functools import lru_cache

from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI
from langchain_community.embeddings import DashScopeEmbeddings

from app.core.config import settings

logger = logging.getLogger(__name__)


class ModelFactory:
    def __init__(self):
        self.default_chat_max_tokens = 32768
        self.default_chat_temperature = 0.1

        self.embedding_model = DashScopeEmbeddings(
            model=settings.EMBEDDING_MODEL,
            dashscope_api_key=settings.EMBEDDING_API_KEY,
        )

        self.chat_model = self.build_chat_model()

    def build_chat_model(
        self,
        *,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> ChatOpenAI:
        return ChatOpenAI(
            model=settings.CHAT_MODEL,
            api_key=settings.CHAT_API_KEY,
            base_url=settings.CHAT_BASE_URL,
            max_tokens=max_tokens or self.default_chat_max_tokens,
            temperature=(
                self.default_chat_temperature if temperature is None else temperature
            ),
        )


@lru_cache
def get_model_factory():
    return ModelFactory()


get_model = get_model_factory()
