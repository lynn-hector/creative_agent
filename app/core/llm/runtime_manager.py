from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Callable, Dict, Optional

from langchain.chat_models import init_chat_model
from langchain_core.language_models.chat_models import BaseChatModel

from app.settings import settings


@dataclass
class LLMConfig:
    """用于创建 LLM 的基础配置。"""

    model: str
    api_key: str
    base_url: Optional[str] = None
    temperature: float = 0.0


def default_llm_config_loader(model_id: Optional[str]) -> LLMConfig:
    """根据 settings 生成默认的 DeepSeek 配置。"""
    resolved_model = (
        model_id
        or settings.DEEPSEEK_MODEL
        or settings.DEFAULT_LLM_MODEL
        or "deepseek:deepseek-reasoner"
    )

    if not settings.DEEPSEEK_API_KEY:
        raise ValueError("DEEPSEEK_API_KEY is not configured")

    return LLMConfig(
        model=resolved_model,
        api_key=settings.DEEPSEEK_API_KEY,
        base_url=settings.DEEPSEEK_API_BASE or "https://api.deepseek.com/v1",
        temperature=0.0,
    )


class LLMRuntimeManager:
    """负责 LLM 实例的创建、复用与关闭。"""

    def __init__(
        self,
        default_model: Optional[str],
        config_loader: Callable[[Optional[str]], LLMConfig] = default_llm_config_loader,
    ):
        self._default_model = default_model
        self._config_loader = config_loader
        self._cache: Dict[str, BaseChatModel] = {}
        self._lock = asyncio.Lock()

    async def get_llm(self, model_id: Optional[str] = None) -> BaseChatModel:
        """按模型 ID 获取/缓存 LLM。"""
        key = str(model_id) if model_id else (self._default_model or "")
        if not key:
            raise ValueError("model_id is required and default model is not configured")

        if key in self._cache:
            return self._cache[key]

        async with self._lock:
            if key in self._cache:
                return self._cache[key]

            config = self._config_loader(model_id)
            llm = init_chat_model(
                model=config.model,
                temperature=config.temperature,
                base_url=config.base_url,
                api_key=config.api_key,
            )
            self._cache[key] = llm
            return llm

    async def preload(self, model_ids: Optional[list[str]] = None) -> None:
        """预热指定模型，减少首包延迟。"""
        targets = model_ids or [self._default_model] if self._default_model else []
        for model_id in targets:
            if model_id:
                await self.get_llm(model_id)

    async def close(self) -> None:
        """关闭所有 LLM 连接。部分模型支持 aclose。"""
        for llm in self._cache.values():
            close_fn = getattr(llm, "aclose", None)
            if close_fn:
                await close_fn()
        self._cache.clear()
