"""LangGraph 生成器到统一流接口的包装实现。"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator, Protocol

from app.core.streaming.events import StreamEvent
from app.core.agent_factory.orchestrator import Orchestrator
from app.schemas.chat import ChatV1Request


class StreamProvider(Protocol):
    """统一的异步流接口，可由 StreamController 直接消费。"""

    conversation_id: str

    def __aiter__(self) -> AsyncIterator[StreamEvent]:
        """返回一个按顺序产出 StreamEvent 的异步迭代器。"""
        ...

    async def cancel(self) -> None:
        """停止产出事件并释放上游资源。"""
        ...


class LangGraphStreamProvider:
    """将 Orchestrator.stream_process 包装成 StreamProvider。"""

    def __init__(
        self,
        orchestrator: Orchestrator,
        chat_request: ChatV1Request,
        config: dict | None = None,
    ):
        self._orchestrator = orchestrator
        self._chat_request = chat_request
        self._config = config
        self._generator = orchestrator.stream_process(chat_request, config=config)
        self.conversation_id = chat_request.conversation_id
        self._closed = False
        self._lock = asyncio.Lock()

    def __aiter__(self) -> AsyncIterator[StreamEvent]:
        return self

    async def __anext__(self) -> StreamEvent:
        """转发 LangGraph 事件并管理完成状态。"""
        if self._closed:
            raise StopAsyncIteration
        try:
            return await self._generator.__anext__()
        except StopAsyncIteration:
            self._closed = True
            raise

    async def cancel(self) -> None:
        """幂等地关闭底层异步生成器。"""
        async with self._lock:
            if self._closed:
                return
            self._closed = True

            aclose = getattr(self._generator, "aclose", None)
            if aclose:
                await aclose()
