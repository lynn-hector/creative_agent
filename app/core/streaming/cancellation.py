"""控制器使用的取消信号抽象。"""

from __future__ import annotations

from typing import Iterable, Protocol

from fastapi import Request


class CancellationSource(Protocol):
    """定义一个取消信号源（HTTP 断连、Redis 标记等）。"""

    async def should_cancel(self, conversation_id: str) -> bool:
        """返回 True 表示应立即终止该会话。"""
        ...


class NullCancellationSource:
    """默认无操作实现，在未注入取消器时兜底。"""

    async def should_cancel(self, conversation_id: str) -> bool:
        return False


class RequestDisconnectSource:
    """基于 FastAPI Request 的客户端断连检测。"""

    def __init__(self, request: Request):
        self._request = request

    async def should_cancel(self, conversation_id: str) -> bool:
        """通过 ASGI 的断连标记在前端关闭连接时停止流。"""
        return await self._request.is_disconnected()


class CompositeCancellationSource:
    """组合多个取消源，只要任意一个返回 True 即触发。"""

    def __init__(self, sources: Iterable[CancellationSource]):
        self._sources = list(sources)

    async def should_cancel(self, conversation_id: str) -> bool:
        for source in self._sources:
            if await source.should_cancel(conversation_id):
                return True
        return False
