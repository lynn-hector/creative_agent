"""流事件在不同传输协议下的编码抽象。"""

from __future__ import annotations

import json
from typing import Protocol

from app.core.streaming.events import StreamEvent


class StreamTransport(Protocol):
    """不同传输协议需将领域事件转换为客户端可消费的字节或字符串。"""

    async def encode(self, event: StreamEvent) -> str | bytes:
        """把 StreamEvent 序列化为具体传输层的载荷格式。"""
        ...


class SSETransport:
    """默认 SSE 传输，将事件编码为兼容现有前端的 data 帧。"""

    async def encode(self, event: StreamEvent) -> str:
        """按照 SSE 规范加上 `data:` 前缀与空行结尾。"""
        payload = dict(event.payload or {})

        payload.setdefault("conversation_id", event.conversation_id)
        payload.setdefault("type", event.event_type.value)

        if event.seq is not None:
            payload.setdefault("seq", event.seq)

        if event.trace_id:
            payload.setdefault("trace_id", event.trace_id)

        return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
