"""LangGraph 底层流与控制层之间共享的领域事件定义。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class EventType(str, Enum):
    """抽象所有与传输协议无关的流事件类型。"""

    RESPONSE_CREATED = "response.created"
    RESPONSE_FIRST = "response.first"
    RESPONSE_DELTA = "response.delta"
    RESPONSE_COMPLETION = "response.completion"
    RESPONSE_DONE = "response.done"
    ERROR = "error"
    HEARTBEAT = "heartbeat"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"

    @classmethod
    def from_raw(cls, raw: str) -> "EventType":
        """将历史字符串类型尽量映射为枚举，无法识别时默认视为中间包。"""
        for event_type in cls:
            if event_type.value == raw:
                return event_type
        # 默认为中间包，尽量保持原有语义
        return cls.RESPONSE_DELTA


@dataclass
class StreamEvent:
    """流向控制器并等待具体传输编码的标准化事件结构。"""

    conversation_id: str
    event_type: EventType
    payload: Dict[str, Any] = field(default_factory=dict)
    source: str = "system"
    seq: Optional[int] = None
    trace_id: Optional[str] = None
