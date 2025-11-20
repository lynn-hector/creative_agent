import logging
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from app.core.msg_manage import message_enum
from app.core.msg_manage.ds import parse_ds_message_chunk_v2

logger = logging.getLogger(__name__)


@dataclass
class MessageAdapterResult:
    conversation_type: str
    response_type: str
    content: str
    response_source: str
    response_name: str


class StreamContext:
    """记录当前流的状态（seq、首包等）。"""

    def __init__(self, conversation_id: str):
        self.conversation_id = conversation_id
        self.seq = 0
        self._first_emitted = False

    def next_sequence(self, conversation_type: str) -> int:
        """根据消息类型计算序号，完成包沿用上一条 seq。"""
        if (
            conversation_type
            in [
                message_enum.RESPONSE_STATUS_TYPE_COMPLETION,
                message_enum.RESPONSE_STATUS_TYPE_DONE,
            ]
            and self.seq > 0
        ):
            return self.seq

        if conversation_type != message_enum.RESPONSE_STATUS_TYPE_CREATED:
            self.seq += 1
        return self.seq

    def adjust_type_for_first(self, conversation_type: str, seq: int) -> str:
        """首包需要替换成 response.first。"""
        if (
            not self._first_emitted
            and seq == 1
            and conversation_type != message_enum.RESPONSE_STATUS_TYPE_CREATED
        ):
            self._first_emitted = True
            return message_enum.RESPONSE_STATUS_TYPE_FIRST
        return conversation_type


class LangGraphMessageAdapter:
    """
    将 LangGraph 的 (agent_info, message_chunk) 转换为统一结构。
    """

    def adapt(self, agent_info, message_chunk) -> Optional[MessageAdapterResult]:
        if not message_chunk:
            return None

        message = message_chunk[0]
        response_source = message_enum.Role_Type_System_LLM
        response_name = str(uuid.uuid4())

        if agent_info and len(agent_info) > 0:
            response_name = agent_info[0]
            agent_sources = response_name.split(":")
            if len(agent_sources) > 1:
                response_source = agent_sources[0]

        conversation_type, content_type, content = parse_ds_message_chunk_v2(
            message, response_source
        )

        return MessageAdapterResult(
            conversation_type=conversation_type,
            response_type=content_type,
            content=content,
            response_source=response_source,
            response_name=response_name,
        )


class ResponseBuilder:
    """
    基于上下文状态，构造统一的实时响应结构。
    """

    def build_message(self, context: StreamContext, payload: MessageAdapterResult):
        seq = context.next_sequence(payload.conversation_type)
        conversation_type = context.adjust_type_for_first(
            payload.conversation_type, seq
        )

        content = payload.content or ""
        is_completion = conversation_type in [
            message_enum.RESPONSE_STATUS_TYPE_COMPLETION,
            message_enum.RESPONSE_STATUS_TYPE_DONE,
        ]
        if is_completion and not content:
            content = ""

        if content or conversation_type in [
            message_enum.RESPONSE_STATUS_TYPE_CREATED,
            message_enum.RESPONSE_STATUS_TYPE_FIRST,
            message_enum.RESPONSE_STATUS_TYPE_DELTA,
            message_enum.RESPONSE_STATUS_TYPE_COMPLETION,
        ]:
            logger.debug("[%s] %s", conversation_type, content)

        return {
            "conversation_id": context.conversation_id,
            "created": int(time.time()),
            "seq": seq,
            "role": message_enum.Role_Type_System,
            "type": conversation_type,
            "object": "realtime.response",
            "response": [
                {
                    "id": 0,
                    "source": payload.response_source,
                    "type": payload.response_type,
                    "name": payload.response_name,
                    "output": [
                        {
                            "type": "text",
                            "content": content,
                        }
                    ],
                }
            ],
        }

    def build_cancel(self, conversation_id: str):
        return {
            "conversation_id": conversation_id,
            "type": "cancelled",
            "object": "realtime.cancelled",
            "created": int(time.time()),
            "message": "Stream cancelled by user",
        }

    def build_error(self, conversation_id: str, code: int, message: str, err_type: str):
        return {
            "conversation_id": conversation_id,
            "type": "error",
            "object": "realtime.error",
            "created": int(time.time()),
            "error": [
                {
                    "code": code,
                    "message": message,
                    "type": err_type,
                }
            ],
        }

    def build_done(self, conversation_id: str):
        return {
            "conversation_id": conversation_id,
            "created": int(time.time()),
            "seq": -1,
            "role": message_enum.Role_Type_System,
            "type": message_enum.RESPONSE_STATUS_TYPE_DONE,
            "object": "realtime.response",
            "response": [
                {
                    "id": 0,
                    "source": message_enum.Role_Type_System_LLM,
                    "type": message_enum.ConversationResponseInnerContentTypeMap["done"],
                    "name": "",
                    "output": [
                        {
                            "type": "text",
                            "content": "",
                        }
                    ],
                }
            ],
        }
