import asyncio
import uuid
import time
from typing import Dict, AsyncGenerator, Any

from langchain.agents import create_agent
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage
from langchain.agents.middleware import HumanInTheLoopMiddleware

from app.core.msg_manage import message_enum
from app.core.msg_manage.ds import parse_ds_message_chunk_v2
from app.schemas.chat import ChatV1Request
from app.services.response import ResponseCode, get_error_message


class Orchestrator:
    def __init__(self, name, desc):
        self.name = name
        self.desc = desc
        self.graph = None
        self.tools = []
        self.llm = init_chat_model(
                        model="deepseek:deepseek-reasoner",
                        temperature=0,
                        base_url="https://api.deepseek.com/v1",
                        api_key="sk-759ff171d9144ca6861f1c89a9f3976b"
                    )

    async def create_graph(self, checkpointer, system_prompt: str = None):
        # 确保tools是扁平化的列表
        flat_tools = []
        for tool in self.tools:
            if isinstance(tool, list):
                flat_tools.extend(tool)
            else:
                flat_tools.append(tool)

        self.graph = create_agent(
            model=self.llm,
            tools=flat_tools if flat_tools else None,
            system_prompt=system_prompt,
            checkpointer=checkpointer,
        )

    def register_tool(self, tool):
        if isinstance(tool, list):
            self.tools.extend(tool)
        else:
            self.tools.append(tool)


    async def stream_process(self, request: ChatV1Request, config: Dict = None) -> AsyncGenerator[Dict, None]:
        seq = 0
        try:
            async for agent_info, message_chunk in self.graph.astream(
                    input={"messages": [HumanMessage(content=request.message)]},
                    config=config,
                    stream_mode="messages",
                    subgraphs=True,
            ):
                # todo: 检查取消状态，管理取消机制

                if not message_chunk or len(message_chunk) == 0:
                    continue

                message = message_chunk[0]
                msg_type = message.__class__.__name__
                response_source = message_enum.Role_Type_System_LLM
                response_name = str(uuid.uuid4())
                if agent_info and len(agent_info) > 0:
                    response_name = agent_info[0]

                if len(agent_info) > 0:
                    agent_sources = response_name.split(":")
                    if len(agent_sources) > 1:
                        response_source = agent_sources[0]

                conversation_type, content_type, content = parse_ds_message_chunk_v2(message, response_source)

                # 检查是否为尾包或完成状态
                is_completion = conversation_type in [
                    message_enum.RESPONSE_STATUS_TYPE_COMPLETION,
                    message_enum.RESPONSE_STATUS_TYPE_DONE
                ]

                # 对于尾包，保持与前一个消息相同的序列号
                if is_completion and seq > 0:
                    current_seq = seq  # 保持当前序列号
                elif conversation_type != message_enum.RESPONSE_STATUS_TYPE_CREATED:
                    seq += 1
                    current_seq = seq
                else:
                    current_seq = seq

                # 首包特殊处理
                if seq == 1 and conversation_type != message_enum.RESPONSE_STATUS_TYPE_CREATED:
                    conversation_type = message_enum.RESPONSE_STATUS_TYPE_FIRST

                # 确保尾包有正确的内容
                if is_completion and not content:
                    content = ""  # 尾包可以是空内容

                if content or conversation_type in [
                    message_enum.RESPONSE_STATUS_TYPE_CREATED,
                    message_enum.RESPONSE_STATUS_TYPE_FIRST,
                    message_enum.RESPONSE_STATUS_TYPE_DELTA,
                    message_enum.RESPONSE_STATUS_TYPE_COMPLETION
                ]:
                    print(f"[{conversation_type}] {content}", flush=True)

                yield self._build_response(request.conversation_id, seq, conversation_type, response_source,
                                            content_type, response_name, content, "text")
        except asyncio.CancelledError:
            yield self._build_cancel_response(request.conversation_id)
            return
        except Exception as e:
            yield self._build_error_resp(request.conversation_id, ResponseCode.SYSTEM_ERROR.value,
                                          get_error_message(ResponseCode.SYSTEM_ERROR), "")
        finally:

            yield self._build_response(request.conversation_id, -1, message_enum.RESPONSE_STATUS_TYPE_DONE,
                                        message_enum.Role_Type_System_LLM,
                                        message_enum.ConversationResponseInnerContentTypeMap["done"], "", "", "text")



    def _build_response(
            self,
            conversation_id: str,
            seq: int,
            conversation_type: str,
            response_source: str,
            response_type: str,
            response_name: str,
            content: str = "",
            content_type: str = "",
    ) -> Dict[str, Any]:
        return {
            "conversation_id": conversation_id,
            "created": int(time.time()),
            "seq": seq,
            "role": message_enum.Role_Type_System,  # 可根据需要替换为统一的 message_enum.Role_Type_System
            "type": conversation_type,
            "object": "realtime.response",
            "response": [{
                "id": 0,
                "source": response_source,
                "type": response_type,
                "name": response_name,
                "output": [
                    {
                        "type": content_type,
                        "content": content
                    }
                ],
            }]
        }

    def _build_cancel_response(self, conversation_id):
        return {
            "conversation_id": conversation_id,
            "type": "cancelled",
            "object": "realtime.cancelled",
            "created": int(time.time()),
            "message": "Stream cancelled by user"
        }

    def _build_error_resp(self, conversation_id, code, message, err_type):
        return {
            "conversation_id": conversation_id,
            "type": "error",
            "object": "realtime.error",
            "created": int(time.time()),
            "error": [{
                "code": code,
                "message": message,
                "type": err_type
            }]
        }

