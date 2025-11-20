import asyncio
import logging
from typing import AsyncGenerator, Dict

from langchain.agents import create_agent
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage

from app.core.agent_factory.streaming import (
    LangGraphMessageAdapter,
    ResponseBuilder,
    StreamContext,
)
from app.schemas.chat import ChatV1Request
from app.services.response import ResponseCode, get_error_message
from app.settings import settings

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, name, desc):
        self.name = name
        self.desc = desc
        self.graph = None
        self.tools = []

        api_key = settings.DEEPSEEK_API_KEY
        if not api_key:
            raise ValueError("DEEPSEEK_API_KEY is not configured in environment variables")

        base_url = settings.DEEPSEEK_API_BASE or "https://api.deepseek.com/v1"
        model_name = settings.DEEPSEEK_MODEL or "deepseek:deepseek-reasoner"

        self.llm = init_chat_model(
            model=model_name,
            temperature=0,
            base_url=base_url,
            api_key=api_key,
        )

        self._message_adapter = LangGraphMessageAdapter()
        self._response_builder = ResponseBuilder()

    def reset_tools(self, tools):
        self.tools = []
        if tools:
            self.register_tool(tools)

    async def create_graph(self, checkpointer, system_prompt: str = None, tools=None):
        if tools is not None:
            self.reset_tools(tools)

        flat_tools = self._flatten_tools(self.tools)

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

    def _flatten_tools(self, tools):
        if not tools:
            return []
        flat_tools = []
        for tool in tools:
            if isinstance(tool, list):
                flat_tools.extend(tool)
            else:
                flat_tools.append(tool)
        return flat_tools

    async def stream_process(self, request: ChatV1Request, config: Dict = None) -> AsyncGenerator[Dict, None]:
        context = StreamContext(request.conversation_id)
        try:
            async for agent_info, message_chunk in self.graph.astream(
                input={"messages": [HumanMessage(content=request.message)]},
                config=config,
                stream_mode="messages",
                subgraphs=True,
            ):
                payload = self._message_adapter.adapt(agent_info, message_chunk)
                if not payload:
                    continue
                yield self._response_builder.build_message(context, payload)
        except asyncio.CancelledError:
            yield self._response_builder.build_cancel(request.conversation_id)
            return
        except Exception:
            logger.exception("Orchestrator stream error for %s", request.conversation_id)
            yield self._response_builder.build_error(
                request.conversation_id,
                ResponseCode.SYSTEM_ERROR.value,
                get_error_message(ResponseCode.SYSTEM_ERROR),
                "",
            )
        finally:
            yield self._response_builder.build_done(request.conversation_id)
