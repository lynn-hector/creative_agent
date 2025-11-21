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
from app.core.streaming.events import StreamEvent
from app.schemas.chat import ChatV1Request
from app.services.response import ResponseCode, get_error_message
from app.settings import settings

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, name, desc):
        """
        Args:
            name: 协调器名称，用于日志或可视化展示。
            desc: 协调器描述，帮助外部理解该 orchestrator 的职责。
        """
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
        """
        Args:
            checkpointer: LangGraph 的检查点存储，用于线程状态恢复。
            system_prompt: 可选的系统提示词，影响 LLM 行为。
            tools: 额外工具列表；若提供则覆盖现有注册的工具。
        """
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

    async def stream_process(
        self, request: ChatV1Request, config: Dict = None
    ) -> AsyncGenerator[StreamEvent, None]:
        """驱动 LangGraph astream 并转换为标准化的 StreamEvent。"""
        context = StreamContext(request.conversation_id)
        should_emit_done = True
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
            logger.info("Stream for %s cancelled by controller", request.conversation_id)
            should_emit_done = False
            return
        except Exception:
            logger.exception("Orchestrator stream error for %s", request.conversation_id)
            yield self._response_builder.build_error(
                request.conversation_id,
                ResponseCode.SYSTEM_ERROR.value,
                get_error_message(ResponseCode.SYSTEM_ERROR),
                "",
            )
            should_emit_done = False
        finally:
            if should_emit_done:
                yield self._response_builder.build_done(request.conversation_id)
