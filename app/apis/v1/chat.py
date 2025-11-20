import json
import uuid
import time
import asyncio
import logging
from typing import AsyncGenerator

from fastapi import Depends
from fastapi import Request
from langchain_mcp_adapters.client import MultiServerMCPClient

from app.schemas.chat import ChatV1Request
from fastapi.responses import StreamingResponse
from app.core.middleware.auth import auth_dependency
from app.core.stream_controller import create_cancellable_stream
from app.services.response import ResponseCode, get_error_message
from app.settings import settings
from app.core.agent_factory.orchestrator import Orchestrator
from . import router
from ...core.pg_pool_context_manager import pg_hybrid


@router.post("/chat/stream")
async def stream_chat_v1(request: Request, param: ChatV1Request,
        user_id: str = Depends(auth_dependency), dependencies=[]):
    """处理流式聊天请求"""
    # 新建项目
    if param.type == "create" or param.conversation_id is None:
        conversation_id = str(uuid.uuid4())
        param.conversation_id = conversation_id
        # 保存user_id和thread_id到pg
        # mongo_client = request.app.state.mongo_client
        # db = mongo_client[settings.MONGODB_DB_NAME]
        # collection = db["user_conversion"]
        # await collection.insert_one({
        #     "conversion_id": conversion_id,
        #     "user_id": user_id,
        #     "first_message": param.message,
        #     "state": "create",
        #     "created_at": int(time.time()),
        #     "updated_at": int(time.time())
        # })
    print(param.conversation_id)
    # 创建配置，包含必要的字段
    config = {
        "configurable": {
            "thread_id": param.conversation_id,
            "user_id": user_id,
        }
    }

    # 创建响应流
    async def response_stream() -> AsyncGenerator[str, None]:
        try:
            async with pg_hybrid.context() as checkpointer:
                # checkpointer = await pg_hybrid.get_saver()
                agent = await create_chat_agent(checkpointer)
                original_stream = agent.stream_process(param, config=config)

                # 包装为可取消的流（5分钟超时）
                async for chunk in create_cancellable_stream(
                        conversation_id=param.conversation_id,
                        original_stream=original_stream,
                        check_interval=0.5,  # 每0.5秒检查一次取消状态
                        timeout_seconds=600,  # 10分钟超时
                        heartbeat_interval=5,  # 每5秒一次心跳
                        enable_heartbeat=True
                ):
                    yield chunk

        except asyncio.TimeoutError:
            logger = logging.getLogger(__name__)
            logger.error(f"Request timeout for conversation {param.conversation_id}")
            timeout_response = {
                "conversation_id": param.conversation_id,
                "type": "error",
                "object": "realtime.error",
                "created": int(time.time()),
                "error": [{
                    "code": ResponseCode.TIMEOUT_ERROR,
                    "message": get_error_message(ResponseCode.TIMEOUT_ERROR),
                    "type": "timeout_error"
                }]
            }
            yield f"data: {json.dumps(timeout_response, ensure_ascii=False)}\n\n"

        except ConnectionError as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Connection error for conversation {param.conversation_id}: {e}")
            connection_error_response = {
                "conversation_id": param.conversation_id,
                "type": "error",
                "object": "realtime.error",
                "created": int(time.time()),
                "error": [{
                    "code": ResponseCode.CONNECTION_ERROR,
                    "message": get_error_message(ResponseCode.CONNECTION_ERROR),
                    "type": "connection_error"
                }]
            }
            yield f"data: {json.dumps(connection_error_response, ensure_ascii=False)}\n\n"

        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Unexpected error in stream_chat_v1 for conversation {param.conversation_id}: {e}", exc_info=True)

            error_response = {
                "conversation_id": param.conversation_id,
                "type": "error",
                "object": "realtime.error",
                "created": int(time.time()),
                "error": [{
                    "code": ResponseCode.SYSTEM_ERROR,
                    "message": get_error_message(ResponseCode.SYSTEM_ERROR),
                    "type": "system_err",
                    "details": str(e) if settings.DEBUG else None  # 仅在DEBUG模式下显示详细错误信息
                }]
            }
            yield f"data: {json.dumps(error_response, ensure_ascii=False)}\n\n"

        # finally:
            # 清理项目资源（如果需要）
            # await supervisor_manager.cleanup_project(param.conversion_id)

    return StreamingResponse(
        response_stream(),
        media_type="text/event-stream"
    )

async def create_chat_agent(checkpointer):
    client = MultiServerMCPClient({
        # 高德地图MCP Server
        "amap-amap-sse": {
            "url": settings.AMAP_MCP_URI,
            "transport": "sse",
        }
    })

    # 从MCP Server中获取可提供使用的全部工具
    tools = await client.get_tools()

    system_message = "你是一个AI助手，使用高德地图工具集合获取信息，以及给出方案。"
    orchtor = Orchestrator("指挥官", "调度整个会话与流程")
    orchtor.register_tool(tools)


    await orchtor.create_graph(checkpointer, system_message)

    return orchtor