import signal
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from app.core.llm.runtime_manager import LLMRuntimeManager
from app.core.tools.runtime_manager import ToolRuntimeManager
from app.settings import settings

SHOULD_EXIT = False


def setup_graceful_shutdown(loop):
    def _signal_handler(signum, frame):
        global should_exit
        print("收到 SIGTERM，开始优雅下线流程...")
        should_exit = True

    signal.signal(signal.SIGTERM, _signal_handler)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 在这里注册信号处理器
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    setup_graceful_shutdown(loop)

    # 初始化全局Mongo连接池
    # mongo_client = AsyncIOMotorClient(settings.MONGODB_URI)
    # app.state.mongo_client = mongo_client
    #
    # # 创建索引
    # db = mongo_client[settings.MONGODB_DB_NAME]
    # await db.user_projects.create_index("user_id")
    # await db.user_projects.create_index("project_id")

    # try:
    #     # 初始化 SupervisorManager
    #     await supervisor_manager._ensure_redis_connection()
    #     print("start")
    #
    # except Exception as e:
    #     raise

    llm_manager = LLMRuntimeManager(
        default_model=settings.DEEPSEEK_MODEL or settings.DEFAULT_LLM_MODEL or "deepseek:deepseek-reasoner"
    )
    app.state.llm_manager = llm_manager

    tool_servers = {}
    if settings.AMAP_MCP_URI:
        tool_servers["amap-amap-sse"] = {
            "url": settings.AMAP_MCP_URI,
            "transport": "sse",
        }
    tool_manager = ToolRuntimeManager(
        tool_servers,
        refresh_ttl=settings.TOOL_REFRESH_TTL,
    )
    app.state.tool_manager = tool_manager

    # ✅ 初始化 AgentPool（可选：预热默认 Agent）
    try:
        # 预热默认 Agent（可选，减少首次请求延迟）
        await llm_manager.preload()
        if tool_servers:
            await tool_manager.preload()
        print("AgentPool initialized and warmed up")
    except Exception as e:
        print(f"Warning: Failed to warm up AgentPool: {e}")

    try:
        yield
    finally:
        print("生命周期结束")

        # ✅ 清理 AgentPool
        try:
            await llm_manager.close()
            await tool_manager.close()
            print("AgentPool cleaned up")
        except Exception as e:
            print(f"Error cleaning up AgentPool: {e}")

        # 关闭 supervisor 管理器（如果已初始化）
        # try:
        #
        # except Exception as e:
        #     print(f"Error shutting down supervisor_manager: {e}")

        # 关闭数据库连接
        # mongo_client.close()
