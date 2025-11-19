from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg_pool import AsyncConnectionPool
from contextlib import asynccontextmanager

from app.settings import settings

# 可混合模式， 提供两种使用方式
class PGPoolHybrid:
    """混合模式：支持单例和上下文管理器两种模式"""

    def __init__(self):
        self._pool: AsyncConnectionPool | None = None
        self._saver: AsyncPostgresSaver | None = None
        self._context_count = 0  # 上下文使用计数

    async def get_pool(self) -> AsyncConnectionPool:
        """获取连接池（单例模式）"""
        if self._pool is None:
            self._pool = AsyncConnectionPool(
                conninfo=settings.POSTGRESQL_URI,
                min_size=2,
                max_size=20,
                kwargs={"autocommit": True, "prepare_threshold": 0},
                open=False
            )
            await self._pool.open()
        return self._pool

    async def get_saver(self) -> AsyncPostgresSaver:
        """获取 saver（单例模式）"""
        if self._saver is None:
            pool = await self.get_pool()
            self._saver = AsyncPostgresSaver(pool)
            await self._saver.setup()
        return self._saver

    @asynccontextmanager
    async def context(self):
        """上下文管理器模式"""
        self._context_count += 1
        try:
            # pool = await self.get_pool()
            saver = await self.get_saver()
            yield saver
        finally:
            self._context_count -= 1
            # 如果没有上下文在使用，可以考虑关闭连接池
            # if self._context_count == 0:
            #     await self.close()

    async def close(self):
        """关闭连接池"""
        if self._saver:
            self._saver = None

        if self._pool:
            await self._pool.close()
            self._pool = None

# 全局实例
pg_hybrid = PGPoolHybrid()

# 向后兼容的函数
async def get_pg_pool() -> AsyncConnectionPool:
    """向后兼容：获取连接池"""
    return await pg_hybrid.get_pool()

async def get_saver() -> AsyncPostgresSaver:
    """向后兼容：获取 saver"""
    return await pg_hybrid.get_saver()