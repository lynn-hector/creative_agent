from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from langchain_mcp_adapters.client import MultiServerMCPClient


@dataclass
class ToolFilter:
    """简单的工具过滤条件。"""

    include: Optional[Sequence[str]] = None
    exclude: Optional[Sequence[str]] = None
    capabilities: Optional[Sequence[str]] = None


@dataclass
class ToolSnapshot:
    """缓存的工具快照。"""

    tools: List[Any] = field(default_factory=list)
    updated_at: float = field(default_factory=time.time)


class ToolRuntimeManager:
    """第一阶段：缓存 MCP 工具并提供基础过滤。"""

    def __init__(
        self,
        servers: Dict[str, Dict[str, Any]],
        *,
        refresh_ttl: int = 300,
    ):
        self._servers = servers
        self._refresh_ttl = refresh_ttl
        self._snapshot: Optional[ToolSnapshot] = None
        self._lock = asyncio.Lock()

    async def get_tools(self, tool_filter: Optional[ToolFilter] = None) -> List[Any]:
        """获取工具集合，必要时刷新缓存。"""
        await self._ensure_snapshot()
        assert self._snapshot is not None
        tools = list(self._snapshot.tools)
        return self._apply_filter(tools, tool_filter)

    async def refresh(self) -> None:
        """主动拉取最新工具列表。"""
        async with self._lock:
            if not self._servers:
                self._snapshot = ToolSnapshot(tools=[], updated_at=time.time())
                return
            client = MultiServerMCPClient(self._servers)
            tools = await client.get_tools()
            self._snapshot = ToolSnapshot(tools=tools, updated_at=time.time())

    async def preload(self) -> None:
        """应用启动阶段预加载工具。"""
        await self.refresh()

    async def close(self) -> None:
        """当前实现无长连接，仅保留接口供后续扩展。"""
        self._snapshot = None

    async def _ensure_snapshot(self) -> None:
        async with self._lock:
            now = time.time()
            if (
                self._snapshot is None
                or (now - self._snapshot.updated_at) > self._refresh_ttl
            ):
                if not self._servers:
                    self._snapshot = ToolSnapshot(tools=[], updated_at=now)
                else:
                    client = MultiServerMCPClient(self._servers)
                    tools = await client.get_tools()
                    self._snapshot = ToolSnapshot(tools=tools, updated_at=now)

    def _apply_filter(
        self, tools: List[Any], tool_filter: Optional[ToolFilter]
    ) -> List[Any]:
        if not tool_filter:
            return tools

        filtered = tools

        if tool_filter.include:
            include_set = set(tool_filter.include)
            filtered = [
                tool
                for tool in filtered
                if getattr(tool, "name", None) in include_set
            ]

        if tool_filter.exclude:
            exclude_set = set(tool_filter.exclude)
            filtered = [
                tool
                for tool in filtered
                if getattr(tool, "name", None) not in exclude_set
            ]

        if tool_filter.capabilities:
            need = set(tool_filter.capabilities)

            def has_capability(tool: Any) -> bool:
                metadata = getattr(tool, "metadata", None)
                if isinstance(metadata, dict):
                    caps = metadata.get("capabilities")
                    if caps:
                        return bool(set(caps) & need)
                return False

            filtered = [tool for tool in filtered if has_capability(tool)]

        return filtered
