# Tool Runtime Manager 设计方案

## 1. 背景与目标

当前工具管理流程较为分散：每个请求在 `create_chat_agent` 中通过 `MultiServerMCPClient.get_tools()` 拉取工具，再直接注册到 `Orchestrator`。存在以下问题：

- **重复握手**：所有请求都要与 MCP Server 通信，增加延迟与资源消耗。
- **缺乏版本/状态管理**：无法区分工具的版本、可用性，也无法在工具异常时快速下线。
- **无上下文差异**：不同用户/模型无法按需选择工具组合。
- **难以观测与控制**：缺少调用统计、错误监控、限流等能力。

因此需要一个独立的 Tool Runtime 组件，集中管理工具的生命周期、缓存与策略配置，为 Orchestrator/调用方提供统一接口。

## 2. 核心职责

1. **工具注册与缓存**：从 MCP Server 或其他来源加载工具，维护快照，提供查询/过滤功能。
2. **版本/状态管理**：跟踪每个工具的版本、可用状态、最近更新时间，支持灰度/下线。
3. **按需组合**：根据请求上下文（模型、租户、场景、用户权限等）返回适用的工具集合。
4. **生命周期控制**：在应用启动时预热，在关闭时清理，支持定时刷新或手动更新。
5. **可观测性**：记录工具调用频率、失败率、刷新耗时等指标，便于运维。

## 3. 组件设计

### 3.1 模块结构

```
app/core/tools/
├── registry.py          # ToolRuntimeManager & ToolRegistry
├── providers.py         # MCP provider、静态配置 provider 等
└── models.py            # ToolMetadata / ToolSnapshot
```

### 3.2 数据模型

`ToolMetadata`:
```python
@dataclass
class ToolMetadata:
    name: str
    version: str
    provider: str          # e.g. "mcp:amap"
    capabilities: set[str] # 标签或功能分类
    updated_at: datetime
    extra: dict
```

`ToolSnapshot`:
```python
@dataclass
class ToolSnapshot:
    tools: dict[str, ToolMetadata]         # name -> metadata
    last_refresh: datetime
    etag: Optional[str]
```

### 3.3 ToolRuntimeManager 接口

```python
class ToolRuntimeManager:
    def __init__(self, providers: list[ToolProvider], refresh_interval: int = 300):
        ...

    async def preload(self):
        """在启动阶段拉取一次工具快照。"""

    async def refresh(self):
        """主动刷新工具列表，可由定时任务调用。"""

    def get_tools(self, filter: ToolFilter | None = None) -> list[Any]:
        """根据过滤条件返回工具实例（LangChain 兼容对象）。"""

    def get_metadata(self, name: str) -> ToolMetadata | None:
        ...

    async def close(self):
        """关闭 provider 连接等资源。"""
```

`ToolFilter` 可定义以下字段：
- `capabilities`: 需要具备的能力标签
- `include`: 指定包含的工具名
- `exclude`: 排除的工具名
- `tenant_id`/`user_id`: 用于做权限判断
- `model_id`: 某些模型仅支持特定工具

### 3.4 Provider 抽象

```python
class ToolProvider(Protocol):
    name: str

    async def fetch(self) -> list[ToolMetadata]
    async def close(self) -> None: ...
```

实现示例：
- `MCPToolProvider`: 连接 `MultiServerMCPClient`，支持 SSE/WebSocket，利用 ETag/last-modified 减少全量拉取。
- `StaticToolProvider`: 从配置文件加载静态工具。
- 未来可扩展 `DBToolProvider`/`RESTToolProvider` 等。

### 3.5 与 Orchestrator 集成

1. 在 `lifespan` 中创建 `ToolRuntimeManager`，与 LLM manager 类似：
   ```python
   tool_manager = ToolRuntimeManager([mcp_provider])
   await tool_manager.preload()
   app.state.tool_manager = tool_manager
   ```

2. 在 `stream_chat_v1` 中：
   ```python
   tool_filter = ToolFilter(capabilities=param.capabilities, tenant_id=user_id)
   tools = tool_manager.get_tools(tool_filter)
   orchestrator = Orchestrator(..., llm, tools=tools)
   ```

3. Orchestrator 的 `create_graph` 接受工具列表，但不需要关心工具来源。

### 3.6 刷新策略

- **定时刷新**：在 ToolRuntimeManager 内部启动后台任务，每 `refresh_interval` 秒触发 `fetch`。
- **事件驱动刷新**：支持 REST API/管理后台调用 `tool_manager.refresh()`，实现手动更新。
- **增量更新**：provider 返回 `etag` 或 `updated_at`，manager 只替换变化的工具。

### 3.7 可观测性

- 记录刷新耗时、成功/失败次数。
- 暴露工具调用统计：Orchestrator 每次调用工具时上报到 manager（或直接发送 metrics）。
- 在工具异常时更新 metadata 中的 `status` 字段，避免继续下发有问题的工具。

## 4. 实施步骤

1. 创建 `app/core/tools/` 模块，实现上文结构。
2. 在 `lifespan` 注入 `ToolRuntimeManager`，与 LLM manager 并行启动/关闭。
3. 修改 `create_chat_agent`，从 manager 获取工具列表并注册。
4. 增加配置项：
   - MCP provider 列表、刷新间隔、默认能力映射等。
5. 编写测试：
   - Provider fetch mock；
   - Tool filtering 行为；
   - 刷新逻辑与缓存。

## 5. 后续扩展

- **权限控制**：结合用户角色决定可用工具。
- **灰度发布**：配合 metadata 标志来逐步放量某个新工具。
- **工具健康监控**：自动记录失败次数，一定阈值后自动下线。
- **工具链组合**：根据场景预定义“工具包”，manager 返回预配置组合。

---

通过该 Tool Runtime 组件，工具生命周期与策略管理将被抽离出来，Orchestrator 只需消费已经筛选好的工具，减少重复调用和复杂度，并可持续扩展能力。***
