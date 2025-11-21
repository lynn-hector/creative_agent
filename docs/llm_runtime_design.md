# LLM 工厂/管理器注入 Orchestrator 方案

## 1. 背景

当前 `Orchestrator` 在 `__init__` 中直接读取 `settings` 并调用 `init_chat_model` 创建 DeepSeek LLM：

- 每次实例化都会访问配置、发起握手，难以复用；
- 无法在测试中注入 mock LLM，也不便于多模型/多租户切换；
- 生命周期分散，无法集中释放资源或统一监控。

因此需要把 LLM 初始化逻辑抽离为独立工厂/管理器，由该组件负责创建、复用与清理，`Orchestrator` 仅通过依赖注入获取 LLM 实例。

## 2. 设计目标

1. **集中管理**：单一入口负责根据配置创建 LLM，支持缓存/共享。
2. **灵活注入**：`Orchestrator` 可以接收 LLM 实例或工厂，易于测试与扩展。
3. **多模型支持**: 按模型标识/租户参数获取不同实例，避免硬编码 DeepSeek。
4. **生命周期控制**: 在 FastAPI `lifespan` 中统一初始化/释放，防止资源泄漏。
5. **可观测性**: 管理器负责记录创建时延、失败次数等指标。

## 3. 方案概述

### 3.1 组件结构

```
┌───────────────────────┐
│  LLMRuntimeManager    │
│  - get_llm(model_id)  │◀────── settings / env
│  - preload()          │
│  - close()            │
└─────────▲─────────────┘
          │
          │ ChatOrchestratorFactory / dependency injection
┌─────────┴─────────────┐
│      Orchestrator     │  <-- 接收 llm 实例 (LangChain ChatModel)
└───────────────────────┘
```

### 3.2 关键接口

1. **LLMRuntimeManager**
   ```python
   class LLMRuntimeManager:
       def __init__(self, default_model: str, config_loader: Callable):
           ...

       async def get_llm(self, model_id: str | None = None) -> BaseChatModel:
           """按模型 ID 获取/创建 LLM 实例（带缓存）。"""

       async def preload(self, model_ids: list[str] | None = None):
           """可选：在应用启动时预热常用模型，避免首请求延迟。"""

       async def close(self):
           """释放底层资源，供 lifespan 调用。"""
   ```

   - 使用 `asyncio.Lock` 或 `asyncio.TaskGroup` 确保并发安全；
   - LLM cache 可设为 `{model_id: BaseChatModel}`，需要线程安全；
   - `config_loader(model_id)` 返回 `{"api_key": ..., "base_url": ..., "temperature": ...}`。

2. **Orchestrator 构造函数**

   ```python
   class Orchestrator:
       def __init__(self, name: str, desc: str, llm: BaseChatModel):
           self.llm = llm
           ...
   ```

   - 移除 settings 依赖，仅保留调度逻辑；
   - `create_graph` 时使用注入的 LLM 构建 LangGraph。

3. **工厂函数**

   ```python
   async def create_orchestrator(
       name: str,
       desc: str,
       manager: LLMRuntimeManager,
       model_id: str | None = None,
   ) -> Orchestrator:
       llm = await manager.get_llm(model_id)
       return Orchestrator(name, desc, llm)
   ```

   - API 层只需传入模型标识（或从请求参数解析），剩余工作交给管理器；
   - 方便在路由/依赖中复用。

## 4. 生命周期与依赖注入

1. **应用启动 (`lifespan`)**
   - 创建 `LLMRuntimeManager`，注入默认 model id 和配置加载器；
   - 可调用 `await manager.preload([default_model])` 预热；
   - 挂载到 `app.state.llm_manager`。

2. **请求处理**
   - 在 `stream_chat_v1` 中获取 manager：`manager = request.app.state.llm_manager`;
   - 调用 `create_orchestrator(..., manager, model_id=param.model or None)`；
   - 之后流程与现有逻辑一致。

3. **应用关闭**
   - lifespan `finally` 中调用 `await manager.close()` 释放资源。

## 5. 配置与扩展点

1. **配置加载器**：
   - 从 `settings` 读取全局配置；
   - 支持根据 `model_id` 查询不同的 base_url/API key；
   - 可扩展为读取数据库或配置中心。

2. **多租户/用户级别模型**：
   - `get_llm(model_id, tenant_id=None)`：cache key 包含两者；
   - 配置加载器根据 tenant 返回专属凭证。

3. **容错与降级**：
   - 若创建 LLM 失败，可记录日志并抛出自定义异常；
   - 可尝试回退到默认模型。

4. **Observability**：
   - 记录 `create_llm_duration`, `llm_cache_hit` 等指标；
   - 在 manager 内使用结构化日志打印模型 ID、调用次数。

## 6. 实施步骤

1. 创建 `app/core/llm/runtime_manager.py`，实现 `LLMRuntimeManager`；
2. 调整 `Orchestrator` 构造函数，改为接收 `llm` 参数；
3. 在 `app/apis/v1/chat.py` 中通过 manager 工厂创建 orchestrator；
4. 更新 `lifespan`，注入 manager 并保证关闭；
5. 编写单元测试：
   - manager 缓存逻辑；
   - orchestrator 注入 mock LLM 的行为；
   - 请求流程中切换模型 ID。

---

通过该设计，LLM 的创建与复用可以集中管理，从而减少重复握手、提升测试友好性，并为后续支持多模型策略或动态配置奠定基础。***
