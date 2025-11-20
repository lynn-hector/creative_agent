# 对话级 Agent 复用方案

## 背景

当前 `/api/v1/chat/stream` 每次请求都会：

1. 新建 `MultiServerMCPClient` 并调用 `get_tools`；
2. 新建 `Orchestrator` 与 DeepSeek LLM；
3. 通过 `create_agent` 构建 LangGraph；
4. 立即丢弃实例，只保留持久化的 checkpoint（`thread_id = conversation_id`）。

这导致相同 `conversation_id` 在多轮对话中缺少 agent 级状态复用，初始化成本高，同时无法做资源级别的取消/监控。

## 目标

- **上下文一致**：同一会话的所有请求共享 agent 实例，从而复用工具调用上下文和本地缓存。
- **性能优化**：减少重复的 MCP 探活、工具注册和 LLM 初始化。
- **易于管理**：可以精确取消、统计、回收 agent；防止同会话在多实例间竞争资源。

## 设计概要

### 1. AgentPool

- 在 `app.core.lifecycle.lifespan` 初始化一个 `AgentPool` 单例（可用 `dict[str, AgentEntry]` 实现）。
- `AgentEntry` 结构：
  - `conversation_id`
  - `agent: Orchestrator`
  - `checkpointer`
  - `last_active_ts`
  - `lock`（可选，避免同会话并发操作）。

### 2. 获取逻辑

`stream_chat_v1` 中：

1. 根据 `conversation_id` 从 `AgentPool` 查找现有 agent。
2. 若存在且未过期，则直接复用：
   - 更新 `last_active_ts`
   - 复用同一个 `checkpointer`（可拆成共享 `AsyncPostgresSaver` + 逻辑层 checkpoint id）。
3. 若不存在，则新建：
   - `client.get_tools()` 只在启动或定期刷新，全局缓存；
   - 构建 `Orchestrator`，完成 `create_graph`；
   - 放入 pool。

### 3. 生命周期管理

- **TTL 驱动清理**：定期（如每 10 分钟）扫描 `last_active_ts`，超过阈值（例如 30 分钟无访问）则关闭并移除。
- **显式完成**：在 `response.done` 时，可根据前端意图立即清理。
- **并发控制**：`conversation_id` 级锁确保同一会话的多个 HTTP 请求按顺序串行，避免状态竞争。

### 4. 取消与监控

- `StreamController.is_stream_cancelled` 可以查询 `AgentPool` 维护的状态（例如 Redis flag 或内存标记）。
- Agent 结构中可存储正在运行的任务句柄，用于真正的流中断。

### 5. 错误恢复

- 如果 agent 复用失败（如 Graph 崩溃），从 pool 中移除并重新创建，确保不会卡死。
- `PGPoolHybrid` 依旧维持数据库级 checkpoint，agent 复用只影响应用内状态。

## 落地步骤

1. **实现 AgentPool**
   - 新增 `app/core/agent_pool.py`
   - 在 `lifespan` 中初始化、在 shutdown 时清理。
2. **改造聊天接口**
   - `create_chat_agent` 改为 `get_or_create_agent(conversation_id)`
   - 接入 `AgentPool` 与 TTL。
3. **改造工具加载**
   - MCP `client.get_tools()` 结果缓存 + 定期刷新机制。
4. **取消/监控集成**
   - `StreamController.is_stream_cancelled` 查询 pool 或外部存储。
5. **测试**
   - 同一 `conversation_id` 多次请求响应时间下降。
   - 并行不同会话互不影响。
   - 长时间无请求自动清理。

该方案兼顾上下文一致性与性能，在无需大规模重构的前提下即可提升体验。后续可进一步将 AgentPool 与 redis/pubsub 结合，实现跨实例的会话粘性。***
