# 轻量内核 + 按需装配方案

## 目标

1. **最小化上下文**：Agent 仅携带基础系统提示，按需注入工具/技能描述，避免长期积累导致 token 消耗爆炸。
2. **动态组合能力**：每轮对话根据任务意图装配必要的工具链或技能图谱，支持模块化扩展。
3. **保持状态一致**：尽管 Agent 不跨轮复用整机，但保留必要的会话记忆（checkpoint/向量）以维持语义连贯。

## 总体架构

```
Request
  └─▶ ConversationContext (历史摘要 + 关键事实)
        └─▶ Intent/Skill Planner
              ├─▶ Tool Registry
              └─▶ Skill Graph (可选)
                    └─▶ Loader -> Agent Core (LLM prompt)
                            └─▶ LangGraph/LCEL 执行
```

### 1. Agent Core

- `Orchestrator` 内核只负责：
  - 初始化 LLM（DeepSeek）；
  - 提供标准化 `stream_process`。
- System Prompt 仅描述角色/输出格式，不包含具体工具指令。
- 提供 `build_graph(tools, skills, system_prompt)` 方法，每次按需生成流水线。

### 2. Tool & Skill Registry

- 新增 `app/core/skills/registry.py`：
  - 定义 `Skill` / `ToolMetadata` 数据类，包括标签、依赖、成本。
  - 提供 `get_tools_by_tags(tags: list[str])`、`load_tool(tool_id)` 接口。
- MCP 工具仍由 `MultiServerMCPClient` 统一管理，只在启动时建立连接。
  - 实现“懒加载”：仅当 planner 返回需要某个工具时才 `await client.get_tool(tool_id)`。

### 3. Intent / Skill Planner

- 组件位置：`app/services/planner.py`（或 LangChain Router Chain）。
- 基础思路：
  1. 输入：当前用户 message + ConversationContext 摘要。
  2. 输出：`{tools: [...], skills: [...], system_prompt_suffix: "..."}`。
  3. 可以先用简单规则/关键词，后续替换为模型推理。
- Planner 结果可缓存到 `conversation_state`，如果同一会话的下一条消息仍需同样工具，则直接复用。

### 4. Conversation Context

- 保留 `thread_id` 与 Postgres checkpoint，只不过 agent 不再常驻。
- 每轮请求：
  1. 从 checkpoint / Redis 取出“摘要 + 关键事实”；
  2. 构建短上下文传入 planner 与 agent core；
  3. 如需更长记忆，可在 planner 前运行专门的“摘要器”。

### 5. 流程细节

1. **接收请求**
   - 读取 `conversation_id`，从 DB 获取历史摘要（没有则创建）。
2. **规划阶段**
   - 调用 Planner：`plan = planner.decide(message, summary)`.
   - plan 包含 `required_tools`, `skills`, `extra_prompt`.
3. **装配阶段**
   - 调用 Registry 加载 `required_tools`;
   - 构建系统 prompt = base_prompt + plan.extra_prompt + tool说明；
   - `await orchestrator.build_graph(tools, skills, system_prompt)`.
4. **执行/流式返回**
   - 按现有 `StreamController` 流式输出。
5. **更新状态**
   - 将 planner 结果、用到的工具记录到 `conversation_state`，供下一轮参考。

### 6. 缓存策略

- **工具缓存**：`registry` 内部维护 `{tool_id: tool_instance}`，避免重复 load。
- **planner 提示缓存**：对相同 message + summary 组合可利用简单哈希缓存短期结果。
- **技能依赖**：可以把技能实现成 LCEL graph，按需 import。

## 实施步骤

1. **搭建 Registry**
   - 约定工具/技能的 metadata 文件（YAML/JSON），支持分类与标签检索。
   - MCP 工具注册入口统一在 `lifespan` 中初始化 client。
2. **实现 Planner MVP**
   - 先用 rule-based：例如“包含导航关键词 -> 加载高德导航工具”。
   - 逐步升级为小型 LLM Router（LangChain MultiPrompt 或 LlamaIndex Selector）。
3. **改造 Orchestrator**
   - 拆分出 `AgentCore`（含 LLM + build_graph）；
   - `stream_chat_v1` 调用 `core.run(request, plan)` 而非固定 graph。
4. **上下文摘要**
   - 在 checkpoint 基础上增加“摘要/事实”字段，超过长度时自动压缩。
5. **状态记录**
   - 每次 planner 决策写入数据库（工具列表、技能、耗时），便于优化和可观测。
6. **监控与回退**
   - 如果 Planner 结果为空或加载失败，自动回退到“默认工具集合”。
   - 为关键工具提供健康检查，避免请求时动态加载失败。

## 注意事项

- **延迟**：规划与装配引入额外步骤，需控制在 <200ms，必要时并行加载工具和上下文。
- **安全**：不同技能/工具可能需要不同权限，应在 planner 层做白名单。
- **一致性**：Session 级别仍然需要锁或排队，以避免同一 `conversation_id` 同时触发多个装配流程导致冲突。

## 目前落地

- `app/core/skills/registry.py`：实现 `MCPToolRegistry`，缓存工具并提供标签检索、提示生成。
- `app/services/planner.py`：基于规则的 `RuleBasedPlanner`，根据用户输入返回所需标签及 system prompt 补充。
- `app/apis/v1/chat.py`：在请求路径中调用 planner + registry，按需装配工具后再构建 `Orchestrator`。
- `app/core/agent_factory/orchestrator.py`：新增 `reset_tools` 与可传入工具的 `create_graph`，并用事件流水线封装 `stream_process`。
- `app/core/agent_factory/streaming.py`：提供 `StreamContext`、`LangGraphMessageAdapter`、`ResponseBuilder`，将 chunk 解析、序列维护和响应构建解耦。

通过上述步骤，可以在不维护大规模 AgentPool 的前提下，构建“轻量内核 + 动态组合”的多智能体系统，同时保持可扩展性与上下文可控。***
