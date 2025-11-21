# 底层流与控制层解耦方案

## 1. 背景与问题

- 目前 `stream_chat_v1` 直接把 `agent.stream_process(...)` 的输出（LangGraph 返回的 `Dict`）传递给 `create_cancellable_stream`，控制层同时承担了事件序列化、SSE 协议包装、心跳/超时/取消等职责。
- 控制层与底层业务耦合过深：它依赖具体的消息 schema、不了解客户端断连，仅能通过超时结束，导致无法在其他传输协议（WebSocket、消息队列）中复用。
- 缺乏统一的事件模型，使得上层难以扩展多个流来源（例如工具流、审计流）或进行测试注入。

## 2. 设计目标

1. **分离关注点**：底层 LangGraph/Agent 仅生成领域事件（Domain Event），控制层专注于跨领域能力（心跳、超时、取消），传输层负责协议适配（SSE/WebSocket）。
2. **可组合性**：控制层通过接口注入不同的 `StreamProvider`、`Transport`、`CancellationSource`，实现最小化耦合。
3. **易监控**：在控制层统一收集指标（message lag、heartbeat drop、cancel latency 等），便于后续监控。
4. **渐进式迁移**：允许现有 SSE 端点无缝切换，同时为后续多路流整合提供基础。

## 3. 架构概览

```
┌──────────────────┐    DomainEvent     ┌───────────────────────┐    TransportEvent    ┌────────────────────┐
│ StreamProvider(s) │ ────────────────▶ │   StreamController    │ ───────────────────▶ │ StreamTransport(s) │
└──────────────────┘   (业务无关结构)   │  (取消/超时/心跳/队列) │   serialize/send      └────────────────────┘
         ▲                                 ▲        ▲        ▲             │
         │                                 │        │        │             ▼
         │                      CancellationSource   │   MetricsSink   Client (SSE/WebSocket/Queue)
         │                                          RetryPolicy
```

核心抽象：

- **StreamEvent**（DomainEvent）：在 `app/core/streaming/events.py` 定义 dataclass，包含 `conversation_id`、`event_type`（created/delta/done/error/heartbeat/custom）、`payload`、`source`、`meta` 等字段。
- **StreamProvider**：暴露 `__aiter__` 和 `cancel()`，作为底层流的统一接口。LangGraph 端通过 `LangGraphStreamProvider` 实现。
- **StreamController**：接收 `AsyncIterator[StreamEvent]`，实现心跳、超时、取消、回压、错误处理等，与传输细节无关。
- **StreamTransport**：提供 `send(event: StreamEvent) -> str | bytes`，用于编码输出。`SSETransport` 负责 `data: ...\n\n` 的序列化，未来可新增 `WebSocketTransport`。
- **CancellationSource**：封装取消检测（如 Redis、用户断连、手动取消 API），控制器通过注入接口查询即可。

## 4. 组件设计细节

### 4.1 StreamEvent

文件：`app/core/streaming/events.py`

```python
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

class EventType(str, Enum):
    CREATED = "created"
    DELTA = "delta"
    COMPLETION = "completion"
    DONE = "done"
    ERROR = "error"
    HEARTBEAT = "heartbeat"
    CANCELLED = "cancelled"

@dataclass
class StreamEvent:
    conversation_id: str
    event_type: EventType
    payload: Dict[str, Any] = field(default_factory=dict)
    source: str = "system"
    trace_id: Optional[str] = None
    seq: Optional[int] = None
```

`ResponseBuilder` 改为返回 `StreamEvent`，`StreamController` 根据 event_type 决定是否触发完成/终止逻辑。

### 4.2 StreamProvider 接口

文件：`app/core/streaming/providers.py`

```python
from typing import AsyncIterator, Protocol

class StreamProvider(Protocol):
    conversation_id: str

    def __aiter__(self) -> AsyncIterator[StreamEvent]:
        ...

    async def cancel(self) -> None:
        ...
```

实现：`LangGraphStreamProvider` 包装 `Orchestrator.graph.astream`，在 `__aiter__` 中逐条 yield `StreamEvent`。必要时支持多路复合 Provider（例如工具流与模型流合并）。

### 4.3 StreamTransport 接口

文件：`app/core/streaming/transport.py`

```python
class StreamTransport(Protocol):
    async def encode(self, event: StreamEvent) -> str | bytes:
        ...
```

`SSETransport` 的行为：

1. 将 `StreamEvent` 转换成与当前前端兼容的 JSON。
2. 保留心跳/取消事件类型，统一 `data: ...\n\n`。
3. 允许注入自定义字段（比如 trace_id、source）。

### 4.4 StreamController 改造

职责保持不变，但输入与输出都以接口形式注入：

- 构造函数参数：
  - `provider: StreamProvider`
  - `transport: StreamTransport`
  - `cancel_source: CancellationSource`
  - `timeout_policy: TimeoutPolicy`
  - `heartbeat_policy: HeartbeatPolicy`
  - `metrics_sink: MetricsSink`
- `run()`：驱动 provider 迭代、执行心跳循环、监听取消/断连，将 encode 后的结果 yield 给 FastAPI `StreamingResponse`。
- 将 `StreamController` 移到 `app/core/streaming/controller.py`，原有 `asyncio.Queue` 逻辑可以保留，但 message 类型改成 `StreamEvent`，最终由 `transport.encode(event)` 负责变成字符串。
- 错误场景：当 provider 抛出异常时，controller 会：
  1. 发送 `EventType.ERROR`。
  2. 停止心跳任务，结束队列。

### 4.5 CancellationSource 与断连检测

定义协议 `CancellationSource`：

```python
class CancellationSource(Protocol):
    async def should_cancel(self, conversation_id: str) -> bool: ...
```

实现：

1. `RedisCancellationSource`：查询 Redis 中的取消标记。
2. `RequestDisconnectSource`：封装 FastAPI `Request` 的 `is_disconnected()`。
3. `CompositeCancellationSource`：多策略组合，任一触发即取消。

`StreamController` 每个 `check_interval` 调用 `should_cancel`，若 True 则发送 `EventType.CANCELLED`，并调用 `await provider.cancel()`。

### 4.6 传输层扩展

- **SSETransport**（默认）：兼容现有消息格式。
- **WebSocketTransport**：将事件包装成 JSON frame，通过 FastAPI WebSocket 发送。
- **QueueTransport**：将事件推入 Kafka/Redis Stream，供异步消费。

所有 transport 共享同一个 `StreamEvent` 定义，从而真正实现“一套业务，多种传输”。

## 5. 集成步骤

1. **新增基础模块**：
   - `app/core/streaming/events.py`
   - `app/core/streaming/providers.py`
   - `app/core/streaming/transport.py`
   - `app/core/streaming/controller.py`（重构自现有 `stream_controller`）
2. **调整 Orchestrator**：
   - `ResponseBuilder` 输出 `StreamEvent`。
   - `create_chat_agent` 返回 `StreamProvider`，由 API 负责传给 controller。
3. **FastAPI 接入**：
   ```python
   provider = LangGraphStreamProvider(orchestrator, param, config)
   transport = SSETransport()
   cancel_source = CompositeCancellationSource([
       RequestDisconnectSource(request),
       RedisCancellationSource(redis_client)
   ])
   controller = StreamController(
       provider=provider,
       transport=transport,
       cancel_source=cancel_source,
       timeout_policy=TimeoutPolicy(total_seconds=600),
   )
   return StreamingResponse(controller.run(), media_type="text/event-stream")
   ```
4. **迁移旧逻辑**：
   - 原心跳/超时/队列/metrics 代码迁移到新 controller。
   - 保持原有 SSE 消息结构，前端无需改动。
5. **测试与回滚**：
   - 编写 `fake_provider`、`fake_transport` 单元测试 controller。
   - 在 Staging 环境验证取消、超时、断连、错误路径。
   - 若出现问题，可通过配置开关切回旧实现。

## 6. 验证与测试建议

1. **单元测试**：为 `StreamController`、`SSETransport`、`LangGraphStreamProvider` 新增覆盖，使用 `pytest` + `asyncio`。
2. **集成测试**：模拟 FastAPI `StreamingResponse`，验证断连时 provider 被取消、done/error 不会重复发送。
3. **性能测试**：压测多个并发流，观察队列回压与心跳丢失指标。
4. **监控**：在 controller 中记录开始时间、结束时间、heartbeats、重试次数，输出到日志或 metrics。

---

通过以上设计，底层 LangGraph 流可以与上层控制器、传输协议彻底解耦，主流程具备更好的复用性和可测试性，同时为未来支持多种输出通道奠定基础。***
