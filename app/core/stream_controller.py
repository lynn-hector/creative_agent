"""负责取消、超时、心跳等通用能力的高层流控制器。"""

import asyncio
import logging
import time
from typing import AsyncGenerator, Optional

from app.core.streaming.cancellation import (
    CancellationSource,
    NullCancellationSource,
)
from app.core.streaming.events import EventType, StreamEvent
from app.core.streaming.provider import StreamProvider
from app.core.streaming.transport import SSETransport, StreamTransport

logger = logging.getLogger(__name__)


class StreamController:
    """控制可取消流的生命周期，负责心跳、超时、取消与编码。"""

    def __init__(
        self,
        provider: StreamProvider,
        transport: StreamTransport | None = None,
        cancel_source: Optional[CancellationSource] = None,
        *,
        check_interval: float = 0.5,
        timeout_seconds: Optional[int] = 300,
        heartbeat_interval: float = 30.0,
        enable_heartbeat: bool = True,
        queue_size: int = 100,
    ):
        """
        Args:
            provider: 上游 StreamProvider，持续产出 StreamEvent。
            transport: 负责编码事件的传输实现，默认 SSE。
            cancel_source: 可选取消信号源（断连、外部指令等）。
            check_interval: 监控循环检测取消/超时的间隔秒数。
            timeout_seconds: 总体超时时长；None 表示不启用。
            heartbeat_interval: 发送心跳的间隔秒数。
            enable_heartbeat: 是否启用心跳任务。
            queue_size: 控制内部队列大小，防止积压。
        """
        self.provider = provider
        self.transport = transport or SSETransport()
        self.cancel_source = cancel_source or NullCancellationSource()

        self.check_interval = check_interval
        self.timeout_seconds = timeout_seconds
        self.heartbeat_interval = heartbeat_interval
        self.enable_heartbeat = enable_heartbeat
        self.queue_size = queue_size

        self._start_time = time.time()
        self._last_data_time = self._start_time
        self._last_heartbeat = self._start_time
        self._stream_ended = False
        self._queue: asyncio.Queue[str | bytes | None] | None = None

        self._heartbeat_drop_count = 0
        self._data_retry_count = 0

    async def run(self) -> AsyncGenerator[str | bytes, None]:
        """
        消费 provider 并输出编码后的字符串（默认 SSE）。

        控制器会持续拉取直到完成、超时或被取消，并把经过传输层编码的内容
        回传给 StreamingResponse（或其他消费者）。
        """
        self._queue = asyncio.Queue(maxsize=self.queue_size)
        tasks = [
            asyncio.create_task(self._drain_provider(), name="stream-provider"),
            asyncio.create_task(self._monitor_stream(), name="stream-monitor"),
        ]

        if self.enable_heartbeat:
            tasks.append(
                asyncio.create_task(self._emit_heartbeat(), name="stream-heartbeat")
            )

        end_signals = 0
        expected_end_signals = 2  # provider + monitor

        try:
            while True:
                try:
                    message = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    if self._stream_ended:
                        break
                    continue

                if message is None:
                    end_signals += 1
                    if end_signals >= expected_end_signals:
                        break
                    continue
                yield message
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            if self._heartbeat_drop_count or self._data_retry_count:
                logger.info(
                    "Stream %s completed. Heartbeat drops=%s, data retries=%s",
                    self.provider.conversation_id,
                    self._heartbeat_drop_count,
                    self._data_retry_count,
                )

    async def _drain_provider(self) -> None:
        """消费上游事件并写入内部队列。"""
        assert self._queue is not None
        try:
            async for event in self.provider:
                self._last_data_time = time.time()
                await self._deliver_event(event)

                if event.event_type in (
                    EventType.RESPONSE_DONE,
                    EventType.ERROR,
                ):
                    self._stream_ended = True
                    break
        except asyncio.CancelledError:
            await self.provider.cancel()
            self._stream_ended = True
            raise
        except Exception as exc:
            logger.exception(
                "Error while draining stream for %s",
                self.provider.conversation_id,
            )
            error_event = StreamEvent(
                conversation_id=self.provider.conversation_id,
                event_type=EventType.ERROR,
                payload={
                    "conversation_id": self.provider.conversation_id,
                    "type": EventType.ERROR.value,
                    "object": "realtime.error",
                    "created": int(time.time()),
                    "error": [
                        {
                            "code": 500000,
                            "message": str(exc),
                            "type": "stream_error",
                        }
                    ],
                },
            )
            await self._deliver_event(error_event)
            self._stream_ended = True
        finally:
            await self._queue.put(None)

    async def _monitor_stream(self) -> None:
        """定时检查是否超时或触发了外部取消。"""
        assert self._queue is not None
        try:
            while not self._stream_ended:
                await asyncio.sleep(self.check_interval)

                if await self.cancel_source.should_cancel(
                    self.provider.conversation_id
                ):
                    logger.info(
                        "Stream %s cancelled by cancellation source",
                        self.provider.conversation_id,
                    )
                    await self._deliver_event(self._build_cancel_event(origin="controller"))
                    self._stream_ended = True
                    break

                if self._is_timeout():
                    logger.info(
                        "Stream %s reaches timeout",
                        self.provider.conversation_id,
                    )
                    await self.provider.cancel()
                    await self._deliver_event(self._build_timeout_event())
                    self._stream_ended = True
                    break
        finally:
            await self._queue.put(None)

    async def _emit_heartbeat(self) -> None:
        """按配置间隔生成心跳，保持连接活跃。"""
        assert self._queue is not None
        try:
            while not self._stream_ended:
                await asyncio.sleep(self.heartbeat_interval)
                event = self._build_heartbeat_event()
                try:
                    await asyncio.wait_for(self._deliver_event(event), timeout=1.0)
                    self._last_heartbeat = time.time()
                except asyncio.TimeoutError:
                    self._heartbeat_drop_count += 1
        except asyncio.CancelledError:
            logger.debug(
                "Heartbeat task cancelled for %s", self.provider.conversation_id
            )
            raise

    async def _deliver_event(self, event: StreamEvent) -> None:
        """调用 transport 编码事件并投递到队列。"""
        assert self._queue is not None
        encoded = await self.transport.encode(event)
        await self._queue.put(encoded)

    def _build_heartbeat_event(self) -> StreamEvent:
        """构造心跳事件，包含运行时指标。"""
        current_time = time.time()
        payload = {
            "conversation_id": self.provider.conversation_id,
            "type": EventType.HEARTBEAT.value,
            "object": "realtime.heartbeat",
            "created": int(current_time),
            "heartbeat": {
                "timestamp": current_time,
                "uptime": current_time - self._start_time,
                "last_data": current_time - self._last_data_time,
                "status": "active",
            },
        }
        return StreamEvent(
            conversation_id=self.provider.conversation_id,
            event_type=EventType.HEARTBEAT,
            payload=payload,
        )

    def _build_timeout_event(self) -> StreamEvent:
        """构造超时事件。"""
        payload = {
            "conversation_id": self.provider.conversation_id,
            "type": EventType.TIMEOUT.value,
            "object": "realtime.timeout",
            "created": int(time.time()),
        }
        return StreamEvent(
            conversation_id=self.provider.conversation_id,
            event_type=EventType.TIMEOUT,
            payload=payload,
        )

    def _build_cancel_event(self, origin: str = "controller") -> StreamEvent:
        """构造取消事件，允许标记触发源。"""
        payload = {
            "conversation_id": self.provider.conversation_id,
            "type": EventType.CANCELLED.value,
            "object": "realtime.cancelled",
            "created": int(time.time()),
            "origin": origin,
        }
        return StreamEvent(
            conversation_id=self.provider.conversation_id,
            event_type=EventType.CANCELLED,
            payload=payload,
        )

    def _is_timeout(self) -> bool:
        if not self.timeout_seconds:
            return False
        return (time.time() - self._start_time) >= self.timeout_seconds


async def create_cancellable_stream(
    provider: StreamProvider,
    *,
    transport: StreamTransport | None = None,
    cancel_source: Optional[CancellationSource] = None,
    check_interval: float = 0.5,
    timeout_seconds: Optional[int] = 300,
    heartbeat_interval: float = 30.0,
    enable_heartbeat: bool = True,
) -> AsyncGenerator[str | bytes, None]:
    """便捷封装，供简单场景快速创建流控制器。"""
    controller = StreamController(
        provider=provider,
        transport=transport,
        cancel_source=cancel_source,
        check_interval=check_interval,
        timeout_seconds=timeout_seconds,
        heartbeat_interval=heartbeat_interval,
        enable_heartbeat=enable_heartbeat,
    )
    async for chunk in controller.run():
        yield chunk
