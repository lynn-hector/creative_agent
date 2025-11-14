import asyncio
import json
import time
from typing import AsyncGenerator, Dict, Any


from app.core.msg_manage import message_enum
from app.services.response import ResponseCode
import logging

logger = logging.getLogger(__name__)


class StreamController:
    """流控制器，支持取消、超时和心跳功能"""
    
    def __init__(
        self,
        conversation_id: str,
        check_interval: float = 0.5,
        timeout_seconds: int = 300,
        heartbeat_interval: float = 30.0,
        enable_heartbeat: bool = True
    ):
        self.conversation_id = conversation_id
        self.check_interval = check_interval
        self.timeout_seconds = timeout_seconds
        self.heartbeat_interval = heartbeat_interval
        self.enable_heartbeat = enable_heartbeat
        
        self.start_time = time.time()
        self.last_heartbeat = time.time()
        self.last_data_time = time.time()
        self.is_cancelled = False
        self.heartbeat_task = None
        self.stream_ended = False
        self.heartbeat_drop_count = 0
        self.data_retry_count = 0
        
    async def create_heartbeat_message(self) -> Dict[str, Any]:
        """创建心跳消息"""
        current_time = time.time()
        return {
            "conversation_id": self.conversation_id,
            "type": "heartbeat",
            "role": "system",
            "object": "realtime.heartbeat",
            "created": int(current_time),
            "heartbeat": {
                "timestamp": current_time,
                "uptime": current_time - self.start_time,
                "last_data": current_time - self.last_data_time,
                "status": "active"
            }
        }
    
    async def create_timeout_message(self) -> Dict[str, Any]:
        """创建超时消息"""
        return {
            "conversation_id": self.conversation_id,
            "type": "timeout",
            "role": "system",
            "object": "realtime.timeout",
            "created": int(time.time()),
        }
    
    async def create_cancel_message(self) -> Dict[str, Any]:
        """创建取消消息"""
        return {
            "conversation_id": self.conversation_id,
            "type": "cancelled",
            "role": "human",
            "object": "realtime.cancelled",
            "created": int(time.time()),
        }
    
    async def should_send_heartbeat(self) -> bool:
        """检查是否应该发送心跳"""
        if not self.enable_heartbeat:
            return False
        
        current_time = time.time()
        return (current_time - self.last_heartbeat) >= self.heartbeat_interval
    
    async def is_timeout(self) -> bool:
        """检查是否超时"""
        current_time = time.time()
        return (current_time - self.start_time) >= self.timeout_seconds
    
    async def is_stream_cancelled(self) -> bool:
        """检查流是否被取消"""
        return
        # return await supervisor_manager.is_stream_cancelled(self.conversation_id)
    
    def is_response_done(self, chunk: Dict[str, Any]) -> bool:
        """统一检测response.done消息"""
        return chunk.get("type") == message_enum.RESPONSE_STATUS_TYPE_DONE
    
    def is_critical_message(self, chunk: Dict[str, Any]) -> bool:
        """检测是否为关键消息类型（response.done 或 error）"""
        return chunk.get("type") in [message_enum.RESPONSE_STATUS_TYPE_DONE, message_enum.ERROR_STATUS]
    
    async def format_data_chunk(self, chunk: Dict[str, Any]) -> str:
        """格式化数据块为 SSE 格式"""
        return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
    
    async def format_heartbeat_chunk(self) -> str:
        """格式化心跳消息为 SSE 格式"""
        heartbeat_msg = await self.create_heartbeat_message()
        return f"data: {json.dumps(heartbeat_msg, ensure_ascii=False)}\n\n"


async def create_cancellable_stream(
    conversation_id: str,
    original_stream: AsyncGenerator[Dict[str, Any], None],
    check_interval: float = 0.5,
    timeout_seconds: int = 300,
    heartbeat_interval: float = 30.0,
    enable_heartbeat: bool = True
) -> AsyncGenerator[str, None]:
    """
    创建可取消的流，支持心跳消息
    
    Args:
        conversation_id: 会话ID
        original_stream: 原始数据流
        check_interval: 检查间隔（秒）
        timeout_seconds: 超时时间（秒）
        heartbeat_interval: 心跳间隔（秒）
        enable_heartbeat: 是否启用心跳
    
    Yields:
        str: SSE 格式的数据
    """
    controller = StreamController(
        conversation_id=conversation_id,
        check_interval=check_interval,
        timeout_seconds=timeout_seconds,
        heartbeat_interval=heartbeat_interval,
        enable_heartbeat=enable_heartbeat
    )
    
    logger.info(f"Starting cancellable stream for conversation {conversation_id}")
    
    try:
        # 使用有界队列来收集所有消息，防止内存泄漏
        message_queue = asyncio.Queue(maxsize=100)
        
        # 创建任务
        tasks = []
        
        # 原始流任务
        stream_task = asyncio.create_task(
            _process_original_stream(original_stream, controller, message_queue)
        )
        tasks.append(stream_task)
        
        # 心跳任务
        if enable_heartbeat:
            heartbeat_task = asyncio.create_task(
                _process_heartbeat(controller, message_queue)
            )
            tasks.append(heartbeat_task)
        
        # 监控任务
        monitor_task = asyncio.create_task(
            _process_monitor(controller, message_queue)
        )
        tasks.append(monitor_task)
        
        # 消费队列中的消息
        try:
            end_signal_count = 0
            expected_end_signals = 2  # 原始流任务 + 监控任务
            timeout_count = 0
            # 动态计算最大超时次数，基于总超时时间
            max_timeouts = max(60, timeout_seconds // 10)  # 至少60次，或者总超时时间的1/10
            
            while True:
                try:
                    # 等待消息或任务完成
                    message = await asyncio.wait_for(message_queue.get(), timeout=1.0)
                    timeout_count = 0  # 重置超时计数
                    
                    if message is None:  # 结束信号
                        end_signal_count += 1
                        if end_signal_count >= expected_end_signals:
                            break
                        continue

                    yield message
                    
                    # 简单检查结束标志
                    if controller.stream_ended:
                        logger.info(f"Stream ended gracefully for conversation {conversation_id}")
                        break
                    
                except asyncio.TimeoutError:
                    timeout_count += 1
                    
                    # 检查是否所有关键任务都已完成
                    if stream_task.done() and monitor_task.done():
                        break
                    
                    # 检查是否已经超过流的总超时时间
                    if await controller.is_timeout():
                        logger.info(f"Stream timeout reached for conversation {conversation_id}, exiting gracefully")
                        break
                    
                    # 防止无限等待，但给予更多时间
                    if timeout_count >= max_timeouts:
                        logger.warning(f"Max queue timeouts ({max_timeouts}) reached for conversation {conversation_id}, forcing exit")
                        break
                    
                    continue
                    
        finally:
            # 清理任务
            for task in tasks:
                if not task.done():
                    task.cancel()
            
            # 等待任务完成
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # 记录统计信息
            if controller.heartbeat_drop_count > 0:
                logger.info(f"Stream completed for conversation {conversation_id}. Heartbeat drops: {controller.heartbeat_drop_count}, Data retries: {controller.data_retry_count}")
            else:
                logger.info(f"Stream completed for conversation {conversation_id}. No message delivery issues detected.")
            
    except Exception as e:
        logger.error(f"Error in cancellable stream for project {conversation_id}: {e}")
        error_msg = {
            "conversation_id": conversation_id,
            "type": "error",
            "object": "realtime.error",
            "created": int(time.time()),
            "error": {
                "code": ResponseCode.SYSTEM_ERROR.value,
                "message": str(e)
            }
        }
        yield f"data: {json.dumps(error_msg, ensure_ascii=False)}\n\n"


async def _process_original_stream(
    original_stream: AsyncGenerator[Dict[str, Any], None],
    controller: StreamController,
    message_queue: asyncio.Queue
) -> None:
    """改进的原始流处理，减少消息丢失"""
    chunk_count = 0
    max_retries = 3
    
    try:
        logger.info(f"Starting to process original stream for conversation {controller.conversation_id}")
        async for chunk in original_stream:
            chunk_count += 1
            logger.debug(f"Received chunk #{chunk_count} for conversation {controller.conversation_id}: {chunk}")
            controller.last_data_time = time.time()
            formatted_chunk = await controller.format_data_chunk(chunk)
            
            # 智能重试机制
            delivered = False
            current_retries = 0
            
            while not delivered and current_retries < max_retries:
                try:
                    # 根据队列状态调整超时时间
                    queue_size = message_queue.qsize()
                    timeout = max(1.0, 5.0 - queue_size * 0.1)  # 队列越满，超时越短
                    
                    await asyncio.wait_for(message_queue.put(formatted_chunk), timeout=timeout)
                    delivered = True
                    logger.debug(f"Successfully queued chunk #{chunk_count} for conversation {controller.conversation_id}")
                    
                except asyncio.TimeoutError:
                    current_retries += 1
                    controller.data_retry_count += 1
                    
                    if current_retries < max_retries:
                        # 短暂等待后重试
                        await asyncio.sleep(0.1 * current_retries)
                        logger.debug(f"Retrying message delivery for conversation {controller.conversation_id}, attempt {current_retries}")
                    else:
                        # 达到最大重试次数，记录错误但继续处理
                        logger.error(f"Failed to deliver message after {max_retries} retries for conversation {controller.conversation_id}, dropping chunk #{chunk_count}")
                        
                        # 如果是关键消息类型，记录严重错误
                        if controller.is_critical_message(chunk):
                            logger.critical(f"Critical message dropped for conversation {controller.conversation_id}: {chunk.get('type')}")
            
            # 检查是否收到 response.done 消息，如果是则设置结束标志
            if controller.is_response_done(chunk):
                logger.info(f"Received response.done for conversation {controller.conversation_id}, ending stream processing")
                controller.stream_ended = True
                break
                
            # 流控：如果重试次数过多，暂停一下
            if controller.data_retry_count > 20:
                logger.warning(f"High retry count detected for conversation {controller.conversation_id}, applying flow control")
                await asyncio.sleep(0.2)
                controller.data_retry_count = 0
                
    except Exception as e:
        logger.error(f"Error processing original stream: {e}")
        # 错误消息必须投递
        await _deliver_critical_message(controller, message_queue, {
            "conversation_id": controller.conversation_id,
            "type": "error",
            "object": "realtime.error",
            "created": int(time.time()),
            "error": {
                "code": ResponseCode.SYSTEM_ERROR.value,
                "message": str(e)
            }
        })
    finally:
        logger.info(f"Original stream processing completed for conversation {controller.conversation_id}, processed {chunk_count} chunks, total retries: {controller.data_retry_count}")
        # 结束信号必须投递
        await _deliver_end_signal(message_queue, controller.conversation_id)


async def _deliver_critical_message(controller: StreamController, queue: asyncio.Queue, message_dict: Dict[str, Any]) -> None:
    """保证关键消息投递"""
    formatted_message = await controller.format_data_chunk(message_dict)
    retries = 0
    max_retries = 10
    
    while retries < max_retries:
        try:
            await asyncio.wait_for(queue.put(formatted_message), timeout=1.0)
            logger.debug(f"Critical message delivered for conversation {controller.conversation_id} after {retries} retries")
            return
        except asyncio.TimeoutError:
            retries += 1
            await asyncio.sleep(0.1)
    
    logger.critical(f"Failed to deliver critical message for conversation {controller.conversation_id} after {max_retries} retries")


async def _deliver_end_signal(queue: asyncio.Queue, project_id: str, max_retries: int = 5) -> None:
    """保证结束信号投递"""
    for i in range(max_retries):
        try:
            await asyncio.wait_for(queue.put(None), timeout=1.0)
            logger.debug(f"End signal delivered for project {project_id}")
            return
        except asyncio.TimeoutError:
            if i < max_retries - 1:
                await asyncio.sleep(0.2)
            else:
                logger.critical(f"Failed to deliver end signal for project {project_id} after {max_retries} retries")


async def _process_heartbeat(
    controller: StreamController,
    message_queue: asyncio.Queue
) -> None:
    """改进的心跳处理，减少消息丢失"""
    try:
        next_heartbeat_time = time.time() + controller.heartbeat_interval
        consecutive_failures = 0
        
        while True:
            current_time = time.time()
            
            # 计算精确的等待时间，避免累积误差
            wait_time = max(0.1, next_heartbeat_time - current_time)
            await asyncio.sleep(wait_time)
            
            # 发送心跳
            controller.last_heartbeat = time.time()
            heartbeat_chunk = await controller.format_heartbeat_chunk()
            
            # 尝试多种策略投递心跳
            delivered = False
            
            # 策略1: 非阻塞投递
            try:
                message_queue.put_nowait(heartbeat_chunk)
                delivered = True
                consecutive_failures = 0
            except asyncio.QueueFull:
                # 策略2: 短时间等待投递
                try:
                    await asyncio.wait_for(message_queue.put(heartbeat_chunk), timeout=0.1)
                    delivered = True
                    consecutive_failures = 0
                except asyncio.TimeoutError:
                    consecutive_failures += 1
                    
            # 策略3: 如果连续失败，记录但继续运行
            if not delivered:
                controller.heartbeat_drop_count += 1
                if consecutive_failures >= 3:
                    logger.warning(f"Heartbeat delivery issues for conversation {controller.conversation_id}, consecutive failures: {consecutive_failures}, total dropped: {controller.heartbeat_drop_count}")
                    consecutive_failures = 0  # 重置计数器
            
            # 计算下次心跳时间
            next_heartbeat_time = controller.last_heartbeat + controller.heartbeat_interval
                
    except asyncio.CancelledError:
        logger.info(f"Heartbeat task cancelled for conversation {controller.conversation_id}")
    except Exception as e:
        logger.error(f"Error in heartbeat task: {e}")


async def _process_monitor(
    controller: StreamController,
    message_queue: asyncio.Queue
) -> None:
    """处理监控消息并放入队列"""
    try:
        while True:
            await asyncio.sleep(controller.check_interval)
            
            # 检查是否被取消
            if await controller.is_stream_cancelled():
                logger.info(f"Stream cancelled for conversation {controller.conversation_id}")
                cancel_msg = await controller.create_cancel_message()
                formatted_cancel = await controller.format_data_chunk(cancel_msg)
                
                try:
                    await asyncio.wait_for(message_queue.put(formatted_cancel), timeout=1.0)
                except asyncio.TimeoutError:
                    logger.warning(f"Failed to send cancel message for conversation {controller.conversation_id}")
                break
            
            # 检查是否超时
            if await controller.is_timeout():
                logger.info(f"Stream timeout for conversation {controller.conversation_id}")
                timeout_msg = await controller.create_timeout_message()
                formatted_timeout = await controller.format_data_chunk(timeout_msg)
                
                try:
                    await asyncio.wait_for(message_queue.put(formatted_timeout), timeout=1.0)
                except asyncio.TimeoutError:
                    logger.warning(f"Failed to send timeout message for conversation {controller.conversation_id}")
                break
                
    except asyncio.CancelledError:
        logger.info(f"Monitor task cancelled for conversation {controller.conversation_id}")
    except Exception as e:
        logger.error(f"Error in monitor task: {e}")
    finally:
        # 确保发送结束信号，使用改进的投递方法
        await _deliver_end_signal(message_queue, controller.conversation_id)





async def create_heartbeat_only_stream(
    conversation_id: str,
    heartbeat_interval: float = 30.0,
    max_duration: int = 300
) -> AsyncGenerator[str, None]:
    """
    创建仅心跳的流（用于测试或保持连接）
    
    Args:
        conversation_id: 会话ID
        heartbeat_interval: 心跳间隔（秒）
        max_duration: 最大持续时间（秒）
    
    Yields:
        str: SSE 格式的心跳消息
    """
    controller = StreamController(
        conversation_id=conversation_id,
        heartbeat_interval=heartbeat_interval,
        timeout_seconds=max_duration,
        enable_heartbeat=True
    )
    
    logger.info(f"Starting heartbeat-only stream for project {conversation_id}")
    
    try:
        while True:
            await asyncio.sleep(controller.check_interval)
            
            # 检查是否被取消
            if await controller.is_stream_cancelled():
                cancel_msg = await controller.create_cancel_message()
                yield await controller.format_data_chunk(cancel_msg)
                break
            
            # 检查是否超时
            if await controller.is_timeout():
                timeout_msg = await controller.create_timeout_message()
                yield await controller.format_data_chunk(timeout_msg)
                break
            
            # 发送心跳
            if await controller.should_send_heartbeat():
                controller.last_heartbeat = time.time()
                yield await controller.format_heartbeat_chunk()
                
    except Exception as e:
        logger.error(f"Error in heartbeat-only stream: {e}")
        error_msg = {
            "conversation_id": conversation_id,
            "type": "error",
            "object": "realtime.error",
            "created": int(time.time()),
            "error": {
                "code": ResponseCode.SYSTEM_ERROR.value,
                "message": str(e)
            }
        }
        yield f"data: {json.dumps(error_msg, ensure_ascii=False)}\n\n"