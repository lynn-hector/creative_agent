import uuid
from fastapi import Request

async def trace_id_middleware(request: Request, call_next):
    # 生成唯一trace_id
    trace_id = str(uuid.uuid4())
    # 注入到request.state
    request.state.trace_id = trace_id
    response = await call_next(request)
    # 你也可以把trace_id加到响应头
    response.headers["X-Trace-Id"] = trace_id
    return response