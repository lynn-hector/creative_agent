import time
from fastapi import Request

async def timing_middleware(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    duration = (time.time() - start_time) * 1000  # 毫秒
    print(f"接口 {request.url.path} 耗时: {duration:.2f} ms")
    return response