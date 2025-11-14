import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.core.lifecycle import lifespan
from app.apis import v1
from app.settings import settings
from app.core.middleware.timing import timing_middleware
from app.core.middleware.auth import AuthError

app = FastAPI(title="creative-mutil-agent", description="基于LangGraph提供创意AI Agent服务", lifespan=lifespan)

# 注册 AuthError 的全局异常处理器
@app.exception_handler(AuthError)
async def auth_error_handler(request: Request, exc: AuthError):
    return JSONResponse(
        status_code=200,  # 你可以改成401等
        content={
            "code": exc.code,
            "message": exc.message,
            "data": exc.data
        }
    )

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(v1.router, prefix="/api/v1")

app.middleware("http")(timing_middleware)


@app.get("/")
async def root():
    return {"message": "Welcome to creative-ai Mutil-Agent API"}


@app.get("/health")
async def health():
    return {"message": "Creative-Mutil-Agent Is Healthy"}


if __name__ == '__main__':
    uvicorn.run("app.main:app", host='0.0.0.0', port=settings.PORT, reload=True)