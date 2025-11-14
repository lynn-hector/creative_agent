import os
from dotenv import load_dotenv
from typing import Optional
from pydantic import Field

load_dotenv()


class Settings:
    PROJECT_NAME: str = "chat-ai Agent"
    PROJECT_VERSION: str = "1.0.0"
    PORT: int = int(os.getenv("PORT", 9536))
    DEBUG: bool = os.getenv("DEBUG", True)
    MODE: str = os.getenv("MODE", "dev")
    WORKS: int = int(os.getenv("WORKS", 1))

    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE_PATH: str = os.getenv("LOG_FILE_PATH", "./logs/agent.log")

    JWT_TOKEN_AUTH_SECRET: str = os.getenv("JWT_TOKEN_AUTH_SECRET", "")
    JWT_TOKEN_EXPIRE_TIME: int = int(os.getenv("JWT_TOKEN_EXPIRE_TIME", 864000))
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")


    # llm配置
    LLM_MAX_TOKENS: int = int(os.getenv("LLM_MAX_TOKENS", 4096))
    LLM_MAX_CONTEXT_LENGTH: int = int(os.getenv("LLM_MAX_TOKENS", 2048))
    DEFAULT_LLM_PROVIDER: str = os.getenv("DEFAULT_LLM_PROVIDER", "deepseek")
    DEFAULT_LLM_MODEL: str = os.getenv("DEFAULT_LLM_MODEL", "v2")

    # Deepseek配置
    DEEPSEEK_API_KEY: str = os.getenv("DEEPSEEK_API_KEY", "")
    DEEPSEEK_API_BASE: str = os.getenv("DEEPSEEK_API_BASE", "")
    DEEPSEEK_MODEL: str = os.getenv("DEEPSEEK_MODEL", "")

    POSTGRESQL_URI: str = os.getenv("POSTGRESQL_URI", "")

    LANGSMITH_TRACING: bool = bool(os.getenv("LANGSMITH_TRACING", True))
    LANGSMITH_API_KEY: str = os.getenv("LANGSMITH_API_KEY", "")

    # Redis 配置
    REDIS_HOST: str = os.getenv("REDIS_HOST", "127.0.0.1")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB: int = int(os.getenv("REDIS_DB", 4))
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "admin")

    AMAP_MCP_URI: str = os.getenv("AMAP_MCP_URI", "")


settings = Settings()