from fastapi import Request
from app.services.response import ResponseCode
from . import router


@router.get("/sys/info")
async def system_info_v1(request: Request):
    """
    获取当前系统的信息
    res:
    model: 返回当前的模型
    active_project: 活跃会话数量
    history_thread: 总会话数
    active_user_project: 活跃会话用户去重数量
    history_user_project: 总会话用户去重数量
    """
    return {
        "code": ResponseCode.SUCCESS.value,
        "message": "",
        "data": {
            "model": "deepseek",
            "active_project": 1,
            "history_project": 1,
            "active_user_project": 1,
            "history_user_project": 1,
        }
    }