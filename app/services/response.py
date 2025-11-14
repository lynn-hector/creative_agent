from enum import Enum


class ResponseCode(Enum):
    SUCCESS = 0  # 成功
    SYSTEM_ERROR = 500000  # 系统错误
    PARAM_ERROR = 400  # 参数错误
    UNAUTHORIZED_ERROR = 401
    USER_NOT_LOGIN_ERROR = 10202 # 未登录

Response_MESSAGES = {
    ResponseCode.SUCCESS: "成功",
    ResponseCode.PARAM_ERROR: "参数错误",
    ResponseCode.SYSTEM_ERROR: "系统错误",
    ResponseCode.UNAUTHORIZED_ERROR: "用户未注册",
    ResponseCode.USER_NOT_LOGIN_ERROR: "用户未登录",
}

def get_error_message(code: ResponseCode) -> str:
    """
    根据错误码获取对应的中文错误信息。
    """
    return Response_MESSAGES.get(code, "未知错误")