from fastapi import Request

class AuthError(Exception):
    def __init__(self, code, message, data=None):
        self.code = code
        self.message = message
        self.data = data

async def auth_dependency(request: Request):
    # from app.service.auth.jwt import get_user_id_from_header
    # from app.service.error_code import ErrorCode, get_error_message

    # auth_resp = get_user_id_from_header(request.headers)
    # if auth_resp["error"] or not auth_resp.get("id"):
    #     raise AuthError(
    #         code=ErrorCode.USER_NOT_LOGIN_ERROR.value,
    #         message=get_error_message(ErrorCode.USER_NOT_LOGIN_ERROR),
    #         data=None
    #     )
    # return auth_resp["id"]
    return 10086