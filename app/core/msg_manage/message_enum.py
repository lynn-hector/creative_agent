FINISH_RESPONSE_CONTENT = "done"

# 外层返回结果类型
RESPONSE_STATUS_TYPE_CREATED = "response.created" # 始包
RESPONSE_STATUS_TYPE_FIRST = "response.first" # 首包
RESPONSE_STATUS_TYPE_DELTA = "response.delta" # 间包
RESPONSE_STATUS_TYPE_COMPLETION = "response.completion" # 尾包
RESPONSE_STATUS_TYPE_DONE = "response.done" # 终包
ERROR_STATUS = "error"
REQUEST_STATUS = "request"
HEARTBEAT_STATUS = "heartbeat"

# 内部的返回消息类型
INNER_CONTENT_TYPE_START = "start" # 开始内容
INNER_CONTENT_TYPE_THINKING= "thinking" # 思考内容
INNER_CONTENT_TYPE_CONTENT = "content" # 正常内容
INNER_CONTENT_TYPE_COMPLETION = "completion" # 尾包
INNER_CONTENT_TYPE_DONE = "done" # 完成内容

Role_Type_User = "user"
Role_Type_System = "system"
Role_Type_System_LLM = "system.llm"
Role_Type_System_Tool = "system.tool"
Role_Type_Key_Human_Message = "HumanMessage"
Role_Type_Key_AI_Message = "AIMessage"
Role_Type_Key_Tool_Message = "ToolMessage"



ConversationRoleMap = {
    Role_Type_Key_Human_Message: Role_Type_User,
    Role_Type_Key_AI_Message: Role_Type_System_LLM,
    Role_Type_Key_Tool_Message: Role_Type_System_Tool,
}


ConversationResponseStatusTypeMap = {
    "start": RESPONSE_STATUS_TYPE_CREATED,
    "thinking": RESPONSE_STATUS_TYPE_DELTA,
    "content": RESPONSE_STATUS_TYPE_DELTA,
    "finish_reason": RESPONSE_STATUS_TYPE_COMPLETION,
    "done": RESPONSE_STATUS_TYPE_DONE,

    "error": ERROR_STATUS,
    "request": REQUEST_STATUS,
}

ConversationResponseInnerContentTypeMap = {
    "start": INNER_CONTENT_TYPE_START,
    "thinking": INNER_CONTENT_TYPE_THINKING,
    "content": INNER_CONTENT_TYPE_CONTENT,
    "finish_reason": INNER_CONTENT_TYPE_COMPLETION,
    "done": INNER_CONTENT_TYPE_DONE,
}
