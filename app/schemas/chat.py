from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field

class ChatV1Request(BaseModel):
    """聊天请求"""
    conversation_id: Optional[str] = Field(None, description="会话ID")
    # create-新建, chat-聊天, cancel-取消返回，需带上conversation_id
    type: str = Field(..., description="类型")
    message: str = Field(..., description="用户输入的消息内容")
    model: Optional[int] = Field(None, description="模型")


class ChatV1StreamContentResponse:
    type: str = Field(..., description="消息类型")
    content: str = Field(..., description="消息id")


class ChatV1StreamMessageResponse:
    """流式返回详细信息"""
    id: str = Field(..., description="消息id")
    seq: int = Field(..., description="消息序号")
    name: str = Field(..., description="角色名称")
    status: str = Field(..., description="状态")
    role: str = Field(..., description="角色")
    output: List[dict] = Field(..., description="内容")


class ChatV1StreamErrorResponse:
    """流式返回详细信息"""
    code: int = Field(..., description="角色名称")
    message: str = Field(..., description="角色")
    param: Dict[str, Any] = Field(..., description="内容")



class ChatV1StreamResponse(BaseModel):
    """聊天请求返回"""
    conversion_id: str = Field(..., description="会话id")
    created: int = Field(..., description="创建时间戳")
    seq: int = Field(..., description="消息序号")
    type: str = Field(..., description="消息类型")
    object: str = Field(..., description="消息对象")
    extra: Dict[str, Dict] = Field(None, description="二维额外信息")
    response: Optional[dict] = Field(None, description="消息")
    error: Optional[dict] = Field(None, description="错误信息")



class ChatV1HistoryListRequest(BaseModel):
    page: int = Field(1, description="页码")
    page_size: int = Field(10, description="每页条数")


class ChatV1HistoryDetailRequest(BaseModel):
    conversion_id: str = Field(..., description="会话id")



class ChatV1ReportRequest(BaseModel):
    """取消聊天请求的数据模型"""
    conversion_id: str = Field(..., description="会话id")
    type: str = Field(..., description="类型") # cancel取消

