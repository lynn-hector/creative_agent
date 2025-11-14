from app.core.msg_manage import message_enum


def parse_ds_message_chunk_v2(message_chunk, response_source: str):
    print(message_chunk)
    if message_chunk.content:
        return message_enum.ConversationResponseStatusTypeMap["content"], \
            message_enum.ConversationResponseInnerContentTypeMap["content"], message_chunk.content
    additional_kwargs = getattr(message_chunk, 'additional_kwargs', {}) or {}
    if isinstance(additional_kwargs, dict):
        for key, value in additional_kwargs.items():
            if key == "reasoning_content":
                return message_enum.ConversationResponseStatusTypeMap["thinking"], \
                    message_enum.ConversationResponseInnerContentTypeMap["thinking"], value
            else:
                print(key, value)
    response_metadata = getattr(message_chunk, 'response_metadata', {}) or {}
    if isinstance(response_metadata, dict):
        for key, value in response_metadata.items():
            if key == "finish_reason" and value == "stop" :
                return message_enum.ConversationResponseStatusTypeMap["finish_reason"], \
                    message_enum.ConversationResponseInnerContentTypeMap["finish_reason"], "",
    # 如果没有找到，返回两个 None，避免解包错误
    return message_enum.ConversationResponseStatusTypeMap["start"], \
        message_enum.ConversationResponseInnerContentTypeMap["start"], ""