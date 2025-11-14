from langchain_core.messages.utils import count_tokens_approximately, trim_messages


def pre_model_hook_by_max_token(state):
    trimmed_messages = trim_messages(
        messages=state["messages"],
        max_tokens=20,
        strategy="last",
        # 使用 len 计数消息数量
        token_counter=len,
        start_on="human",
        include_system=True,
        allow_partial=False,
    )
    return {"llm_input_messages": trimmed_messages}
