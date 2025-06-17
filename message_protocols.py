from pydantic import BaseModel
from typing import List
from autogen_core.models import LLMMessage


class UserStartSession(BaseModel):
    pass


class UserEndSession(BaseModel):
    pass



class UserTask(BaseModel):
    context: List[LLMMessage]
    
    

class AgentResponse(BaseModel):
    reply_to_topic_type: str
    reply_from_topic_type: str
    context: List[LLMMessage]
    


class ReplyTask(BaseModel):
    work_order_id: str
    context: List[LLMMessage]
    

class FollowUpPayload(BaseModel):
    user_session_id: str
    work_order_id: str
    user_id: str
    delay_seconds: int
    message_to_deliver: str


class FollowUpMessage(BaseModel):
    payload: FollowUpPayload


