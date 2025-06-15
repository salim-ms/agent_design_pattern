import json
import uuid
from typing import List, Tuple
import asyncio
from autogen_core import (
    FunctionCall,
    MessageContext,
    RoutedAgent,
    SingleThreadedAgentRuntime,
    TopicId,
    TypeSubscription,
    message_handler,
)
from autogen_core.models import (
    AssistantMessage,
    ChatCompletionClient,
    FunctionExecutionResult,
    FunctionExecutionResultMessage,
    LLMMessage,
    SystemMessage,
    UserMessage,
)
from autogen_core.tools import FunctionTool, Tool
from autogen_ext.models.openai import OpenAIChatCompletionClient
from pydantic import BaseModel
from message_protocols import UserTask, AgentResponse, ReplyTask

user_topic_type = "User"
reply_agent_topic_type = "ReplyAgent"

class ReplyAgent(RoutedAgent):
    def __init__(
        self,
        description: str,
        model_client: ChatCompletionClient,
        tools: List[Tool],
        delegate_tools: List[Tool],
        reply_to_topic_type: str,
    ) -> None:
        super().__init__(description)
        self._system_message = SystemMessage(content="""
        You are a reply agent working for a property management company.
        Given a work order ID and context, generate a friendly and informative response to the user.
        The response should:
        1. Acknowledge receipt of their work order request
        2. Confirm the work order ID
        3. Inform them that their request is being processed
        4. Keep the tone professional but warm
        Keep your response concise and to the point.
        """)
        self._model_client = model_client
        self._tools = dict([(tool.name, tool) for tool in tools])
        self._tool_schema = [tool.schema for tool in tools]
        self._delegate_tools = dict([(tool.name, tool) for tool in delegate_tools])
        self._delegate_tool_schema = [tool.schema for tool in delegate_tools]
        self._reply_to_topic_type = reply_to_topic_type # will be work order detection agent topic type
        self._user_topic_type = user_topic_type
        
        
    @message_handler
    async def handle_task(self, message: ReplyTask, ctx: MessageContext) -> None:
        print(f"handling task {'-'*80}\n{self.id.type}:\n{message}", flush=True)
        
        # Add work order ID to the context
        message.context.append(
            AssistantMessage(
                content=f"Work Order ID: {message.work_order_id}",
                source=self.id.type
            )
        )
        
        # Send the task to the LLM
        llm_result = await self._model_client.create(
            messages=[self._system_message] + message.context,
            tools=self._tool_schema + self._delegate_tool_schema,
            cancellation_token=ctx.cancellation_token,
        )
        print(f"{'-'*80}\n{self.id.type}:\n{llm_result.content}", flush=True)
        
        # Process the LLM result
        if isinstance(llm_result.content, str):
            # Add the response to the context
            message.context.append(AssistantMessage(content=llm_result.content, source=self.id.type))
            
            # Send the response back to the user
            await self.publish_message(
                AgentResponse(context=message.context, reply_to_topic_type=self._reply_to_topic_type),
                topic_id=TopicId(self._user_topic_type, source=self.id.key),
            )
        else:
            raise ValueError(f"Unexpected LLM result type: {type(llm_result.content)}")
