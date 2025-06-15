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


reply_agent_topic_type = "ReplyAgent"
work_order_orchestration_agent_topic_type = "WorkOrderOrchestrationAgent"


"""Execution tools"""
# None Fow now


"""Delegation tools"""
def transfer_to_reply_agent(work_order_id: str) -> Tuple[str, str]:
    """
    Transfer the task to the reply agent to inform the user that we are working on it. This is the last step in the orchestration process.
    Args:
        work_order_id (str): The work order id to transfer to the reply agent.
    Returns:
        Tuple[str, str]: The topic type and the work order id to transfer to the reply agent.
    """
    return reply_agent_topic_type, work_order_id


transfer_to_reply_agent_tool = FunctionTool(
    transfer_to_reply_agent, description="Use when property management work order id has been found and Want to inform the user that we are working on it. This is the last step in the orchestration process."
)


class WorkOrderOrchestrationAgent(RoutedAgent):
    def __init__(
        self,
        description: str,
        model_client: ChatCompletionClient,
        tools: List[Tool],
        delegate_tools: List[Tool],
    ) -> None:
        super().__init__(description)
        self._system_message = SystemMessage(content="""
        You are a work order orchestration agent working for a property management company.
        Given a user message containing a work order id, transfer the task to the reply agent to inform the user that we are working on it.
        """)
        self._model_client = model_client
        self._tools = dict([(tool.name, tool) for tool in tools])
        self._tool_schema = [tool.schema for tool in tools]
        self._delegate_tools = dict([(tool.name, tool) for tool in delegate_tools])
        self._delegate_tool_schema = [tool.schema for tool in delegate_tools]
        self._agent_topic_type = work_order_orchestration_agent_topic_type
        self._reply_agent_topic_type = reply_agent_topic_type

    @message_handler
    async def handle_task(self, message: UserTask, ctx: MessageContext) -> None:
        print(f"handling task {'-'*80}\n{self.id.type}:\n{message}", flush=True)
        # Send the task to the LLM.
        llm_result = await self._model_client.create(
            messages=[self._system_message] + message.context,
            tools=self._tool_schema + self._delegate_tool_schema,
            cancellation_token=ctx.cancellation_token,
        )
        print(f"{'-'*80}\n{self.id.type}:\n{llm_result.content}", flush=True)
        # Process the LLM result.
        while isinstance(llm_result.content, list) and all(isinstance(m, FunctionCall) for m in llm_result.content):
            tool_call_results: List[FunctionExecutionResult] = []
            delegate_targets: List[Tuple[str, ReplyTask]] = []
            # Process each function call.
            for call in llm_result.content:
                arguments = json.loads(call.arguments)
                print(f"orchestration agent: processing function call {call.name} with arguments {arguments}", flush=True)
                if call.name in self._tools:
                    # Execute the tool directly.
                    result = await self._tools[call.name].run_json(arguments, ctx.cancellation_token)
                    print(f"orchestration agent: tool call {call.name} result {result}", flush=True)
                    result_as_str = self._tools[call.name].return_value_as_string(result)
                    print(f"orchestration agent: tool call {call.name} result as string {result_as_str}", flush=True)
                    tool_call_results.append(
                        FunctionExecutionResult(call_id=call.id, content=result_as_str, is_error=False, name=call.name)
                    )
                elif call.name in self._delegate_tools:
                    # Execute the tool to get the delegate agent's topic type.
                    result = await self._delegate_tools[call.name].run_json(arguments, ctx.cancellation_token)
                    print(f"orchestration agent: delegate tool call {call.name} result {result}", flush=True)
                    topic_type, work_order_id = result
                    print(f"orchestration agent: delegate tool call {call.name} topic type {topic_type}", flush=True)
                    # Create the context for the delegate agent, including the function call and the result.
                    delegate_messages = list(message.context) + [
                        AssistantMessage(content=[call], source=self.id.type),
                        FunctionExecutionResultMessage(
                            content=[
                                FunctionExecutionResult(
                                    call_id=call.id,
                                    content=f"Transferred to {topic_type}. Adopt persona immediately.",
                                    is_error=False,
                                    name=call.name,
                                )
                            ]
                        ),
                    ]
                    print(f"orchestration agent: delegate messages {delegate_messages}", flush=True)
                    delegate_targets.append((topic_type, ReplyTask(context=delegate_messages, work_order_id=work_order_id)))
                else:
                    raise ValueError(f"Unknown tool: {call.name}")
            if len(delegate_targets) > 0:
                # Delegate the task to other agents by publishing messages to the corresponding topics.
                for topic_type, task in delegate_targets:
                    print(f"{'-'*80}\n{self.id.type}:\nDelegating to {topic_type}", flush=True)
                    await self.publish_message(task, topic_id=TopicId(topic_type, source=self.id.key))
            if len(tool_call_results) > 0:
                print(f"{'-'*80}\n{self.id.type}:\n{tool_call_results}", flush=True)
                # Make another LLM call with the results.
                message.context.extend(
                    [
                        AssistantMessage(content=llm_result.content, source=self.id.type),
                        FunctionExecutionResultMessage(content=tool_call_results),
                    ]
                )
                llm_result = await self._model_client.create(
                    messages=[self._system_message] + message.context,
                    tools=self._tool_schema + self._delegate_tool_schema,
                    cancellation_token=ctx.cancellation_token,
                )
                print(f"{'-'*80}\n{self.id.type}:\n{llm_result.content}", flush=True)
            else:
                # The task has been delegated, so we are done.
                print(f"orchestration agent: task delegated, done", flush=True)
                return
        