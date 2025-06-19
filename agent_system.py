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

from agents.user_agent import UserAgent
from message_protocols import UserStartSession, UserTask, AgentResponse
import os
from pathlib import Path

from agents.work_order_detection_agent import WorkOrderDetectionAgent, escalate_to_human_tool, transfer_to_work_order_orchestration_agent_tool, look_up_work_order_tool
from agents.human_agent import HumanAgent
from agents.work_order_orchestation_agent import WorkOrderOrchestrationAgent, transfer_to_reply_agent_tool
from agents.reply_agent import ReplyAgent



import logging

logging.basicConfig(level=logging.WARNING)
logging.getLogger("autogen_core").setLevel(logging.DEBUG)
# logging.getLogger("autogen_core").setLevel(logging.INFO)
# logging.getLogger("autogen_core").setLevel(logging.WARNING)



# Load API keys from ~/.bashrc
bashrc_path = Path.home() / '.bashrc'
with open(bashrc_path) as f:
    for line in f:
        if line.startswith('export OPENAI_API_KEY='):
            os.environ['OPENAI_API_KEY'] = line.split('=')[1].strip().strip('"\'')
        elif line.startswith('export DEEPSEEK_API='):
            os.environ['DEEPSEEK_API'] = line.split('=')[1].strip().strip('"\'')





"""Topics & Subscriptions"""
user_topic_type = "User"
work_order_detection_agent_topic_type = "WorkOrderDetectionAgent"
work_order_orchestration_agent_topic_type = "WorkOrderOrchestrationAgent"
human_agent_topic_type = "HumanAgent"
reply_agent_topic_type = "ReplyAgent"


"""Agent Runtime"""
runtime = SingleThreadedAgentRuntime()

model_client = OpenAIChatCompletionClient(
    model="gpt-4o-mini",
    api_key=os.environ['OPENAI_API_KEY'],
)


# Register the user agent.
async def register_user_agent():
    user_agent_type = await UserAgent.register(
        runtime,
        type=user_topic_type,
        factory=lambda: UserAgent(
            description="A user agent.",
            user_topic_type=user_topic_type,
            agent_topic_type=work_order_detection_agent_topic_type,  # Start with the work order detection agent.
        ),
    )
    # Add subscriptions for the user agent: it will receive messages published to its own topic only.
    await runtime.add_subscription(TypeSubscription(topic_type=user_topic_type, agent_type=user_agent_type.type))


# Register the work order detection agent.
async def register_work_order_detection_agent():
    work_order_detection_agent_type = await WorkOrderDetectionAgent.register(
        runtime,
        type=work_order_detection_agent_topic_type,
        factory=lambda: WorkOrderDetectionAgent(    
            description="Work order detection agent",
            model_client=model_client,
            tools=[look_up_work_order_tool],
            delegate_tools=[escalate_to_human_tool, transfer_to_work_order_orchestration_agent_tool],
        ),
    )
    # Add subscriptions for the work order detection agent: it will receive messages published to its own topic only.
    await runtime.add_subscription(TypeSubscription(topic_type=work_order_detection_agent_topic_type, agent_type=work_order_detection_agent_type.type))


# Register the human agent.
async def register_human_agent():
    human_agent_type = await HumanAgent.register(
        runtime,
        type=human_agent_topic_type,
        factory=lambda: HumanAgent(
            description="Human agent",
            agent_topic_type=human_agent_topic_type,
            user_topic_type=user_topic_type,
        ),
    )
    # Add subscriptions for the human agent: it will receive messages published to its own topic only.
    await runtime.add_subscription(TypeSubscription(topic_type=human_agent_topic_type, agent_type=human_agent_type.type))

# Register the work order orchestration agent.
async def register_work_order_orchestration_agent():
    work_order_orchestration_agent_type = await WorkOrderOrchestrationAgent.register(
        runtime,
        type=work_order_orchestration_agent_topic_type,
        factory=lambda: WorkOrderOrchestrationAgent(
            description="Work order orchestration agent",
            model_client=model_client,
            tools=[],
            delegate_tools=[transfer_to_reply_agent_tool],
        ),
    )
    # Add subscriptions for the work order orchestration agent: it will receive messages published to its own topic only.
    await runtime.add_subscription(TypeSubscription(topic_type=work_order_orchestration_agent_topic_type, agent_type=work_order_orchestration_agent_type.type))

# Register the reply agent.
async def register_reply_agent():
    reply_agent_type = await ReplyAgent.register(
        runtime,
        type=reply_agent_topic_type,
        factory=lambda: ReplyAgent(
            description="Reply agent",
            model_client=model_client,
            tools=[],
            delegate_tools=[],
            reply_to_topic_type=work_order_detection_agent_topic_type,
        ),
    )
    # Add subscriptions for the reply agent: it will receive messages published to its own topic only.
    await runtime.add_subscription(TypeSubscription(topic_type=reply_agent_topic_type, agent_type=reply_agent_type.type))




# async def load_runtime_state_if_exists(session_id: str):
#     session_file = os.path.join("sessions_persistant", f"runtime_state_{session_id}.json")
#     if os.path.exists(session_file):
#         with open(session_file, "r") as f:
#             state = json.load(f)
#         await runtime.load_state(state)


async def main():
    await register_user_agent()
    await register_work_order_detection_agent()
    await register_human_agent()
    await register_work_order_orchestration_agent()
    await register_reply_agent()
    
    # Create sessions directory if it doesn't exist
    os.makedirs("sessions_persistant", exist_ok=True)
    
    # Load saved state (if it exists)
    # await load_runtime_state_if_exists(session_id)
    
    # Start the runtime.
    runtime.start()

    # Create a new session for the user.
    session_id = str(uuid.uuid4())
    print(f"Creating new session for user with ID: {session_id}")
    await runtime.publish_message(UserStartSession(), topic_id=TopicId(user_topic_type, source=session_id))

    # # Save state before exit
    # state = await runtime.save_state()
    # session_file = os.path.join("sessions_persistant", f"runtime_state_{session_id}.json")
    # with open(session_file, "w") as f:
    #     json.dump(state, f, indent=4)
        
    # Run until completion.
    await runtime.stop_when_idle()
    await model_client.close()

if __name__ == "__main__":
    asyncio.run(main())