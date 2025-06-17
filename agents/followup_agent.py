import asyncio
from autogen_core import (
    MessageContext,
    RoutedAgent,
    TopicId,
    message_handler,
)
from autogen_core.models import (
    UserMessage,
)
from message_protocols import AgentResponse, FollowUpMessage, FollowUpPayload
from scheduling.scheduler_client import SchedulerClient

# Topic type for the followup agent
followup_agent_topic_type = "FollowUpAgent"


"""
FollowUpAgent is a agent that will receive every message a user receives from our agents and decides if it needs to be followed up with a message to the user at a later time.

It uses the scheduler system to manage the timing by sending a message to scheduler_queue via SchedulerClient.

The message will be a dict with the following keys:
- "payload": the message to be delivered
- "delay_seconds": the number of seconds to wait before delivering the message

The message will be pushed to another queue followup_agent_queue in RabbitMQ.

For the followup Agent to receive the delayed message from its queue the application or agent-runtime must subscribe to the followup_agent_queue and route the message to the followup_agent.
via its topic type FollowUpAgent.

"""
class FollowUpAgent(RoutedAgent):
    def __init__(self, description: str) -> None:
        super().__init__(description)
        self._followup_agent_topic_type = followup_agent_topic_type

        self._scheduler_client = SchedulerClient()


    """Capture and intercept all messages from other agents and decide if it needs to be followed up with a message to the user at a later time."""
    @message_handler
    async def handle_response_to_user(self, message: AgentResponse, ctx: MessageContext) -> None:
        print(f"FollowUp Agent received AgentResponse {'-'*80}")
        print(f"FollowUpAgent: Message From: {message.reply_from_topic_type}")
        print(f"FollowUpAgent: Message Reply To: {message.reply_to_topic_type}")
        print(f"FollowUpAgent: Message context: {message.context}")
        print(f"{'-'*80}")
        
        """
        Note for future self:
        when running the system with User Input input(), messages received by the followup agent will contain the user input.
        due to followup agent running after user input, in final system, we won't be blocking on user input. so this should work
        
        alternatively, we push messages to followup agent directly and explicitly from other agents interaction with user. but cleaner to just interecept
        """
        # TODO: decide if it needs to be followed up with a message to the user at a later time.
        # if yes, schedule a message to be delivered after delay_seconds.
        # if no, do nothing.
        # the message to be delivered is the message.content
        # the delay_seconds is 10 seconds
        
        
        # self.schedule_message(FollowUpPayload(user_session_id=message.user_session_id, work_order_id=message.work_order_id, delay_seconds=10, message_to_deliver=message.content))
        
    
    
    @message_handler
    async def handle_followup_message(self, message: FollowUpMessage, ctx: MessageContext) -> None:
        print(f"FollowUp Agent received FollowUpMessage {'-'*80}")
        print(f"Message content: {message.payload.model_dump_json()}")
        print(f"{'-'*80}")

        # TODO: deliver the payload to the OrchestrationAgent via the OrchestrationAgent topic type to craft final message to the user
        # NOte that the message will be intercepted again by the followup agent so this agent must be able to determine if need to schedule another follow up message based on
        # a follow up message and so on. "Full Agency"
    
    
    def schedule_message(self, followup_payload: FollowUpPayload) -> None:
        """
        Schedule a message to be delivered after delay_seconds.
        """
        
        delay_seconds = followup_payload.delay_seconds
        payload = followup_payload.model_dump_json()
        
        # use SchedulerClient to schedule the message
        self._scheduler_client.schedule_message(payload, delay_seconds)
    
    
        