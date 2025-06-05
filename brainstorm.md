The agents required are:

- UserAgent: Represents the user and interacts with the system
- HumanAgent: Represents the human in the loop and is responsible for sending messages back to the user
- WorkOrderDetectionAgent: Detects work orders in the user's message and ensures that we have all the information we need to process the work order
- WorkOrderOrchestrationAgent: Orchestrates the work order by coordinating the other agents - ReplyWriterAgent, ReplyEditorAgent, FollowUpAgent, HumanAgent
- ReplyWriterAgent: Writes the reply to the user
- ReplyEditorAgent: Edits the reply to the user ensuring it adheres to the organization's tone and style
- FollowUpAgent: Schedules follow up message in advance with the user if needed based on organization's policy (3h, 12h, 24h etc)
- ReplyFinalizerAgent: Finalizes the reply to the user in structured format

Design & Architecture
the design will be a mix of sequential and Orchestration design pattern

Sequential flow

- UserAgent -> WorkOrderDetectionAgent -> WorkOrderOrchestrationAgent

Orchestration flow
2 states

1. If user is requesting or escalating by requesting a human agent

- WorkOrderOrchestrationAgent -> HumanAgent

2. If user is making a request that can be handled by the AI agents reply to the user

- WorkOrderOrchestrationAgent -> ReplyWriterAgent -> ReplyEditorAgent -> ReplyFinalizerAgent + FollowUpAgent
