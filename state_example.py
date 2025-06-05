import asyncio
import json
from dataclasses import dataclass

from autogen_core import (
    AgentId,
    MessageContext,
    RoutedAgent,
    SingleThreadedAgentRuntime,
    message_handler,
)

# Define a simple message type
@dataclass
class MyMessageType:
    content: str

# Implement a custom agent
class MyAgent(RoutedAgent):
    def __init__(self) -> None:
        super().__init__("MyAgent")
        self._counter = 0  # Example internal state

    @message_handler
    async def handle_my_message_type(self, message: MyMessageType, ctx: MessageContext) -> None:
        self._counter += 1
        print(f"[{self.id.key}] Received: {message.content} (Count: {self._counter})")

    async def save_state(self) -> dict:
        return {"counter": self._counter}

    async def load_state(self, state: dict) -> None:
        self._counter = state.get("counter", 0)

async def main():
    # Initialize the runtime
    runtime = SingleThreadedAgentRuntime()

    # Register the agent
    await MyAgent.register(runtime, "my_agent", lambda: MyAgent())

    # Start the runtime
    runtime.start()

    # Send a message to the agent
    agent_id = AgentId("my_agent", "default")
    await runtime.send_message(MyMessageType("Hello, World!"), agent_id)

    # Save the runtime state to a file
    state = await runtime.save_state()
    with open("example_runtime_state.json", "w") as f:
        json.dump(state, f, indent=4)

    # Stop the runtime
    await runtime.stop()

    # Load the runtime state from the file
    with open("example_runtime_state.json", "r") as f:
        loaded_state = json.load(f)
    await runtime.load_state(loaded_state)

    # Restart the runtime
    runtime.start()

    # Send another message to the agent
    await runtime.send_message(MyMessageType("Hello again!"), agent_id)

    # Stop the runtime
    await runtime.stop()

# Run the main function
asyncio.run(main())
