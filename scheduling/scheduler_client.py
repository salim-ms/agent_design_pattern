from kombu import Connection, Producer, Exchange
import os
from typing import Dict, Any
import json

class SchedulerClient:
    def __init__(self, rabbitmq_url: str = None):
        self.rabbitmq_url = rabbitmq_url or os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost//')
        self.exchange = Exchange('scheduler_exchange', type='direct')
        self.schedule_queue_name = os.getenv('SCHEDULE_QUEUE_NAME', 'schedule_requests')
    
    def schedule_message(self, payload: Dict[str, Any], delay_seconds: int) -> None:
        """
        Schedule a message to be delivered after delay_seconds.
        
        Args:
            payload: The message payload to be delivered
            delay_seconds: Number of seconds to wait before delivering the message
        """
        # Create a new connection for each message to ensure proper cleanup
        with Connection(self.rabbitmq_url) as connection:
            with connection.channel() as channel:
                producer = Producer(channel, exchange=self.exchange)
                producer.publish(
                    {
                        'payload': payload,
                        'delay_seconds': delay_seconds
                    },
                    routing_key=self.schedule_queue_name,
                    serializer='json',
                    delivery_mode=2  # Make message persistent
                )

# Example usage:
if __name__ == "__main__":
    client = SchedulerClient()
    message = {
        "id": "test-123",
        "agent": "agent-007",
        "content": "Test scheduled message"
    }
    client.schedule_message(message, delay_seconds=30)
    print("Schedule request sent to queue")