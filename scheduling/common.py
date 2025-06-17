import os
from kombu import Connection, Exchange, Queue

# RabbitMQ configuration
RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost//')

# Exchange and queue names
EXCHANGE_NAME = 'scheduler_exchange'
QUEUE_NAME = 'followup_agent_queue'
SCHEDULE_QUEUE_NAME = 'schedule_requests'

# Create exchange
exchange = Exchange(EXCHANGE_NAME, type='direct')

# Create queues
queue = Queue(
    QUEUE_NAME,
    exchange=exchange,
    routing_key=QUEUE_NAME,
    # Remove TTL for now to avoid conflicts with existing queue
    # queue_arguments={'x-message-ttl': 86400000}  # 24 hours in milliseconds
)

schedule_queue = Queue(
    SCHEDULE_QUEUE_NAME,
    exchange=exchange,
    routing_key=SCHEDULE_QUEUE_NAME
)