from kombu.mixins import ConsumerMixin
from kombu import Connection
import logging
from scheduling.common import RABBITMQ_URL, queue, exchange

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('worker')

class MessageWorker(ConsumerMixin):
    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, Consumer, channel):
        return [Consumer(
            queues=[queue],
            callbacks=[self.handle_message],
            accept=['json']
        )]

    def handle_message(self, body, message):
        try:
            logger.info(f"📨 Received scheduled message: {body}")
            # Process the message here
            message.ack()
            logger.info("✅ Message processed successfully")
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
            # Don't ack the message so it can be retried
            message.requeue()

if __name__ == "__main__":
    try:
        with Connection(RABBITMQ_URL) as conn:
            worker = MessageWorker(conn)
            logger.info(f"👂 Worker listening for messages on queue: {queue.name}")
            worker.run()
    except KeyboardInterrupt:
        logger.info("Worker shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
