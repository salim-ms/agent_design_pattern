from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from kombu import Connection, Producer, Consumer, Queue, Exchange
from kombu.exceptions import OperationalError
import uuid
import json
import time
import logging
import os
import signal
import sys
from typing import Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
import pytz
from scheduling.common import RABBITMQ_URL, EXCHANGE_NAME, QUEUE_NAME, SCHEDULE_QUEUE_NAME
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pickle


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('scheduler.log')
    ]
)
logger = logging.getLogger('scheduler')

# Configuration
class Config:
    RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost//')
    QUEUE_NAME = os.getenv('QUEUE_NAME', 'followup_agent_queue')
    SCHEDULE_QUEUE_NAME = os.getenv('SCHEDULE_QUEUE_NAME', 'schedule_requests')
    DB_URL = os.getenv('DB_URL', 'sqlite:///jobs.sqlite')
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))
    TIMEZONE = os.getenv('TIMEZONE', 'UTC')
    # How long to wait before considering a job missed (in seconds)
    MISFIRE_GRACE_TIME = int(os.getenv('MISFIRE_GRACE_TIME', '3600'))  # 1 hour default

# Initialize SQLAlchemy engine and session
engine = create_engine(Config.DB_URL)
Session = sessionmaker(bind=engine)


# Initialize persistent scheduler
scheduler = BackgroundScheduler(
    jobstores={
        'default': SQLAlchemyJobStore(url=Config.DB_URL)
    },
    timezone=Config.TIMEZONE
)

# Initialize RabbitMQ connection and queues
connection = Connection(Config.RABBITMQ_URL)
exchange = Exchange(EXCHANGE_NAME, type='direct')
schedule_queue = Queue(Config.SCHEDULE_QUEUE_NAME, exchange=exchange, routing_key=Config.SCHEDULE_QUEUE_NAME)


@retry(
    stop=stop_after_attempt(Config.MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def send_message(payload: Dict[str, Any], job_id: str, **kwargs) -> None:
    """
    Send message to RabbitMQ with retry logic.
    
    Args:
        payload: The message payload to be delivered
        job_id: The ID of the job
        **kwargs: Additional metadata (ignored)
    """
    try:
        with Connection(Config.RABBITMQ_URL) as conn:
            producer = Producer(conn)
            producer.publish(
                payload,
                exchange=exchange,
                routing_key=Config.QUEUE_NAME,
                serializer='json',
                delivery_mode=2  # Make message persistent
            )
        logger.info(f"🔔 Delivered scheduled message: {payload}")
    except OperationalError as e:
        error_msg = f"Failed to send message: {e}"
        logger.error(error_msg)
        raise

def schedule_message(payload: Dict[str, Any], delay_seconds: int) -> str:
    """Schedule a message to be sent after delay_seconds."""
    now = datetime.now(pytz.timezone(Config.TIMEZONE))
    run_time = now + timedelta(seconds=delay_seconds)
    
    # Validate run time is in the future
    if run_time <= now:
        raise ValueError(f"Cannot schedule job in the past. Run time: {run_time}, Current time: {now}")
    
    # Extract user_id from payload
    user_id = payload.get('user_id')
    if not user_id:
        logger.error("No user_id provided in payload")
        raise ValueError("user_id is required in payload")

    # to keep single message for a user, we use user_id as job_id
    job_id = user_id
    
    try:
        # Add the job
        scheduler.add_job(
            func=send_message,
            trigger='date',
            run_date=run_time,
            args=[payload, job_id],
            id=job_id,
            replace_existing=True,
            misfire_grace_time=Config.MISFIRE_GRACE_TIME
        )
        
        logger.info(f"⏳ Scheduled message for {run_time} ({Config.TIMEZONE}) for user {user_id}: {payload}")
        return job_id
    except Exception as e:
        error_msg = f"Failed to schedule message: {e}"
        logger.error(error_msg)
        raise

def process_schedule_request(body, message):
    """Process a scheduling request from the queue."""
    try:
        payload = body.get('payload')
        delay_seconds = body.get('delay_seconds')
        
        if not payload or not isinstance(delay_seconds, (int, float)):
            logger.error(f"Invalid schedule request: {body}")
            message.ack()
            return
            
        job_id = schedule_message(payload, delay_seconds)
        logger.info(f"Processed schedule request for job {job_id}")
        message.ack()
    except Exception as e:
        logger.error(f"Error processing schedule request: {e}")
        # Don't ack the message so it can be retried
        message.requeue()

def start_queue_consumer():
    """Start consuming from the schedule requests queue."""
    with connection.channel() as channel:
        consumer = Consumer(
            channel=channel,
            queues=[schedule_queue],
            callbacks=[process_schedule_request],
            prefetch_count=1
        )
        consumer.consume()
        
        while True:
            try:
                connection.drain_events()
            except Exception as e:
                logger.error(f"Error in queue consumer: {e}")
                time.sleep(1)  # Prevent tight loop on errors

def shutdown_handler(signum, frame):
    """Handle graceful shutdown."""
    logger.info("Received shutdown signal. Cleaning up...")
    scheduler.shutdown()
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    
    try:
        scheduler.start()
        logger.info(f"Scheduler started successfully (Timezone: {Config.TIMEZONE})")
        
        # Start consuming from the schedule requests queue
        start_queue_consumer()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        scheduler.shutdown()
        sys.exit(1)
