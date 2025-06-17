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
from typing import Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential
import pytz
from enum import Enum
from scheduling.common import RABBITMQ_URL, EXCHANGE_NAME, QUEUE_NAME, SCHEDULE_QUEUE_NAME


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

class JobStatus(Enum):
    SCHEDULED = "scheduled"
    DELIVERED = "delivered"
    MISSED = "missed"
    FAILED = "failed"

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
    # How long to keep completed/failed jobs in the database (in days)
    JOB_CLEANUP_DAYS = int(os.getenv('JOB_CLEANUP_DAYS', '7'))

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


def cleanup_old_jobs():
    """Remove jobs that are older than JOB_CLEANUP_DAYS."""
    try:
        cutoff_date = datetime.now(pytz.UTC) - timedelta(days=Config.JOB_CLEANUP_DAYS)
        jobs = scheduler.get_jobs()
        for job in jobs:
            if job.next_run_time and job.next_run_time < cutoff_date:
                logger.info(f"Removing old job {job.id} scheduled for {job.next_run_time}")
                job.remove()
    except Exception as e:
        logger.error(f"Error during job cleanup: {e}")

def update_job_status(job_id: str, status: JobStatus, error: str = None):
    """Update job status in the database."""
    try:
        job = scheduler.get_job(job_id)
        if job:
            # Store status in job's kwargs
            job_kwargs = job.kwargs or {}
            job_kwargs['status'] = status.value
            job_kwargs['status_updated_at'] = datetime.now(pytz.UTC).isoformat()
            if error:
                job_kwargs['error'] = error
            job.modify(kwargs=job_kwargs)
            logger.info(f"Updated job {job_id} status to {status.value}")
    except Exception as e:
        logger.error(f"Error updating job status: {e}")

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
        update_job_status(job_id, JobStatus.DELIVERED)
    except OperationalError as e:
        error_msg = f"Failed to send message: {e}"
        logger.error(error_msg)
        update_job_status(job_id, JobStatus.FAILED, error_msg)
        raise

def schedule_message(payload: Dict[str, Any], delay_seconds: int) -> str:
    """Schedule a message to be sent after delay_seconds."""
    now = datetime.now(pytz.timezone(Config.TIMEZONE))
    run_time = now + timedelta(seconds=delay_seconds)
    
    # Validate run time is in the future
    if run_time <= now:
        raise ValueError(f"Cannot schedule job in the past. Run time: {run_time}, Current time: {now}")
    
    job_id = f"{payload.get('id', uuid.uuid4())}"
    
    try:
        # Store metadata in job's kwargs
        job_metadata = {
            'status': JobStatus.SCHEDULED.value,
            'scheduled_at': now.isoformat(),
            'scheduled_for': run_time.isoformat()
        }
        
        scheduler.add_job(
            func=send_message,
            trigger='date',
            run_date=run_time,
            args=[payload, job_id],  # Pass job_id to send_message
            id=job_id,
            replace_existing=True,
            misfire_grace_time=Config.MISFIRE_GRACE_TIME,
            kwargs=job_metadata  # Store metadata in job's kwargs
        )
        logger.info(f"⏳ Scheduled message for {run_time} ({Config.TIMEZONE}): {payload}")
        return job_id
    except Exception as e:
        error_msg = f"Failed to schedule message: {e}"
        logger.error(error_msg)
        update_job_status(job_id, JobStatus.FAILED, error_msg)
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
    # Mark any running jobs as failed
    for job in scheduler.get_jobs():
        if job.next_run_time and job.next_run_time <= datetime.now(pytz.UTC):
            update_job_status(job.id, JobStatus.MISSED, "Scheduler shutdown during execution")
    scheduler.shutdown()
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    
    try:
        scheduler.start()
        logger.info(f"Scheduler started successfully (Timezone: {Config.TIMEZONE})")
        
        # Clean up old jobs on startup
        # cleanup_old_jobs()
        
        # Start consuming from the schedule requests queue
        start_queue_consumer()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        scheduler.shutdown()
        sys.exit(1)
