# Scheduling System

This module provides a scheduling system with RabbitMQ integration.

## Quick Start

### Using Makefile (Recommended)

1. Start everything (RabbitMQ, scheduler, and worker):

```bash
make all
```

2. To stop everything:

```bash
make clean
```

3. Individual components can be started separately:

```bash
make start-rabbitmq    # Start RabbitMQ container
make start-scheduler   # Start the scheduler
make start-worker      # Start the worker
```

### Manual Setup

If you prefer to run components manually:

0. Start RabbitMQ:

```bash
docker run -d --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

1. Run the worker:

```bash
python scheduling/worker.py
```

2. Run the scheduler:

```bash
python scheduling/scheduler.py
```

3. For quick test, run the scheduler client:

```bash
python scheduling/scheduler_client.py
```

4. For full test:

```bash
python scheduling/test_scheduler.py --content "Hello world" --delay 30
```

## Configuration

The system uses the following environment variables (with defaults):

- RABBITMQ_URL: amqp://guest:guest@localhost//
- QUEUE_NAME: followup_agent_queue
- SCHEDULE_QUEUE_NAME: schedule_requests
- DB_URL: sqlite:///jobs.sqlite
