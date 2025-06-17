# System Setup and Running Instructions

This system uses RabbitMQ for message handling and scheduling. Follow these steps to run the system:

## 1. Start RabbitMQ

To start the RabbitMQ container:

```bash
make start-rabbitmq
```

This will start RabbitMQ with management interface available at http://localhost:15672 (default credentials: guest/guest)

## 2. Run the System Components

### Start the Worker

The worker processes scheduled messages/reminders that are due:

```bash
make start-worker
```

### Start the Scheduler

The scheduler is responsible for scheduling messages/reminders based on delay:

```bash
make start-scheduler
```

## 3. Testing the System

For a quick test, you can use the scheduler client to put a request in the scheduler queue:

```bash
python scheduler_client.py
```

For a full test with custom content and delay:

```bash
python test_scheduler.py --content "Hello world" --delay 30
```

## 4. Cleanup

To stop all components and clean up:

```bash
make clean
```

This will:

- Stop the worker and scheduler processes
- Stop and remove the RabbitMQ container
