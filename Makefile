.PHONY: start-rabbitmq stop-rabbitmq start-scheduler start-worker clean

# RabbitMQ management with Docker
start-rabbitmq:
	@echo "Starting RabbitMQ container..."
	@if ! docker ps -q --filter "name=rabbitmq" | grep -q .; then \
		docker run -d --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management; \
	else \
		echo "RabbitMQ container is already running"; \
	fi

stop-rabbitmq:
	@echo "Stopping RabbitMQ container..."
	@if docker ps -q --filter "name=rabbitmq" | grep -q .; then \
		docker stop rabbitmq; \
		docker rm rabbitmq; \
	else \
		echo "RabbitMQ container is not running"; \
	fi

# Scheduler and Worker management
start-scheduler:
	@echo "Starting scheduler..."
	@python3 -m scheduling.scheduler

start-worker:
	@echo "Starting worker..."
	@python3 -m scheduling.worker

# Clean up all processes
clean:
	@echo "Cleaning up processes..."
	@-pkill -f "scheduling.scheduler"
	@-pkill -f "scheduling.worker"
	@make stop-rabbitmq
