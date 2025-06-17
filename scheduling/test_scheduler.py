#!/usr/bin/env python3
import argparse
import json
import sys
from datetime import datetime
from scheduler_client import SchedulerClient
import uuid

def schedule_test_message(client, content, delay_seconds, agent_id=None):
    """Schedule a test message with the given content and delay."""
    message = {
        "id": str(uuid.uuid4()),
        "agent": agent_id or "test-agent",
        "content": content,
        "test_timestamp": datetime.now().isoformat()
    }
    
    print(f"Scheduling message:")
    print(f"  Content: {content}")
    print(f"  Delay: {delay_seconds} seconds")
    print(f"  Agent: {message['agent']}")
    print(f"  Message ID: {message['id']}")
    
    client.schedule_message(message, delay_seconds)
    print("Message scheduled successfully!\n")

def main():
    parser = argparse.ArgumentParser(description='Test message scheduler')
    parser.add_argument('--content', '-c', help='Message content to schedule')
    parser.add_argument('--delay', '-d', type=int, help='Delay in seconds')
    parser.add_argument('--agent', '-a', help='Agent ID (optional)')
    parser.add_argument('--batch', '-b', action='store_true', help='Run batch test with multiple messages')
    
    args = parser.parse_args()
    
    client = SchedulerClient()
    
    if args.batch:
        # Run a batch of test messages with different delays
        test_messages = [
            ("Immediate message", 5),
            ("Short delay message", 30),
            ("Medium delay message", 300),
            ("Long delay message", 3600),
        ]
        
        for content, delay in test_messages:
            schedule_test_message(client, content, delay)
            
    elif args.content and args.delay:
        # Schedule a single message
        schedule_test_message(client, args.content, args.delay, args.agent)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main() 
    
"""
# Schedule a single message
python test_scheduler.py --content "Hello world" --delay 30

#Schedule a message with a specific agent
python test_scheduler.py --content "Agent specific message" --delay 60 --agent "agent-123"


# Run a batch test with multiple messages at different delays:
python test_scheduler.py --batch
"""