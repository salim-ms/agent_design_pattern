#!/usr/bin/env python3
import argparse
from datetime import datetime
import pytz
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os

# Initialize SQLAlchemy engine and session
DB_URL = os.getenv('DB_URL', 'sqlite:///jobs.sqlite')
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)

def print_job(job_data: dict):
    """Pretty print a job's data."""
    print("\nJob Details:")
    print(f"ID: {job_data['id']}")
    print(f"Next Run Time: {job_data['next_run_time']}")
    print(f"User ID: {job_data['user_id']}")
    print("-" * 50)

def list_all_jobs():
    """List all active jobs in the database."""
    try:
        with Session() as session:
            results = session.execute(
                text("SELECT id, next_run_time, user_id FROM apscheduler_jobs ORDER BY next_run_time ASC")
            ).mappings().all()
            
            if not results:
                print("No active jobs found")
                return
                
            print(f"\nFound {len(results)} active jobs:")
            for job in results:
                print_job(dict(job))
    except Exception as e:
        print(f"Error querying database: {e}")

def find_jobs_by_user(user_id: str):
    """Find active jobs for a specific user."""
    try:
        with Session() as session:
            results = session.execute(
                text("SELECT id, next_run_time, user_id FROM apscheduler_jobs WHERE user_id = :user_id ORDER BY next_run_time ASC"),
                {"user_id": user_id}
            ).mappings().all()
            
            if not results:
                print(f"No active jobs found for user {user_id}")
                return
                
            print(f"\nFound {len(results)} active jobs for user {user_id}:")
            for job in results:
                print_job(dict(job))
    except Exception as e:
        print(f"Error querying database: {e}")

def main():
    parser = argparse.ArgumentParser(description='Query APScheduler database')
    parser.add_argument('--all', action='store_true', help='List all active jobs')
    parser.add_argument('--user', help='Find active jobs for specific user')
    
    args = parser.parse_args()
    
    if args.all:
        list_all_jobs()
    elif args.user:
        find_jobs_by_user(args.user)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

"""
Example usage:

# List all active jobs
python scheduling/query_scheduler_db.py --all

# Find active jobs for a specific user
python scheduling/query_scheduler_db.py --user "user-123"
""" 