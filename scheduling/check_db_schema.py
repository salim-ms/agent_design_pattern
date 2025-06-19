#!/usr/bin/env python3
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os

DB_URL = os.getenv('DB_URL', 'sqlite:///jobs.sqlite')
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)

with Session() as session:
    try:
        result = session.execute(text('PRAGMA table_info(apscheduler_jobs)'))
        columns = result.fetchall()
        print('Current table columns:')
        for col in columns:
            print(f'  {col[1]} ({col[2]})')
            
        # Check if user_id column exists
        has_user_id = any(col[1] == 'user_id' for col in columns)
        print(f'\nuser_id column exists: {has_user_id}')
        
        if not has_user_id:
            print('Adding user_id column...')
            session.execute(text('ALTER TABLE apscheduler_jobs ADD COLUMN user_id VARCHAR(255)'))
            session.commit()
            print('user_id column added successfully!')
            
    except Exception as e:
        print(f'Error: {e}') 