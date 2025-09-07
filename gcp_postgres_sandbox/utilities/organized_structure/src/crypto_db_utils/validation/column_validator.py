#!/usr/bin/env python3
"""Check columns in problematic tables"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

load_dotenv()

def check_table_columns():
    """Check columns in tables that failed."""
    
    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'), 
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'dbcp')
    }
    
    conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(conn_string)
    inspector = inspect(engine)
    
    # Check problematic tables
    problem_tables = ["NEWS_TOKENOMICS_W", "NEWS_AIRDROPS_W", "FE_CC_INFO_URL"]
    
    for table_name in problem_tables:
        print(f"\n=== {table_name} ===")
        try:
            columns = inspector.get_columns(table_name)
            column_names = [col['name'] for col in columns]
            print(f"Columns: {column_names}")
            
            # Check for common patterns
            has_slug = 'slug' in column_names
            has_timestamp = 'timestamp' in column_names
            has_date = any('date' in col.lower() for col in column_names)
            has_id = 'id' in column_names
            
            print(f"Has slug: {has_slug}")
            print(f"Has timestamp: {has_timestamp}")  
            print(f"Has date field: {has_date}")
            print(f"Has id: {has_id}")
            
        except Exception as e:
            print(f"ERROR: {e}")
    
    engine.dispose()

if __name__ == "__main__":
    check_table_columns()