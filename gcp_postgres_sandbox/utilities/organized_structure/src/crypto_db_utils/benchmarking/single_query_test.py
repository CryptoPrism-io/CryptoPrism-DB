#!/usr/bin/env python3
"""Test single query execution"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import time

load_dotenv()

def test_single_query():
    """Test a single query to ensure it works."""
    
    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'), 
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'dbcp')
    }
    
    conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(conn_string)
    
    query = """
        SELECT f.slug, f.timestamp, f.bullish, f.bearish, f.neutral,
               m.rsi_14, m.roc_21, m.williams_r_14
        FROM "FE_DMV_ALL" f 
        JOIN "FE_MOMENTUM_SIGNALS" m ON f.slug = m.slug AND f.timestamp = m.timestamp
        WHERE f.timestamp >= '2025-08-29'
        LIMIT 10
    """
    
    try:
        start_time = time.time()
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            execution_time = time.time() - start_time
            
            print(f"Query executed successfully!")
            print(f"Execution time: {execution_time:.2f} seconds")
            print(f"Rows returned: {len(rows)}")
            
            if rows:
                print("First row:", dict(rows[0]._mapping))
                
    except Exception as e:
        print(f"ERROR: {e}")
    
    engine.dispose()

if __name__ == "__main__":
    test_single_query()