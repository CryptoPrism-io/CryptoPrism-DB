#!/usr/bin/env python3
"""Test table name casing"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def test_table_names():
    """Test table name casing to debug the issue."""
    
    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'), 
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'dbcp')
    }
    
    conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(conn_string)
    
    test_queries = [
        ('Uppercase', 'SELECT COUNT(*) FROM FE_DMV_ALL LIMIT 1'),
        ('Lowercase', 'SELECT COUNT(*) FROM fe_dmv_all LIMIT 1'), 
        ('Quoted uppercase', 'SELECT COUNT(*) FROM "FE_DMV_ALL" LIMIT 1'),
        ('Quoted lowercase', 'SELECT COUNT(*) FROM "fe_dmv_all" LIMIT 1'),
    ]
    
    for name, query in test_queries:
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                count = result.scalar()
                print(f"OK {name}: {count} rows")
        except Exception as e:
            print(f"ERROR {name}: {e}")
    
    engine.dispose()

if __name__ == "__main__":
    test_table_names()