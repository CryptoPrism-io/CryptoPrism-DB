#!/usr/bin/env python3
"""Inspect column names in key tables"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

load_dotenv()

def inspect_table_columns():
    """Inspect columns in key tables."""
    
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
    
    key_tables = ["FE_DMV_ALL", "FE_MOMENTUM_SIGNALS", "FE_OSCILLATORS_SIGNALS", "FE_RATIOS_SIGNALS"]
    
    for table_name in key_tables:
        print(f"\n=== {table_name} ===")
        try:
            columns = inspector.get_columns(table_name)
            print(f"Total columns: {len(columns)}")
            
            # Show first few columns and their types
            for i, col in enumerate(columns[:10]):
                print(f"  {col['name']} ({col['type']})")
            
            if len(columns) > 10:
                print(f"  ... and {len(columns) - 10} more columns")
            
            # Check for key patterns
            column_names = [col['name'] for col in columns]
            rsi_cols = [name for name in column_names if 'rsi' in name.lower()]
            if rsi_cols:
                print(f"RSI columns: {rsi_cols}")
                
        except Exception as e:
            print(f"ERROR: {e}")
    
    engine.dispose()

if __name__ == "__main__":
    inspect_table_columns()