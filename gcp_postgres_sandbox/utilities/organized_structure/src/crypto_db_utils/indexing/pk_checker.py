#!/usr/bin/env python3
"""Check existing primary keys"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def check_existing_primary_keys():
    """Check what primary keys already exist."""
    
    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'), 
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'dbcp')
    }
    
    conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(conn_string)
    
    print("Checking existing primary keys...")
    
    # Core tables we want to optimize
    core_tables = [
        "FE_DMV_ALL",
        "FE_MOMENTUM_SIGNALS", 
        "FE_OSCILLATORS_SIGNALS",
        "FE_RATIOS_SIGNALS",
        "FE_TVV_SIGNALS",
        "1K_coins_ohlcv"
    ]
    
    try:
        with engine.connect() as conn:
            # Check each table individually
            for table in core_tables:
                pk_result = conn.execute(text(f"""
                    SELECT constraint_name, array_agg(column_name ORDER BY ordinal_position) as columns
                    FROM information_schema.key_column_usage
                    WHERE table_schema = 'public' 
                    AND table_name = '{table}'
                    AND constraint_name IN (
                        SELECT constraint_name 
                        FROM information_schema.table_constraints
                        WHERE constraint_type = 'PRIMARY KEY'
                        AND table_schema = 'public'
                        AND table_name = '{table}'
                    )
                    GROUP BY constraint_name
                """))
                
                pk_info = pk_result.fetchone()
                
                if pk_info:
                    print(f"HAS PK {table}: {pk_info[0]} ({pk_info[1]})")
                else:
                    print(f"NO PK {table}: MISSING PRIMARY KEY")
            
            # Check all existing primary keys in the database
            print("\nAll existing primary keys:")
            all_pks = conn.execute(text("""
                SELECT table_name, constraint_name
                FROM information_schema.table_constraints
                WHERE constraint_type = 'PRIMARY KEY' 
                AND table_schema = 'public'
                ORDER BY table_name
            """))
            
            for row in all_pks.fetchall():
                print(f"  {row[0]}: {row[1]}")
        
        engine.dispose()
        
    except Exception as e:
        print(f"ERROR: {e}")
        engine.dispose()

if __name__ == "__main__":
    check_existing_primary_keys()