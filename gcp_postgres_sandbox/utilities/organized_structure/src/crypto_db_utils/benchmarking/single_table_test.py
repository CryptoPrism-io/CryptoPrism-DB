#!/usr/bin/env python3
"""Test single table optimization"""

import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def test_single_table():
    """Test optimization on just one small table."""
    
    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'), 
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'dbcp')
    }
    
    conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(conn_string)
    
    print("Testing single table optimization...")
    
    try:
        # First check if table already has primary key
        with engine.connect() as conn:
            check_result = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.table_constraints 
                WHERE constraint_type = 'PRIMARY KEY' 
                AND table_schema = 'public'
                AND table_name = 'FE_MOMENTUM_SIGNALS'
            """))
            existing_pk = check_result.scalar()
            
            print(f"Existing primary keys on FE_MOMENTUM_SIGNALS: {existing_pk}")
            
            if existing_pk > 0:
                print("Primary key already exists! Skipping...")
                return True
        
        # Test adding primary key to smallest table
        print("Adding primary key to FE_MOMENTUM_SIGNALS...")
        
        pk_sql = 'ALTER TABLE "FE_MOMENTUM_SIGNALS" ADD CONSTRAINT "pk_fe_momentum_signals" PRIMARY KEY (slug, timestamp);'
        
        start_time = time.time()
        with engine.connect() as conn:
            conn.execute(text(pk_sql))
            conn.commit()
        pk_time = time.time() - start_time
        
        print(f"Primary key added in {pk_time:.2f} seconds")
        
        # Test simple index
        print("Adding simple index...")
        idx_sql = 'CREATE INDEX "idx_test_momentum_slug" ON "FE_MOMENTUM_SIGNALS" (slug);'
        
        start_time = time.time()
        with engine.connect() as conn:
            conn.execute(text(idx_sql))
            conn.commit()
        idx_time = time.time() - start_time
        
        print(f"Index added in {idx_time:.2f} seconds")
        
        # Test query performance
        print("Testing query performance...")
        test_query = 'SELECT COUNT(*) FROM "FE_MOMENTUM_SIGNALS" WHERE slug = \'bitcoin\';'
        
        start_time = time.time()
        with engine.connect() as conn:
            result = conn.execute(text(test_query))
            count = result.scalar()
        query_time = time.time() - start_time
        
        print(f"Test query completed in {query_time*1000:.2f}ms, found {count} records")
        
        print("Single table optimization successful!")
        engine.dispose()
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        engine.dispose()
        return False

if __name__ == "__main__":
    test_single_table()