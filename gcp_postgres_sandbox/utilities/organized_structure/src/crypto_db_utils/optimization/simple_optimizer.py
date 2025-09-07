#!/usr/bin/env python3
"""
Simple Database Optimizer - Execute optimization scripts
"""

import os
import time
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def execute_optimization():
    """Execute database optimization scripts."""
    
    # Database connection
    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'), 
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'dbcp')
    }
    
    conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(conn_string)
    
    print("Starting database optimization...")
    
    try:
        # Find optimization scripts
        script_dir = Path("sql_optimizations")
        pk_scripts = sorted(script_dir.glob("01_primary_keys_*.sql"))
        idx_scripts = sorted(script_dir.glob("02_strategic_indexes_*.sql"))
        
        if not pk_scripts or not idx_scripts:
            print("ERROR: Scripts not found")
            return False
        
        pk_script = pk_scripts[-1]
        idx_script = idx_scripts[-1]
        
        print(f"Using: {pk_script.name}")
        print(f"Using: {idx_script.name}")
        
        # Execute primary keys script
        print("\nStep 1: Adding primary keys...")
        with open(pk_script, 'r', encoding='utf-8') as f:
            pk_sql = f.read()
        
        start_time = time.time()
        with engine.connect() as conn:
            conn.execute(text(pk_sql))
            conn.commit()
        pk_time = time.time() - start_time
        print(f"Primary keys completed in {pk_time:.2f} seconds")
        
        # Execute indexes script  
        print("\nStep 2: Creating indexes...")
        with open(idx_script, 'r', encoding='utf-8') as f:
            idx_sql = f.read()
        
        start_time = time.time()
        with engine.connect() as conn:
            conn.execute(text(idx_sql))
            conn.commit()
        idx_time = time.time() - start_time
        print(f"Indexes completed in {idx_time:.2f} seconds")
        
        # Run ANALYZE
        print("\nStep 3: Updating statistics...")
        start_time = time.time()
        with engine.connect() as conn:
            conn.execute(text("ANALYZE;"))
            conn.commit()
        analyze_time = time.time() - start_time
        print(f"ANALYZE completed in {analyze_time:.2f} seconds")
        
        # Verify results
        print("\nStep 4: Verification...")
        with engine.connect() as conn:
            # Check primary keys
            pk_result = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.table_constraints 
                WHERE constraint_type = 'PRIMARY KEY' AND table_schema = 'public'
            """))
            pk_count = pk_result.scalar()
            
            # Check indexes  
            idx_result = conn.execute(text("""
                SELECT COUNT(*) FROM pg_indexes 
                WHERE schemaname = 'public' AND indexname LIKE 'idx_%'
            """))
            idx_count = idx_result.scalar()
            
            print(f"Primary keys created: {pk_count}")
            print(f"Indexes created: {idx_count}")
        
        print("\nOptimization completed successfully!")
        engine.dispose()
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        engine.dispose()
        return False

if __name__ == "__main__":
    execute_optimization()