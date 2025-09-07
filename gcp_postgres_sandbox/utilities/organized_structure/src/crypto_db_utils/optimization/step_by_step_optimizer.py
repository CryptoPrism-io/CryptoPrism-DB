#!/usr/bin/env python3
"""Step-by-step database optimization"""

import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def step_by_step_optimization():
    """Add primary keys one table at a time."""
    
    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'), 
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'dbcp')
    }
    
    conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(conn_string)
    
    print("Step-by-step database optimization...")
    
    # Start with the smallest table for testing
    tables = ["FE_MOMENTUM_SIGNALS", "FE_TVV_SIGNALS", "FE_RATIOS_SIGNALS", "FE_OSCILLATORS_SIGNALS"]
    
    try:
        for i, table in enumerate(tables):
            print(f"\nStep {i+1}: Optimizing {table}...")
            
            # Check if already has primary key
            with engine.connect() as conn:
                check_result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM information_schema.table_constraints 
                    WHERE constraint_type = 'PRIMARY KEY' 
                    AND table_schema = 'public'
                    AND table_name = '{table}'
                """))
                existing_pk = check_result.scalar()
                
                if existing_pk > 0:
                    print(f"  Primary key already exists on {table}, skipping...")
                    continue
            
            # Add primary key
            print(f"  Adding primary key to {table}...")
            pk_sql = f'ALTER TABLE "{table}" ADD CONSTRAINT "pk_{table.lower()}" PRIMARY KEY (slug, timestamp);'
            
            start_time = time.time()
            try:
                with engine.connect() as conn:
                    conn.execute(text(pk_sql))
                    conn.commit()
                pk_time = time.time() - start_time
                print(f"  Primary key added in {pk_time:.2f} seconds")
                
                # Add basic index
                idx_sql = f'CREATE INDEX "idx_{table.lower()}_slug" ON "{table}" (slug);'
                
                start_time = time.time()
                with engine.connect() as conn:
                    conn.execute(text(idx_sql))
                    conn.commit()
                idx_time = time.time() - start_time
                print(f"  Index added in {idx_time:.2f} seconds")
                
            except Exception as table_error:
                print(f"  ERROR on {table}: {table_error}")
                continue
        
        print(f"\nStep {len(tables)+1}: Testing with larger table FE_DMV_ALL...")
        
        # Check FE_DMV_ALL
        with engine.connect() as conn:
            check_result = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.table_constraints 
                WHERE constraint_type = 'PRIMARY KEY' 
                AND table_schema = 'public'
                AND table_name = 'FE_DMV_ALL'
            """))
            existing_pk = check_result.scalar()
            
            if existing_pk == 0:
                print("  Adding primary key to FE_DMV_ALL...")
                pk_sql = 'ALTER TABLE "FE_DMV_ALL" ADD CONSTRAINT "pk_fe_dmv_all" PRIMARY KEY (slug, timestamp);'
                
                start_time = time.time()
                try:
                    with engine.connect() as conn:
                        conn.execute(text(pk_sql))
                        conn.commit()
                    pk_time = time.time() - start_time
                    print(f"  FE_DMV_ALL primary key added in {pk_time:.2f} seconds")
                    
                except Exception as dmv_error:
                    print(f"  ERROR on FE_DMV_ALL: {dmv_error}")
            else:
                print("  FE_DMV_ALL already has primary key")
        
        # Final verification
        print(f"\nFinal verification...")
        with engine.connect() as conn:
            all_pks = conn.execute(text("""
                SELECT table_name, constraint_name
                FROM information_schema.table_constraints
                WHERE constraint_type = 'PRIMARY KEY' 
                AND table_schema = 'public'
                ORDER BY table_name
            """))
            
            pk_count = 0
            for row in all_pks.fetchall():
                print(f"  PK: {row[0]} -> {row[1]}")
                pk_count += 1
            
            print(f"\nTotal primary keys created: {pk_count}")
        
        print("\nStep-by-step optimization completed!")
        engine.dispose()
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        engine.dispose()
        return False

if __name__ == "__main__":
    step_by_step_optimization()