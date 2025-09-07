#!/usr/bin/env python3
"""Add strategic indexes and run ANALYZE"""

import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def add_strategic_indexes():
    """Add remaining strategic indexes and run ANALYZE."""
    
    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'), 
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'dbcp')
    }
    
    conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(conn_string)
    
    print("Adding strategic indexes...")
    
    # Key performance indexes for the most important tables
    strategic_indexes = [
        # FE_DMV_ALL - Most important table
        'CREATE INDEX "idx_fe_dmv_all_timestamp" ON "FE_DMV_ALL" (timestamp DESC);',
        'CREATE INDEX "idx_fe_dmv_all_slug" ON "FE_DMV_ALL" (slug);',
        
        # OHLCV data
        'ALTER TABLE "1K_coins_ohlcv" ADD CONSTRAINT "pk_1k_coins_ohlcv" PRIMARY KEY (slug, timestamp);',
        'CREATE INDEX "idx_1k_coins_ohlcv_timestamp" ON "1K_coins_ohlcv" (timestamp DESC);',
        
        # Timestamp indexes for time-series queries
        'CREATE INDEX "idx_fe_momentum_signals_timestamp" ON "FE_MOMENTUM_SIGNALS" (timestamp DESC);',
        'CREATE INDEX "idx_fe_oscillators_signals_timestamp" ON "FE_OSCILLATORS_SIGNALS" (timestamp DESC);',
        'CREATE INDEX "idx_fe_ratios_signals_timestamp" ON "FE_RATIOS_SIGNALS" (timestamp DESC);',
    ]
    
    try:
        total_time = 0
        
        for i, index_sql in enumerate(strategic_indexes):
            print(f"  Creating index {i+1}/{len(strategic_indexes)}...")
            
            start_time = time.time()
            try:
                with engine.connect() as conn:
                    conn.execute(text(index_sql))
                    conn.commit()
                index_time = time.time() - start_time
                total_time += index_time
                print(f"    Completed in {index_time:.2f} seconds")
                
            except Exception as index_error:
                print(f"    ERROR: {index_error}")
                continue
        
        print(f"\nAll indexes completed in {total_time:.2f} seconds total")
        
        # Run ANALYZE to update statistics
        print("\nRunning ANALYZE to update table statistics...")
        start_time = time.time()
        with engine.connect() as conn:
            conn.execute(text("ANALYZE;"))
            conn.commit()
        analyze_time = time.time() - start_time
        print(f"ANALYZE completed in {analyze_time:.2f} seconds")
        
        # Verify final state
        print("\nFinal verification...")
        with engine.connect() as conn:
            # Count primary keys
            pk_result = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.table_constraints 
                WHERE constraint_type = 'PRIMARY KEY' AND table_schema = 'public'
            """))
            pk_count = pk_result.scalar()
            
            # Count indexes
            idx_result = conn.execute(text("""
                SELECT COUNT(*) FROM pg_indexes 
                WHERE schemaname = 'public'
            """))
            idx_count = idx_result.scalar()
            
            print(f"Total primary keys: {pk_count}")
            print(f"Total indexes: {idx_count}")
        
        print("\n=== DATABASE OPTIMIZATION COMPLETED! ===")
        print("Ready for performance testing!")
        
        engine.dispose()
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        engine.dispose()
        return False

if __name__ == "__main__":
    add_strategic_indexes()