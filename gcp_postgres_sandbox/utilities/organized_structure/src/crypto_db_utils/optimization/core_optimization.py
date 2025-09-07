#!/usr/bin/env python3
"""
Core Database Optimization - Focus on main FE_ tables only
"""

import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def execute_core_optimization():
    """Execute optimization on core tables that we're sure have slug+timestamp."""
    
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
    
    print("Starting CORE database optimization...")
    print("Targeting main FE_ tables and OHLCV tables only")
    
    # Core tables we know have slug+timestamp
    core_tables = [
        "FE_DMV_ALL",
        "FE_MOMENTUM_SIGNALS", 
        "FE_OSCILLATORS_SIGNALS",
        "FE_RATIOS_SIGNALS",
        "FE_TVV_SIGNALS",
        "1K_coins_ohlcv"
    ]
    
    try:
        print(f"\nTargeting {len(core_tables)} core tables:")
        for table in core_tables:
            print(f"  - {table}")
        
        # Step 1: Add primary keys for core tables
        print("\nStep 1: Adding primary keys to core tables...")
        
        pk_statements = []
        for table in core_tables:
            pk_statements.append(f'ALTER TABLE "{table}" ADD CONSTRAINT "pk_{table.lower().replace("k_", "k_")}" PRIMARY KEY (slug, timestamp);')
        
        pk_sql = "BEGIN;\n" + "\n".join(pk_statements) + "\nCOMMIT;"
        
        start_time = time.time()
        with engine.connect() as conn:
            conn.execute(text(pk_sql))
            conn.commit()
        pk_time = time.time() - start_time
        print(f"Primary keys completed in {pk_time:.2f} seconds")
        
        # Step 2: Add strategic indexes
        print("\nStep 2: Creating strategic indexes...")
        
        index_statements = [
            'BEGIN;',
            '-- Core performance indexes',
            'CREATE INDEX CONCURRENTLY "idx_fe_dmv_all_timestamp" ON "FE_DMV_ALL" (timestamp DESC);',
            'CREATE INDEX CONCURRENTLY "idx_fe_dmv_all_slug" ON "FE_DMV_ALL" (slug);', 
            'CREATE INDEX CONCURRENTLY "idx_fe_momentum_signals_timestamp" ON "FE_MOMENTUM_SIGNALS" (timestamp DESC);',
            'CREATE INDEX CONCURRENTLY "idx_1k_coins_ohlcv_timestamp" ON "1K_coins_ohlcv" (timestamp DESC);',
            'CREATE INDEX CONCURRENTLY "idx_1k_coins_ohlcv_slug" ON "1K_coins_ohlcv" (slug);',
            'COMMIT;'
        ]
        
        idx_sql = "\n".join(index_statements)
        
        start_time = time.time()
        with engine.connect() as conn:
            conn.execute(text(idx_sql))
            conn.commit()
        idx_time = time.time() - start_time
        print(f"Indexes completed in {idx_time:.2f} seconds")
        
        # Step 3: Run ANALYZE
        print("\nStep 3: Updating table statistics...")
        start_time = time.time()
        with engine.connect() as conn:
            conn.execute(text("ANALYZE;"))
            conn.commit()
        analyze_time = time.time() - start_time
        print(f"ANALYZE completed in {analyze_time:.2f} seconds")
        
        # Step 4: Verification
        print("\nStep 4: Verifying results...")
        with engine.connect() as conn:
            # Check primary keys
            pk_result = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.table_constraints 
                WHERE constraint_type = 'PRIMARY KEY' AND table_schema = 'public'
                AND table_name IN ('FE_DMV_ALL', 'FE_MOMENTUM_SIGNALS', 'FE_OSCILLATORS_SIGNALS', 
                                  'FE_RATIOS_SIGNALS', 'FE_TVV_SIGNALS', '1K_coins_ohlcv')
            """))
            pk_count = pk_result.scalar()
            
            # Check indexes
            idx_result = conn.execute(text("""
                SELECT COUNT(*) FROM pg_indexes 
                WHERE schemaname = 'public' AND indexname LIKE 'idx_%'
            """))
            idx_count = idx_result.scalar()
            
            # Test a query
            test_result = conn.execute(text("""
                SELECT COUNT(*) FROM "FE_DMV_ALL" WHERE slug = 'bitcoin'
            """))
            test_count = test_result.scalar()
            
            print(f"Core table primary keys: {pk_count}")
            print(f"Performance indexes: {idx_count}")
            print(f"Test query (bitcoin records): {test_count}")
        
        print("\n=== CORE OPTIMIZATION COMPLETED SUCCESSFULLY! ===")
        print(f"Total time: {pk_time + idx_time + analyze_time:.2f} seconds")
        print("\nOptimized tables:")
        for table in core_tables:
            print(f"  ✓ {table}")
        
        engine.dispose()
        return True
        
    except Exception as e:
        print(f"ERROR during optimization: {e}")
        
        # Attempt rollback
        print("\nAttempting rollback...")
        try:
            rollback_statements = []
            for table in core_tables:
                rollback_statements.append(f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "pk_{table.lower().replace("k_", "k_")}";')
            
            rollback_sql = "BEGIN;\n" + "\n".join(rollback_statements) + "\nCOMMIT;"
            
            with engine.connect() as conn:
                conn.execute(text(rollback_sql))
                conn.commit()
            print("Rollback completed")
            
        except Exception as rollback_error:
            print(f"Rollback also failed: {rollback_error}")
        
        engine.dispose()
        return False

if __name__ == "__main__":
    success = execute_core_optimization()
    print(f"\nOptimization {'SUCCESS' if success else 'FAILED'}")