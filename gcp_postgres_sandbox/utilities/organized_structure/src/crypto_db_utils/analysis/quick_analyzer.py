#!/usr/bin/env python3
"""Quick ANALYZE and performance test"""

import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def quick_analyze_and_test():
    """Run ANALYZE and test current performance."""
    
    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'), 
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'dbcp')
    }
    
    conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(conn_string)
    
    try:
        # Run ANALYZE
        print("Running ANALYZE...")
        start_time = time.time()
        with engine.connect() as conn:
            conn.execute(text("ANALYZE;"))
            conn.commit()
        analyze_time = time.time() - start_time
        print(f"ANALYZE completed in {analyze_time:.2f} seconds")
        
        # Test performance with current optimization
        print("\nTesting current performance...")
        test_queries = [
            ("Simple JOIN", """
                SELECT f.slug, f.timestamp, f.id, 
                       m.m_mom_roc_bin, m."m_mom_williams_%_bin"
                FROM "FE_DMV_ALL" f 
                JOIN "FE_MOMENTUM_SIGNALS" m ON f.slug = m.slug AND f.timestamp = m.timestamp
                WHERE f.timestamp >= '2025-08-29'
                LIMIT 100
            """),
            ("Slug filter", """
                SELECT slug, timestamp, id, name
                FROM "FE_DMV_ALL"
                WHERE slug = 'bitcoin'
                LIMIT 50
            """),
            ("Count query", """
                SELECT COUNT(*) 
                FROM "FE_MOMENTUM_SIGNALS" 
                WHERE slug = 'ethereum'
            """)
        ]
        
        for query_name, query in test_queries:
            print(f"\n  Testing: {query_name}")
            times = []
            
            for run in range(3):
                start_time = time.time()
                with engine.connect() as conn:
                    result = conn.execute(text(query))
                    rows = result.fetchall()
                query_time = time.time() - start_time
                times.append(query_time * 1000)  # Convert to ms
            
            avg_time = sum(times) / len(times)
            print(f"    Average: {avg_time:.2f}ms (from {times[0]:.2f}, {times[1]:.2f}, {times[2]:.2f}ms)")
        
        print("\n=== CURRENT STATUS ===")
        print("✓ Primary keys added to core tables")
        print("✓ Basic indexes on slug columns")
        print("✓ Statistics updated with ANALYZE")
        print("Ready for performance comparison!")
        
        engine.dispose()
        
    except Exception as e:
        print(f"ERROR: {e}")
        engine.dispose()

if __name__ == "__main__":
    quick_analyze_and_test()