#!/usr/bin/env python3
"""
Direct database connection check to verify table existence.
"""

import os
import sys
from pathlib import Path
import sqlalchemy
from sqlalchemy import create_engine, text

# Add paths for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from dotenv import load_dotenv

# Load environment
load_dotenv(current_dir.parent.parent / ".env", override=True)

def direct_database_check():
    """Direct database connection to check table existence."""
    print("=== Direct Database Connection Test ===")
    
    try:
        # Build connection string directly
        DB_HOST = os.getenv('DB_HOST')
        DB_USER = os.getenv('DB_USER') 
        DB_PASSWORD = os.getenv('DB_PASSWORD')
        DB_PORT = os.getenv('DB_PORT', '5432')
        DB_NAME = os.getenv('DB_NAME', 'dbcp')
        
        print(f"Connecting to: {DB_HOST}:{DB_PORT}/{DB_NAME} as {DB_USER}")
        
        # Create engine
        connection_string = f'postgresql+pg8000://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(connection_string, echo=False)
        
        print("[PASS] Database engine created successfully")
        
        # Test connection
        with engine.connect() as conn:
            print("[PASS] Database connection established")
            
            # Get all table names
            print("\n>>> LISTING ALL TABLES IN DATABASE")
            print("-" * 50)
            
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
            """))
            
            tables = [row[0] for row in result.fetchall()]
            print(f"Found {len(tables)} tables:")
            
            for i, table in enumerate(tables, 1):
                print(f"{i:2d}. {table}")
            
            # Check specific tables the QA system was looking for
            print(f"\n>>> CHECKING SPECIFIC TABLES")
            print("-" * 50)
            
            expected_tables = [
                'fe_dmv_all',
                'fe_dmv_scores', 
                'fe_momentum_signals',
                'fe_oscillators_signals',
                'fe_ratios_signals',
                'fe_metrics_signal',
                'fe_tvv_signals',
                'crypto_listings_latest_1000'
            ]
            
            # Special case for table with number prefix
            number_prefix_tables = [
                '1k_coins_ohlcv',
                '"1K_coins_ohlcv"'
            ]
            
            found_tables = []
            missing_tables = []
            
            for table in expected_tables:
                if table in tables:
                    found_tables.append(table)
                    print(f"[FOUND] {table}")
                    
                    # Get row count
                    try:
                        count_result = conn.execute(text(f'SELECT COUNT(*) FROM {table}'))
                        row_count = count_result.fetchone()[0]
                        print(f"        -> {row_count:,} rows")
                    except Exception as e:
                        print(f"        -> Error counting rows: {str(e)[:50]}")
                        
                else:
                    missing_tables.append(table)
                    print(f"[MISSING] {table}")
            
            # Check the number-prefix table specially
            print(f"\n>>> CHECKING NUMBER-PREFIX TABLES")
            print("-" * 50)
            
            for table_variant in number_prefix_tables:
                try:
                    count_result = conn.execute(text(f'SELECT COUNT(*) FROM {table_variant}'))
                    row_count = count_result.fetchone()[0]
                    print(f"[FOUND] {table_variant} -> {row_count:,} rows")
                    found_tables.append(table_variant)
                    break
                except Exception as e:
                    print(f"[MISSING] {table_variant} -> {str(e)[:80]}")
            
            # Look for similar table names (fuzzy matching)
            print(f"\n>>> FUZZY MATCHING FOR MISSING TABLES")
            print("-" * 50)
            
            for missing in missing_tables:
                similar_tables = [t for t in tables if missing in t or t in missing]
                if similar_tables:
                    print(f"[SIMILAR] {missing} -> Found similar: {similar_tables}")
                else:
                    print(f"[NO_MATCH] {missing} -> No similar tables found")
            
            # Summary
            print(f"\n=== VERIFICATION SUMMARY ===")
            total_expected = len(expected_tables) + 1  # +1 for ohlcv table
            total_found = len(found_tables)
            
            print(f"Expected Tables: {total_expected}")
            print(f"Found Tables: {total_found}")
            print(f"Missing Tables: {total_expected - total_found}")
            print(f"Database Completeness: {(total_found / total_expected) * 100:.1f}%")
            
            # QA Validation
            if total_found == 0:
                print(f"\n[VALIDATION] QA findings are CORRECT - No signal tables exist")
                print(f"[CONCLUSION] Technical analysis pipeline has not been executed")
            elif total_found < total_expected * 0.5:
                print(f"\n[VALIDATION] QA findings are MOSTLY CORRECT - Pipeline incomplete")
                print(f"[CONCLUSION] Technical analysis pipeline partially executed")
            else:
                print(f"\n[VALIDATION] QA findings may be INCORRECT - Most tables exist")
                print(f"[CONCLUSION] QA system may have table access or naming issues")
            
            return True
            
    except Exception as e:
        print(f"[ERROR] Direct database check failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Direct Database Verification")
    print("Bypassing QA system to check actual database state...")
    print()
    
    success = direct_database_check()
    
    print("\n" + "=" * 60)
    if success:
        print("DIRECT DATABASE CHECK COMPLETED!")
    else:
        print("DIRECT DATABASE CHECK FAILED!")
    print("=" * 60)