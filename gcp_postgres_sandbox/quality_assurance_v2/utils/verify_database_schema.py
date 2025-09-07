#!/usr/bin/env python3
"""
Verify actual database schema and table existence in DBCP database.
Check if QA findings are accurate or if there are naming/access issues.
"""

import sys
from pathlib import Path

# Add paths for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

try:
    from quality_assurance_v2.core.config import QAConfig
    from quality_assurance_v2.core.database import DatabaseManager
except ImportError:
    try:
        from core.config import QAConfig
        from core.database import DatabaseManager
    except ImportError as e:
        print(f"Import error: {e}")
        sys.exit(1)

def verify_database_schema():
    """Verify actual database schema and table existence."""
    print("=== Database Schema Verification ===")
    print("Checking actual DBCP database structure...")
    print()
    
    try:
        config = QAConfig()
        db_manager = DatabaseManager(config)
        print("[INFO] Database connection initialized")
        
        # Get list of all tables in the database
        print("\n>>> PHASE 1: LIST ALL TABLES IN DBCP DATABASE")
        print("-" * 60)
        
        schema_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name;
        """
        
        result = db_manager.execute_performance_query('dbcp', schema_query, "List all tables")
        
        if result['status'] == 'success' and result.get('rows'):
            all_tables = [row[0] for row in result['rows']]
            print(f"[FOUND] {len(all_tables)} tables in DBCP database:")
            
            for i, table in enumerate(all_tables, 1):
                print(f"{i:2d}. {table}")
        else:
            print("[ERROR] Failed to retrieve table list")
            return False
        
        # Check for signal-related tables
        print(f"\n>>> PHASE 2: ANALYZE SIGNAL AND FEATURE TABLES")
        print("-" * 60)
        
        signal_tables = [t for t in all_tables if 'signal' in t.lower() or 'fe_' in t.lower()]
        if signal_tables:
            print(f"[FOUND] {len(signal_tables)} signal/feature tables:")
            for table in signal_tables:
                print(f"  - {table}")
        else:
            print("[INFO] No signal/feature tables found with expected naming pattern")
        
        # Check for input data tables
        print(f"\n>>> PHASE 3: ANALYZE INPUT DATA TABLES")
        print("-" * 60)
        
        # Look for cryptocurrency-related tables
        crypto_tables = [t for t in all_tables if any(keyword in t.lower() 
                                                    for keyword in ['crypto', 'coin', 'ohlc', 'listing', '1k'])]
        if crypto_tables:
            print(f"[FOUND] {len(crypto_tables)} cryptocurrency-related tables:")
            for table in crypto_tables:
                # Get row count for each crypto table
                try:
                    count_query = f'SELECT COUNT(*) FROM "{table}"'
                    count_result = db_manager.execute_performance_query('dbcp', count_query, f"Count rows in {table}")
                    if count_result['status'] == 'success' and count_result.get('rows'):
                        row_count = count_result['rows'][0][0]
                        print(f"  - {table}: {row_count:,} rows")
                    else:
                        print(f"  - {table}: Unable to count rows")
                except Exception as e:
                    print(f"  - {table}: Error accessing table ({str(e)[:50]})")
        else:
            print("[INFO] No cryptocurrency input tables found")
        
        # Check specific tables that QA reported as missing
        print(f"\n>>> PHASE 4: VERIFY QA REPORTED MISSING TABLES")
        print("-" * 60)
        
        qa_expected_tables = [
            'FE_DMV_ALL',
            'FE_DMV_SCORES', 
            'FE_MOMENTUM_SIGNALS',
            'FE_OSCILLATORS_SIGNALS',
            'FE_RATIOS_SIGNALS',
            'FE_METRICS_SIGNAL',
            'FE_TVV_SIGNALS',
            '1K_coins_ohlcv',
            'crypto_listings_latest_1000'
        ]
        
        print("Checking tables that QA reported as missing:")
        found_count = 0
        
        for expected_table in qa_expected_tables:
            # Check exact match
            exact_match = expected_table in all_tables
            
            # Check case-insensitive match
            case_insensitive_matches = [t for t in all_tables if t.lower() == expected_table.lower()]
            
            # Check partial match
            partial_matches = [t for t in all_tables if expected_table.lower() in t.lower() or t.lower() in expected_table.lower()]
            
            if exact_match:
                print(f"  [EXACT] {expected_table} - EXISTS")
                found_count += 1
            elif case_insensitive_matches:
                print(f"  [CASE-INSENSITIVE] {expected_table} -> Found: {case_insensitive_matches}")
                found_count += 1
            elif partial_matches:
                print(f"  [PARTIAL] {expected_table} -> Similar: {partial_matches}")
            else:
                print(f"  [MISSING] {expected_table} - NOT FOUND")
        
        print(f"\nTable Verification Summary:")
        print(f"  Expected Tables: {len(qa_expected_tables)}")
        print(f"  Found Tables: {found_count}")
        print(f"  Missing Tables: {len(qa_expected_tables) - found_count}")
        print(f"  QA Accuracy: {(found_count / len(qa_expected_tables)) * 100:.1f}%")
        
        # Test database connection method that QA reported as failing
        print(f"\n>>> PHASE 5: TEST DATABASE CONNECTION METHODS")
        print("-" * 60)
        
        try:
            # Test the method QA was trying to use
            if hasattr(db_manager, 'test_connection'):
                connection_test = db_manager.test_connection('dbcp')
                print(f"[INFO] test_connection() method exists: {connection_test}")
            else:
                print("[INFO] test_connection() method does not exist (QA finding confirmed)")
            
            # Test actual connection
            with db_manager.get_connection('dbcp') as conn:
                test_query = "SELECT 1 as test"
                test_result = conn.execute(test_query)
                if test_result:
                    print("[PASS] Database connection is working correctly")
                else:
                    print("[FAIL] Database connection test failed")
                    
        except Exception as e:
            print(f"[ERROR] Connection test failed: {e}")
        
        # Check for case sensitivity issues
        print(f"\n>>> PHASE 6: CASE SENSITIVITY AND NAMING ANALYSIS")
        print("-" * 60)
        
        # Test if PostgreSQL requires quoted identifiers
        test_cases = [
            ('crypto_listings_latest_1000', 'Standard lowercase'),
            ('CRYPTO_LISTINGS_LATEST_1000', 'Standard uppercase'), 
            ('"crypto_listings_latest_1000"', 'Quoted lowercase'),
            ('"CRYPTO_LISTINGS_LATEST_1000"', 'Quoted uppercase')
        ]
        
        for table_name, description in test_cases:
            try:
                test_query = f"SELECT COUNT(*) FROM {table_name}"
                test_result = db_manager.execute_performance_query('dbcp', test_query, f"Test {description}")
                if test_result['status'] == 'success':
                    row_count = test_result.get('rows', [[0]])[0][0]
                    print(f"  [PASS] {table_name} ({description}): {row_count:,} rows")
                    break  # Found working format
                else:
                    print(f"  [FAIL] {table_name} ({description}): Query failed")
            except Exception as e:
                print(f"  [FAIL] {table_name} ({description}): {str(e)[:80]}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Schema verification failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Database Schema Verification Tool")
    print("Checking if QA findings about missing tables are accurate...")
    print()
    
    success = verify_database_schema()
    
    print("\n" + "=" * 70)
    if success:
        print("SCHEMA VERIFICATION COMPLETED!")
        print("Check the analysis above to validate QA findings.")
    else:
        print("SCHEMA VERIFICATION FAILED!")
    print("=" * 70)