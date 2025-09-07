#!/usr/bin/env python3
"""
Duplicate Detection Test - Individual QA Module
Tests for duplicate records using (slug, timestamp) composite keys.
Based on original production QA logic.
"""

import sys
from pathlib import Path

# Add paths for imports - go up two levels to reach quality_assurance_v2 root
current_dir = Path(__file__).parent
qa_root = current_dir.parent.parent
sys.path.insert(0, str(qa_root))

try:
    from quality_assurance_v2.core.config import QAConfig
    from quality_assurance_v2.core.database import DatabaseManager
    from quality_assurance_v2.core.base_qa import QAResult
    from quality_assurance_v2.reporting.notification_system import NotificationSystem
except ImportError:
    try:
        from core.config import QAConfig
        from core.database import DatabaseManager
        from core.base_qa import QAResult
        from reporting.notification_system import NotificationSystem
    except ImportError as e:
        print(f"Import error: {e}")
        sys.exit(1)

def test_duplicate_detection():
    """Test for duplicate records in key tables."""
    print("=== Duplicate Detection Test ===")
    print("Checking for duplicate (slug, timestamp) combinations...")
    print()
    
    results = []
    
    try:
        config = QAConfig()
        db_manager = DatabaseManager(config)
        
        # Focus on key tables that should have unique (slug, timestamp) combinations
        key_tables = ['FE_DMV_ALL', 'FE_DMV_SCORES', '1K_coins_ohlcv', 'crypto_listings_latest_1000']
        
        for table_name in key_tables:
            print(f"Checking duplicates in: {table_name}")
            
            # Check if table exists
            table_info = db_manager.table_exists('dbcp', table_name)
            if not table_info['exists']:
                print(f"[SKIP] {table_name}: Table not found")
                results.append(QAResult(
                    check_name=f"duplicates.table_missing.{table_name}",
                    status="FAIL",
                    risk_level="HIGH",
                    message=f"Cannot check duplicates - table {table_name} not found",
                    details={"database": "dbcp", "table": table_name}
                ))
                continue
            
            actual_table_name = table_info['actual_name']
            
            try:
                # Check for duplicates using the original QA logic
                duplicate_query = '''
                SELECT COUNT(*) - COUNT(DISTINCT (slug, timestamp)) as duplicates,
                       COUNT(*) as total_rows,
                       COUNT(DISTINCT (slug, timestamp)) as unique_combinations
                FROM {table} 
                WHERE slug IS NOT NULL AND timestamp IS NOT NULL
                '''
                
                result = db_manager.execute_performance_query_safe(
                    'dbcp', table_name, duplicate_query, f"Duplicate check for {actual_table_name}"
                )
                
                if result['status'] == 'success' and result['rows']:
                    data = result['rows'][0]
                    duplicate_count = data['duplicates']
                    total_rows = data['total_rows']
                    unique_combinations = data['unique_combinations']
                    
                    print(f"  Total rows: {total_rows:,}")
                    print(f"  Unique combinations: {unique_combinations:,}")
                    print(f"  Duplicates: {duplicate_count:,}")
                    
                    if duplicate_count > 0:
                        duplicate_pct = (duplicate_count / total_rows * 100) if total_rows > 0 else 0
                        print(f"  [FAIL] {duplicate_count:,} duplicate records ({duplicate_pct:.1f}%)")
                        
                        risk_level = "CRITICAL" if duplicate_pct > 10 else "HIGH" if duplicate_pct > 1 else "MEDIUM"
                        
                        results.append(QAResult(
                            check_name=f"duplicates.found.{table_name}",
                            status="FAIL",
                            risk_level=risk_level,
                            message=f"Found {duplicate_count:,} duplicate records in {table_name} ({duplicate_pct:.1f}%)",
                            details={
                                "database": "dbcp",
                                "table": actual_table_name,
                                "duplicate_count": duplicate_count,
                                "total_rows": total_rows,
                                "duplicate_percentage": duplicate_pct
                            }
                        ))
                    else:
                        print(f"  [PASS] No duplicates found")
                        results.append(QAResult(
                            check_name=f"duplicates.clean.{table_name}",
                            status="PASS",
                            risk_level="LOW",
                            message=f"No duplicate records found in {table_name}",
                            details={
                                "database": "dbcp",
                                "table": actual_table_name,
                                "total_rows": total_rows,
                                "unique_combinations": unique_combinations
                            }
                        ))
                    
                    # Additional check: rows with NULL slug or timestamp
                    null_check_query = '''
                    SELECT 
                        COUNT(CASE WHEN slug IS NULL THEN 1 END) as null_slug_count,
                        COUNT(CASE WHEN timestamp IS NULL THEN 1 END) as null_timestamp_count,
                        COUNT(*) as total_rows
                    FROM {table}
                    '''
                    
                    null_result = db_manager.execute_performance_query_safe(
                        'dbcp', table_name, null_check_query, f"Null key check for {actual_table_name}"
                    )
                    
                    if null_result['status'] == 'success' and null_result['rows']:
                        null_data = null_result['rows'][0]
                        null_slugs = null_data['null_slug_count']
                        null_timestamps = null_data['null_timestamp_count']
                        
                        if null_slugs > 0 or null_timestamps > 0:
                            print(f"  [WARN] NULL keys found - Slug: {null_slugs:,}, Timestamp: {null_timestamps:,}")
                            results.append(QAResult(
                                check_name=f"duplicates.null_keys.{table_name}",
                                status="WARNING",
                                risk_level="MEDIUM",
                                message=f"NULL key values in {table_name} - Slug: {null_slugs:,}, Timestamp: {null_timestamps:,}",
                                details={
                                    "database": "dbcp",
                                    "table": actual_table_name,
                                    "null_slug_count": null_slugs,
                                    "null_timestamp_count": null_timestamps
                                }
                            ))
                        else:
                            print(f"  [PASS] All key columns populated")
                            results.append(QAResult(
                                check_name=f"duplicates.keys_complete.{table_name}",
                                status="PASS",
                                risk_level="LOW",
                                message=f"All slug and timestamp values populated in {table_name}",
                                details={"database": "dbcp", "table": actual_table_name}
                            ))
                else:
                    print(f"  [ERROR] Failed to check duplicates: {result.get('error', 'Unknown error')}")
                    results.append(QAResult(
                        check_name=f"duplicates.check_failed.{table_name}",
                        status="ERROR",
                        risk_level="MEDIUM",
                        message=f"Duplicate check failed for {table_name}",
                        details={"database": "dbcp", "table": actual_table_name, "error": result.get('error')}
                    ))
                    
            except Exception as e:
                print(f"  [ERROR] Exception checking {table_name}: {str(e)[:100]}")
                results.append(QAResult(
                    check_name=f"duplicates.exception.{table_name}",
                    status="ERROR",
                    risk_level="MEDIUM",
                    message=f"Exception during duplicate check: {str(e)[:100]}",
                    details={"database": "dbcp", "table": table_name, "error": str(e)}
                ))
            
            print()  # Blank line between tables
        
        # Summary
        print("=== DUPLICATE DETECTION SUMMARY ===")
        total_checks = len(results)
        passed = len([r for r in results if r.status == "PASS"])
        failed = len([r for r in results if r.status == "FAIL"])
        warnings = len([r for r in results if r.status == "WARNING"])
        errors = len([r for r in results if r.status == "ERROR"])
        
        print(f"Total Checks: {total_checks}")
        print(f"Passed: {passed}, Failed: {failed}, Warnings: {warnings}, Errors: {errors}")
        
        # Overall assessment
        if failed == 0 and errors == 0:
            print("Overall Status: CLEAN - No duplicate issues found")
        elif failed > 0:
            print("Overall Status: ISSUES FOUND - Duplicates detected")
        else:
            print("Overall Status: PARTIAL - Some checks could not complete")
        
        # Send Telegram notification
        try:
            notification_system = NotificationSystem(config)
            notification_system.send_individual_test_alert(
                test_name="Duplicate Detection",
                results=results,
                test_type="duplicate_detection"
            )
        except Exception as e:
            print(f"[WARN] Failed to send Telegram notification: {e}")
        
        return results
        
    except Exception as e:
        print(f"[ERROR] Duplicate detection test failed: {e}")
        import traceback
        traceback.print_exc()
        error_result = [QAResult(
            check_name="duplicates.test_failed",
            status="ERROR",
            risk_level="CRITICAL",
            message=f"Duplicate detection test failed: {str(e)[:100]}",
            details={"error": str(e)}
        )]
        
        # Send error notification
        try:
            config = QAConfig()
            notification_system = NotificationSystem(config)
            notification_system.send_individual_test_alert(
                test_name="Duplicate Detection",
                results=error_result,
                test_type="duplicate_detection"
            )
        except:
            pass
        
        return error_result

if __name__ == "__main__":
    print("Individual Duplicate Detection Test")
    print("Checking for duplicate (slug, timestamp) combinations in key tables")
    print()
    
    results = test_duplicate_detection()
    
    print(f"\nTest completed with {len(results)} checks performed.")
    print("Review results above for detailed findings.")