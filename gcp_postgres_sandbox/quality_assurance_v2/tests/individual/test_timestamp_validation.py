#!/usr/bin/env python3
"""
Timestamp Validation Test - Individual QA Module
Tests timestamp quality and business logic rules.
Based on original production QA logic.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

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

def test_timestamp_validation():
    """Test timestamp quality and business logic rules."""
    print("=== Timestamp Validation Test ===")
    print("Checking timestamp quality and business rules...")
    print()
    
    results = []
    
    try:
        config = QAConfig()
        db_manager = DatabaseManager(config)
        
        # Focus on key tables with timestamp requirements
        key_tables = ['FE_DMV_ALL', 'FE_DMV_SCORES', '1K_coins_ohlcv', 'crypto_listings_latest_1000']
        
        for table_name in key_tables:
            print(f"Validating timestamps in: {table_name}")
            
            # Check if table exists
            table_info = db_manager.table_exists('dbcp', table_name)
            if not table_info['exists']:
                print(f"[SKIP] {table_name}: Table not found")
                results.append(QAResult(
                    check_name=f"timestamps.table_missing.{table_name}",
                    status="FAIL",
                    risk_level="HIGH",
                    message=f"Cannot validate timestamps - table {table_name} not found",
                    details={"database": "dbcp", "table": table_name}
                ))
                continue
            
            actual_table_name = table_info['actual_name']
            
            try:
                # Basic timestamp analysis
                timestamp_query = '''
                SELECT 
                    MIN(timestamp) as first_ts,
                    MAX(timestamp) as last_ts,
                    COUNT(CASE WHEN timestamp IS NULL THEN 1 END) as null_count,
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT timestamp) as unique_timestamps
                FROM {table}
                '''
                
                result = db_manager.execute_performance_query_safe(
                    'dbcp', table_name, timestamp_query, f"Timestamp analysis for {actual_table_name}"
                )
                
                if result['status'] == 'success' and result['rows']:
                    data = result['rows'][0]
                    first_ts = data['first_ts']
                    last_ts = data['last_ts']
                    null_count = data['null_count']
                    total_rows = data['total_rows']
                    unique_timestamps = data['unique_timestamps']
                    
                    print(f"  Total rows: {total_rows:,}")
                    print(f"  Unique timestamps: {unique_timestamps:,}")
                    print(f"  First timestamp: {first_ts}")
                    print(f"  Last timestamp: {last_ts}")
                    print(f"  NULL timestamps: {null_count:,}")
                    
                    # Test 1: Check for NULL timestamps
                    if null_count > 0:
                        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
                        print(f"  [FAIL] {null_count:,} NULL timestamps ({null_pct:.1f}%)")
                        
                        risk_level = "CRITICAL" if null_pct > 10 else "HIGH" if null_pct > 1 else "MEDIUM"
                        
                        results.append(QAResult(
                            check_name=f"timestamps.null_values.{table_name}",
                            status="FAIL",
                            risk_level=risk_level,
                            message=f"{null_count:,} NULL timestamps in {table_name} ({null_pct:.1f}%)",
                            details={
                                "database": "dbcp",
                                "table": actual_table_name,
                                "null_count": null_count,
                                "null_percentage": null_pct,
                                "total_rows": total_rows
                            }
                        ))
                    else:
                        print(f"  [PASS] No NULL timestamps")
                        results.append(QAResult(
                            check_name=f"timestamps.no_nulls.{table_name}",
                            status="PASS",
                            risk_level="LOW",
                            message=f"No NULL timestamps in {table_name}",
                            details={"database": "dbcp", "table": actual_table_name, "total_rows": total_rows}
                        ))
                    
                    # Test 2: Business Logic - FE Tables (should have consistent timestamps)
                    if table_name.startswith('FE_') and first_ts and last_ts:
                        if first_ts != last_ts:
                            time_diff = abs((last_ts - first_ts).total_seconds()) if isinstance(first_ts, datetime) and isinstance(last_ts, datetime) else 0
                            print(f"  [WARN] FE table timestamps inconsistent (diff: {time_diff}s)")
                            
                            # Different risk levels based on time difference
                            if time_diff > 86400:  # > 1 day
                                risk_level = "HIGH"
                            elif time_diff > 3600:  # > 1 hour
                                risk_level = "MEDIUM"
                            else:
                                risk_level = "LOW"
                            
                            results.append(QAResult(
                                check_name=f"timestamps.fe_consistency.{table_name}",
                                status="WARNING",
                                risk_level=risk_level,
                                message=f"FE table timestamps inconsistent: {first_ts} != {last_ts}",
                                details={
                                    "database": "dbcp",
                                    "table": actual_table_name,
                                    "first_timestamp": str(first_ts),
                                    "last_timestamp": str(last_ts),
                                    "time_difference_seconds": time_diff
                                }
                            ))
                        else:
                            print(f"  [PASS] FE table timestamps consistent: {first_ts}")
                            results.append(QAResult(
                                check_name=f"timestamps.fe_consistency.{table_name}",
                                status="PASS",
                                risk_level="LOW",
                                message=f"FE table timestamps are consistent",
                                details={
                                    "database": "dbcp",
                                    "table": actual_table_name,
                                    "timestamp": str(first_ts)
                                }
                            ))
                    
                    # Test 3: Historical Tables (should have sufficient date range)
                    elif not table_name.startswith('FE_') and first_ts and last_ts:
                        if isinstance(first_ts, datetime) and isinstance(last_ts, datetime):
                            date_range_days = (last_ts - first_ts).days
                            print(f"  Date range: {date_range_days} days")
                            
                            if date_range_days < 4:
                                print(f"  [WARN] Insufficient date range ({date_range_days} days)")
                                results.append(QAResult(
                                    check_name=f"timestamps.date_range.{table_name}",
                                    status="WARNING",
                                    risk_level="MEDIUM",
                                    message=f"Insufficient timestamp range: {date_range_days} days",
                                    details={
                                        "database": "dbcp",
                                        "table": actual_table_name,
                                        "date_range_days": date_range_days,
                                        "first_timestamp": str(first_ts),
                                        "last_timestamp": str(last_ts)
                                    }
                                ))
                            else:
                                print(f"  [PASS] Good date range ({date_range_days} days)")
                                results.append(QAResult(
                                    check_name=f"timestamps.date_range.{table_name}",
                                    status="PASS",
                                    risk_level="LOW",
                                    message=f"Good timestamp range: {date_range_days} days",
                                    details={
                                        "database": "dbcp",
                                        "table": actual_table_name,
                                        "date_range_days": date_range_days
                                    }
                                ))
                    
                    # Test 4: Data Freshness (how recent is the latest timestamp)
                    if last_ts and isinstance(last_ts, datetime):
                        now = datetime.now(last_ts.tzinfo) if last_ts.tzinfo else datetime.now()
                        hours_since_update = (now - last_ts).total_seconds() / 3600
                        
                        print(f"  Hours since last update: {hours_since_update:.1f}")
                        
                        if hours_since_update > 48:  # > 2 days
                            print(f"  [WARN] Data may be stale ({hours_since_update:.1f} hours old)")
                            results.append(QAResult(
                                check_name=f"timestamps.data_freshness.{table_name}",
                                status="WARNING",
                                risk_level="MEDIUM",
                                message=f"Data may be stale: {hours_since_update:.1f} hours since last update",
                                details={
                                    "database": "dbcp",
                                    "table": actual_table_name,
                                    "hours_since_update": hours_since_update,
                                    "last_timestamp": str(last_ts)
                                }
                            ))
                        elif hours_since_update > 168:  # > 1 week
                            print(f"  [FAIL] Data is stale ({hours_since_update:.1f} hours old)")
                            results.append(QAResult(
                                check_name=f"timestamps.data_freshness.{table_name}",
                                status="FAIL",
                                risk_level="HIGH",
                                message=f"Data is stale: {hours_since_update:.1f} hours since last update",
                                details={
                                    "database": "dbcp",
                                    "table": actual_table_name,
                                    "hours_since_update": hours_since_update,
                                    "last_timestamp": str(last_ts)
                                }
                            ))
                        else:
                            print(f"  [PASS] Data is fresh ({hours_since_update:.1f} hours old)")
                            results.append(QAResult(
                                check_name=f"timestamps.data_freshness.{table_name}",
                                status="PASS",
                                risk_level="LOW",
                                message=f"Data is fresh: {hours_since_update:.1f} hours since last update",
                                details={
                                    "database": "dbcp",
                                    "table": actual_table_name,
                                    "hours_since_update": hours_since_update
                                }
                            ))
                    
                    # Test 5: Timestamp Distribution (check for gaps or clustering)
                    if unique_timestamps > 1:
                        distribution_query = '''
                        SELECT 
                            COUNT(*) as rows_per_timestamp,
                            COUNT(DISTINCT timestamp) as unique_timestamps_check
                        FROM {table}
                        GROUP BY timestamp
                        ORDER BY COUNT(*) DESC
                        LIMIT 1
                        '''
                        
                        dist_result = db_manager.execute_performance_query_safe(
                            'dbcp', table_name, distribution_query, f"Timestamp distribution for {actual_table_name}"
                        )
                        
                        if dist_result['status'] == 'success' and dist_result['rows']:
                            max_rows_per_timestamp = dist_result['rows'][0]['rows_per_timestamp']
                            
                            if max_rows_per_timestamp > total_rows * 0.8:  # > 80% of data in one timestamp
                                print(f"  [WARN] Timestamp clustering detected ({max_rows_per_timestamp:,} rows in single timestamp)")
                                results.append(QAResult(
                                    check_name=f"timestamps.clustering.{table_name}",
                                    status="WARNING",
                                    risk_level="MEDIUM",
                                    message=f"Timestamp clustering: {max_rows_per_timestamp:,} rows in single timestamp",
                                    details={
                                        "database": "dbcp",
                                        "table": actual_table_name,
                                        "max_rows_per_timestamp": max_rows_per_timestamp,
                                        "total_rows": total_rows
                                    }
                                ))
                            else:
                                print(f"  [PASS] Good timestamp distribution")
                                results.append(QAResult(
                                    check_name=f"timestamps.distribution.{table_name}",
                                    status="PASS",
                                    risk_level="LOW",
                                    message=f"Good timestamp distribution in {table_name}",
                                    details={"database": "dbcp", "table": actual_table_name}
                                ))
                
                else:
                    print(f"  [ERROR] Failed to analyze timestamps: {result.get('error', 'Unknown error')}")
                    results.append(QAResult(
                        check_name=f"timestamps.analysis_failed.{table_name}",
                        status="ERROR",
                        risk_level="MEDIUM",
                        message=f"Timestamp analysis failed for {table_name}",
                        details={"database": "dbcp", "table": actual_table_name, "error": result.get('error')}
                    ))
                    
            except Exception as e:
                print(f"  [ERROR] Exception analyzing {table_name}: {str(e)[:100]}")
                results.append(QAResult(
                    check_name=f"timestamps.exception.{table_name}",
                    status="ERROR",
                    risk_level="MEDIUM",
                    message=f"Exception during timestamp analysis: {str(e)[:100]}",
                    details={"database": "dbcp", "table": table_name, "error": str(e)}
                ))
            
            print()  # Blank line between tables
        
        # Summary
        print("=== TIMESTAMP VALIDATION SUMMARY ===")
        total_checks = len(results)
        passed = len([r for r in results if r.status == "PASS"])
        failed = len([r for r in results if r.status == "FAIL"])
        warnings = len([r for r in results if r.status == "WARNING"])
        errors = len([r for r in results if r.status == "ERROR"])
        
        print(f"Total Checks: {total_checks}")
        print(f"Passed: {passed}, Failed: {failed}, Warnings: {warnings}, Errors: {errors}")
        
        # Overall assessment
        if failed == 0 and errors == 0:
            if warnings == 0:
                print("Overall Status: EXCELLENT - All timestamp validations passed")
            else:
                print("Overall Status: GOOD - Minor timestamp issues found")
        elif failed > 0:
            print("Overall Status: ISSUES - Critical timestamp problems detected")
        else:
            print("Overall Status: PARTIAL - Some timestamp checks could not complete")
        
        # Send Telegram notification
        try:
            notification_system = NotificationSystem(config)
            notification_system.send_individual_test_alert(
                test_name="Timestamp Validation",
                results=results,
                test_type="timestamp_validation"
            )
        except Exception as e:
            print(f"[WARN] Failed to send Telegram notification: {e}")
        
        return results
        
    except Exception as e:
        print(f"[ERROR] Timestamp validation test failed: {e}")
        import traceback
        traceback.print_exc()
        error_result = [QAResult(
            check_name="timestamps.test_failed",
            status="ERROR",
            risk_level="CRITICAL",
            message=f"Timestamp validation test failed: {str(e)[:100]}",
            details={"error": str(e)}
        )]
        
        # Send error notification
        try:
            config = QAConfig()
            notification_system = NotificationSystem(config)
            notification_system.send_individual_test_alert(
                test_name="Timestamp Validation",
                results=error_result,
                test_type="timestamp_validation"
            )
        except:
            pass
        
        return error_result

if __name__ == "__main__":
    print("Individual Timestamp Validation Test")
    print("Checking timestamp quality and business logic rules")
    print()
    
    results = test_timestamp_validation()
    
    print(f"\nTest completed with {len(results)} checks performed.")
    print("Review results above for detailed findings.")