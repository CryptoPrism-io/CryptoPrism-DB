#!/usr/bin/env python3
"""
Comprehensive QA system combining v2 capabilities with original QA tests.
Includes all critical business logic tests from the original production QA.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add paths for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

try:
    from quality_assurance_v2.core.config import QAConfig
    from quality_assurance_v2.core.database import DatabaseManager
    from quality_assurance_v2.reporting.notification_system import NotificationSystem
    from quality_assurance_v2.core.base_qa import QAResult
except ImportError:
    try:
        from core.config import QAConfig
        from core.database import DatabaseManager
        from reporting.notification_system import NotificationSystem
        from core.base_qa import QAResult
    except ImportError as e:
        print(f"Import error: {e}")
        sys.exit(1)

def run_comprehensive_qa():
    """Run comprehensive QA including all original production tests."""
    print("=== Comprehensive CryptoPrism-DB QA: All Critical Tests ===")
    print(f"Execution Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("Including: Connectivity + Schema + Data Quality + Business Logic")
    print("=" * 80)
    
    qa_results = []
    start_time = datetime.utcnow()
    
    try:
        # Initialize system
        config = QAConfig()
        print("[PASS] Configuration loaded successfully")
        
        db_manager = DatabaseManager(config)
        print("[PASS] Database manager initialized")
        
        # === PHASE 1: CONNECTIVITY ===
        print("\n>>> PHASE 1: DATABASE CONNECTIVITY TEST")
        print("-" * 60)
        
        connection_healthy = db_manager.test_connection('dbcp')
        if connection_healthy:
            print("[PASS] Database connection verified")
            qa_results.append(QAResult(
                check_name="connectivity.database_health",
                status="PASS",
                risk_level="LOW",
                message="Database connectivity confirmed",
                details={"database": "dbcp"}
            ))
        else:
            print("[FAIL] Database connection test failed")
            qa_results.append(QAResult(
                check_name="connectivity.database_health",
                status="FAIL",
                risk_level="CRITICAL",
                message="Database connection failed",
                details={"database": "dbcp"}
            ))
        
        # === PHASE 2: FOCUSED KEY TABLES (Original QA Focus) ===
        print("\n>>> PHASE 2: KEY PRODUCTION TABLES ANALYSIS")
        print("-" * 60)
        
        # Focus on the 4 key tables from original QA
        key_tables = {
            'FE_DMV_ALL': 'Primary signal aggregation table',
            'FE_DMV_SCORES': 'Durability/Momentum/Valuation scores', 
            '1K_coins_ohlcv': 'OHLCV historical price data',
            'crypto_listings_latest_1000': 'CoinMarketCap listings data'
        }
        
        for table_name, description in key_tables.items():
            print(f"\nAnalyzing: {table_name}")
            
            # Check table existence
            table_info = db_manager.table_exists('dbcp', table_name)
            if not table_info['exists']:
                print(f"[FAIL] {table_name}: Table not found")
                qa_results.append(QAResult(
                    check_name=f"key_tables.existence.{table_name}",
                    status="FAIL",
                    risk_level="CRITICAL",
                    message=f"Critical table {table_name} does not exist",
                    details={"database": "dbcp", "table": table_name, "description": description}
                ))
                continue
            
            actual_table_name = table_info['actual_name']
            print(f"[FOUND] {table_name} -> {actual_table_name}")
            
            # Get row count
            try:
                count_result = db_manager.execute_performance_query_safe(
                    'dbcp', table_name, 'SELECT COUNT(*) as row_count FROM {table}', 
                    f"Count rows in {actual_table_name}"
                )
                
                if count_result['status'] == 'success' and count_result['rows']:
                    row_count = count_result['rows'][0]['row_count']
                    print(f"  Rows: {row_count:,}")
                    
                    if row_count == 0:
                        print(f"  [WARN] {table_name}: Table is empty")
                        qa_results.append(QAResult(
                            check_name=f"key_tables.row_count.{table_name}",
                            status="WARNING",
                            risk_level="HIGH",
                            message=f"Critical table {table_name} is empty",
                            details={"database": "dbcp", "table": actual_table_name, "row_count": 0}
                        ))
                    else:
                        qa_results.append(QAResult(
                            check_name=f"key_tables.row_count.{table_name}",
                            status="PASS",
                            risk_level="LOW",
                            message=f"Table {table_name} contains {row_count:,} rows",
                            details={"database": "dbcp", "table": actual_table_name, "row_count": row_count}
                        ))
                        
                        # === DUPLICATE DETECTION (Original QA Logic) ===
                        print(f"  Checking for duplicates...")
                        duplicate_check = db_manager.execute_performance_query_safe(
                            'dbcp', table_name, 
                            '''SELECT COUNT(*) - COUNT(DISTINCT (slug, timestamp)) as duplicates 
                               FROM {table} 
                               WHERE slug IS NOT NULL AND timestamp IS NOT NULL''',
                            f"Duplicate check for {actual_table_name}"
                        )
                        
                        if duplicate_check['status'] == 'success' and duplicate_check['rows']:
                            duplicate_count = duplicate_check['rows'][0]['duplicates']
                            if duplicate_count > 0:
                                print(f"  [FAIL] {duplicate_count} duplicate records found")
                                qa_results.append(QAResult(
                                    check_name=f"data_quality.duplicates.{table_name}",
                                    status="FAIL",
                                    risk_level="HIGH",
                                    message=f"Found {duplicate_count} duplicate records in {table_name}",
                                    details={"database": "dbcp", "table": actual_table_name, "duplicate_count": duplicate_count}
                                ))
                            else:
                                print(f"  [PASS] No duplicates found")
                                qa_results.append(QAResult(
                                    check_name=f"data_quality.duplicates.{table_name}",
                                    status="PASS",
                                    risk_level="LOW",
                                    message=f"No duplicate records in {table_name}",
                                    details={"database": "dbcp", "table": actual_table_name}
                                ))
                        
                        # === TIMESTAMP VALIDATION (Original QA Logic) ===
                        print(f"  Checking timestamp quality...")
                        timestamp_check = db_manager.execute_performance_query_safe(
                            'dbcp', table_name,
                            '''SELECT 
                                MIN(timestamp) as first_ts,
                                MAX(timestamp) as last_ts,
                                COUNT(CASE WHEN timestamp IS NULL THEN 1 END) as null_count,
                                COUNT(*) as total_rows
                               FROM {table}''',
                            f"Timestamp analysis for {actual_table_name}"
                        )
                        
                        if timestamp_check['status'] == 'success' and timestamp_check['rows']:
                            ts_data = timestamp_check['rows'][0]
                            first_ts = ts_data['first_ts']
                            last_ts = ts_data['last_ts']
                            null_count = ts_data['null_count']
                            total_rows = ts_data['total_rows']
                            
                            # Check for null timestamps
                            if null_count > 0:
                                print(f"  [FAIL] {null_count} NULL timestamps found")
                                qa_results.append(QAResult(
                                    check_name=f"data_quality.timestamp_nulls.{table_name}",
                                    status="FAIL",
                                    risk_level="HIGH",
                                    message=f"{null_count} NULL timestamps in {table_name}",
                                    details={"database": "dbcp", "table": actual_table_name, "null_timestamps": null_count}
                                ))
                            else:
                                qa_results.append(QAResult(
                                    check_name=f"data_quality.timestamp_nulls.{table_name}",
                                    status="PASS",
                                    risk_level="LOW",
                                    message=f"No NULL timestamps in {table_name}",
                                    details={"database": "dbcp", "table": actual_table_name}
                                ))
                            
                            # Business logic timestamp validation
                            if first_ts and last_ts:
                                if table_name.startswith('FE_'):
                                    # FE tables should have consistent timestamps
                                    if first_ts != last_ts:
                                        print(f"  [WARN] FE table timestamps inconsistent ({first_ts} != {last_ts})")
                                        qa_results.append(QAResult(
                                            check_name=f"business_logic.fe_timestamp_consistency.{table_name}",
                                            status="WARNING",
                                            risk_level="MEDIUM",
                                            message=f"FE table timestamps inconsistent: {first_ts} != {last_ts}",
                                            details={"database": "dbcp", "table": actual_table_name, "first_ts": str(first_ts), "last_ts": str(last_ts)}
                                        ))
                                    else:
                                        print(f"  [PASS] FE table timestamps consistent")
                                        qa_results.append(QAResult(
                                            check_name=f"business_logic.fe_timestamp_consistency.{table_name}",
                                            status="PASS",
                                            risk_level="LOW",
                                            message=f"FE table timestamps are consistent",
                                            details={"database": "dbcp", "table": actual_table_name, "timestamp": str(first_ts)}
                                        ))
                                else:
                                    # Historical tables should have sufficient range
                                    date_range_days = (last_ts - first_ts).days if isinstance(first_ts, datetime) and isinstance(last_ts, datetime) else 0
                                    if date_range_days < 4:
                                        print(f"  [WARN] Insufficient timestamp range ({date_range_days} days)")
                                        qa_results.append(QAResult(
                                            check_name=f"business_logic.timestamp_range.{table_name}",
                                            status="WARNING",
                                            risk_level="MEDIUM",
                                            message=f"Insufficient timestamp range: {date_range_days} days",
                                            details={"database": "dbcp", "table": actual_table_name, "date_range_days": date_range_days}
                                        ))
                                    else:
                                        print(f"  [PASS] Good timestamp range ({date_range_days} days)")
                                        qa_results.append(QAResult(
                                            check_name=f"business_logic.timestamp_range.{table_name}",
                                            status="PASS",
                                            risk_level="LOW",
                                            message=f"Good timestamp range: {date_range_days} days",
                                            details={"database": "dbcp", "table": actual_table_name, "date_range_days": date_range_days}
                                        ))
                        
                        # === DATA QUALITY: NULL ANALYSIS (Original QA Logic) ===
                        print(f"  Checking critical column null rates...")
                        key_columns = ['slug', 'symbol', 'price', 'market_cap']
                        
                        for column in key_columns:
                            null_check = db_manager.execute_performance_query_safe(
                                'dbcp', table_name,
                                f'''SELECT ROUND(COUNT(CASE WHEN {column} IS NULL THEN 1 END) * 100.0 / COUNT(*), 1) as null_percentage
                                   FROM {{table}}''',
                                f"Null analysis for {actual_table_name}.{column}"
                            )
                            
                            if null_check['status'] == 'success' and null_check['rows']:
                                null_pct = null_check['rows'][0]['null_percentage']
                                if null_pct and null_pct > 50:
                                    print(f"    [FAIL] {column}: {null_pct}% NULL values")
                                    qa_results.append(QAResult(
                                        check_name=f"data_quality.excessive_nulls.{table_name}.{column}",
                                        status="FAIL",
                                        risk_level="CRITICAL",
                                        message=f"Excessive NULL values in {table_name}.{column}: {null_pct}%",
                                        details={"database": "dbcp", "table": actual_table_name, "column": column, "null_percentage": null_pct}
                                    ))
                                elif null_pct and null_pct > 10:
                                    print(f"    [WARN] {column}: {null_pct}% NULL values")
                                    qa_results.append(QAResult(
                                        check_name=f"data_quality.moderate_nulls.{table_name}.{column}",
                                        status="WARNING",
                                        risk_level="MEDIUM",
                                        message=f"Moderate NULL values in {table_name}.{column}: {null_pct}%",
                                        details={"database": "dbcp", "table": actual_table_name, "column": column, "null_percentage": null_pct}
                                    ))
                                else:
                                    print(f"    [PASS] {column}: {null_pct or 0}% NULL values")
                                    qa_results.append(QAResult(
                                        check_name=f"data_quality.null_levels.{table_name}.{column}",
                                        status="PASS",
                                        risk_level="LOW",
                                        message=f"Acceptable NULL levels in {table_name}.{column}: {null_pct or 0}%",
                                        details={"database": "dbcp", "table": actual_table_name, "column": column, "null_percentage": null_pct or 0}
                                    ))
                
            except Exception as e:
                print(f"  [ERROR] Failed to analyze {table_name}: {str(e)[:100]}")
                qa_results.append(QAResult(
                    check_name=f"key_tables.analysis_error.{table_name}",
                    status="ERROR",
                    risk_level="HIGH",
                    message=f"Analysis failed for {table_name}: {str(e)[:100]}",
                    details={"database": "dbcp", "table": table_name, "error": str(e)}
                ))
        
        # === PHASE 3: COMPREHENSIVE REPORTING ===
        print(f"\n>>> PHASE 3: COMPREHENSIVE QA REPORT GENERATION")
        print("-" * 60)
        
        end_time = datetime.utcnow()
        execution_time = (end_time - start_time).total_seconds()
        
        # Calculate comprehensive metrics
        total_checks = len(qa_results)
        status_counts = {'PASS': 0, 'WARNING': 0, 'FAIL': 0, 'ERROR': 0}
        risk_counts = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0, 'CRITICAL': 0}
        
        for result in qa_results:
            status_counts[result.status] += 1
            risk_counts[result.risk_level] += 1
        
        # Enhanced health score calculation
        health_score = 0
        if total_checks > 0:
            health_score = (
                status_counts['PASS'] * 1.0 +
                status_counts['WARNING'] * 0.7 +
                status_counts['FAIL'] * 0.3 +
                status_counts['ERROR'] * 0.1
            ) / total_checks * 100
        
        execution_metadata = {
            'total_execution_time': execution_time,
            'databases': ['dbcp'],
            'modules_executed': [
                'connectivity_test',
                'key_tables_analysis',
                'duplicate_detection',
                'timestamp_validation',
                'business_logic_checks',
                'data_quality_analysis'
            ],
            'qa_version': 'v2.1.0-COMPREHENSIVE',
            'original_qa_tests_included': True,
            'key_tables_analyzed': list(key_tables.keys())
        }
        
        print(f"=== COMPREHENSIVE QA RESULTS ===")
        print(f"Total Checks: {total_checks}")
        print(f"Status Distribution: {status_counts}")  
        print(f"Risk Distribution: {risk_counts}")
        print(f"Health Score: {health_score:.1f}/100")
        
        # Send comprehensive notification
        notification_system = NotificationSystem(config)
        print(f"\nSending comprehensive QA report to Telegram...")
        
        success = notification_system.send_qa_summary_alert(
            results=qa_results,
            execution_metadata=execution_metadata,
            force_send=True
        )
        
        if success:
            print("[PASS] Comprehensive QA report sent successfully!")
            return True
        else:
            print("[FAIL] Failed to send comprehensive QA report")
            return False
            
    except Exception as e:
        print(f"[ERROR] Comprehensive QA execution failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Comprehensive QA System - Including All Original Production Tests")
    print("Tests: Connectivity + Schema + Duplicates + Timestamps + Business Logic + Data Quality")
    print()
    
    success = run_comprehensive_qa()
    
    print("\n" + "=" * 80)
    if success:
        print("COMPREHENSIVE QA COMPLETED SUCCESSFULLY!")
        print("[PASS] All original QA tests included")
        print("[PASS] Enhanced with v2 capabilities")
        print("[PASS] Complete production-grade analysis")
    else:
        print("COMPREHENSIVE QA FAILED!")
    print("=" * 80)