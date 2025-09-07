#!/usr/bin/env python3
"""
Data Quality Test - Individual QA Module
Tests for excessive NULL values in critical columns.
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

def test_data_quality():
    """Test data quality focusing on NULL value analysis for critical columns."""
    print("=== Data Quality Test ===")
    print("Checking for excessive NULL values in critical columns...")
    print()
    
    results = []
    
    try:
        config = QAConfig()
        db_manager = DatabaseManager(config)
        
        # Focus on key tables and their critical columns
        table_column_mapping = {
            'FE_DMV_ALL': ['slug', 'symbol', 'price', 'market_cap', 'dmv_score'],
            'FE_DMV_SCORES': ['slug', 'symbol', 'durability_score', 'momentum_score', 'valuation_score'],
            '1K_coins_ohlcv': ['slug', 'symbol', 'open', 'high', 'low', 'close', 'volume'],
            'crypto_listings_latest_1000': ['slug', 'symbol', 'price', 'market_cap', 'percent_change_24h']
        }
        
        for table_name, critical_columns in table_column_mapping.items():
            print(f"Checking data quality in: {table_name}")
            
            # Check if table exists
            table_info = db_manager.table_exists('dbcp', table_name)
            if not table_info['exists']:
                print(f"[SKIP] {table_name}: Table not found")
                results.append(QAResult(
                    check_name=f"data_quality.table_missing.{table_name}",
                    status="FAIL",
                    risk_level="HIGH",
                    message=f"Cannot check data quality - table {table_name} not found",
                    details={"database": "dbcp", "table": table_name}
                ))
                continue
            
            actual_table_name = table_info['actual_name']
            
            # Get total row count first
            try:
                count_result = db_manager.execute_performance_query_safe(
                    'dbcp', table_name, 'SELECT COUNT(*) as total_rows FROM {table}', 
                    f"Row count for {actual_table_name}"
                )
                
                if count_result['status'] != 'success' or not count_result['rows']:
                    print(f"  [ERROR] Cannot get row count for {table_name}")
                    continue
                
                total_rows = count_result['rows'][0]['total_rows']
                print(f"  Total rows: {total_rows:,}")
                
                if total_rows == 0:
                    print(f"  [WARN] Table is empty")
                    results.append(QAResult(
                        check_name=f"data_quality.empty_table.{table_name}",
                        status="WARNING",
                        risk_level="MEDIUM",
                        message=f"Table {table_name} is empty",
                        details={"database": "dbcp", "table": actual_table_name}
                    ))
                    continue
                
                # Check each critical column for NULL percentages
                for column in critical_columns:
                    print(f"    Checking column: {column}")
                    
                    # Build null percentage query
                    null_query = f'''
                    SELECT 
                        COUNT(CASE WHEN {column} IS NULL THEN 1 END) as null_count,
                        COUNT(*) as total_count,
                        ROUND(COUNT(CASE WHEN {column} IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) as null_percentage
                    FROM {{table}}
                    '''
                    
                    try:
                        null_result = db_manager.execute_performance_query_safe(
                            'dbcp', table_name, null_query, f"NULL analysis for {actual_table_name}.{column}"
                        )
                        
                        if null_result['status'] == 'success' and null_result['rows']:
                            data = null_result['rows'][0]
                            null_count = data['null_count']
                            null_percentage = data['null_percentage'] or 0
                            
                            print(f"      NULL values: {null_count:,} ({null_percentage}%)")
                            
                            # Apply original QA thresholds
                            if null_percentage > 50:
                                print(f"      [FAIL] CRITICAL: >50% NULL values")
                                results.append(QAResult(
                                    check_name=f"data_quality.excessive_nulls.{table_name}.{column}",
                                    status="FAIL",
                                    risk_level="CRITICAL",
                                    message=f"Excessive NULL values in {table_name}.{column}: {null_percentage}%",
                                    details={
                                        "database": "dbcp",
                                        "table": actual_table_name,
                                        "column": column,
                                        "null_count": null_count,
                                        "null_percentage": null_percentage,
                                        "total_rows": total_rows
                                    }
                                ))
                            elif null_percentage > 20:
                                print(f"      [WARN] HIGH: >20% NULL values")
                                results.append(QAResult(
                                    check_name=f"data_quality.high_nulls.{table_name}.{column}",
                                    status="WARNING",
                                    risk_level="HIGH",
                                    message=f"High NULL values in {table_name}.{column}: {null_percentage}%",
                                    details={
                                        "database": "dbcp",
                                        "table": actual_table_name,
                                        "column": column,
                                        "null_count": null_count,
                                        "null_percentage": null_percentage,
                                        "total_rows": total_rows
                                    }
                                ))
                            elif null_percentage > 5:
                                print(f"      [WARN] MEDIUM: >5% NULL values")
                                results.append(QAResult(
                                    check_name=f"data_quality.moderate_nulls.{table_name}.{column}",
                                    status="WARNING",
                                    risk_level="MEDIUM",
                                    message=f"Moderate NULL values in {table_name}.{column}: {null_percentage}%",
                                    details={
                                        "database": "dbcp",
                                        "table": actual_table_name,
                                        "column": column,
                                        "null_count": null_count,
                                        "null_percentage": null_percentage,
                                        "total_rows": total_rows
                                    }
                                ))
                            else:
                                print(f"      [PASS] Acceptable NULL levels")
                                results.append(QAResult(
                                    check_name=f"data_quality.acceptable_nulls.{table_name}.{column}",
                                    status="PASS",
                                    risk_level="LOW",
                                    message=f"Acceptable NULL levels in {table_name}.{column}: {null_percentage}%",
                                    details={
                                        "database": "dbcp",
                                        "table": actual_table_name,
                                        "column": column,
                                        "null_percentage": null_percentage,
                                        "total_rows": total_rows
                                    }
                                ))
                        
                        else:
                            print(f"      [ERROR] Failed to check column {column}")
                            results.append(QAResult(
                                check_name=f"data_quality.column_check_failed.{table_name}.{column}",
                                status="ERROR",
                                risk_level="MEDIUM",
                                message=f"Failed to check NULL values in {table_name}.{column}",
                                details={
                                    "database": "dbcp",
                                    "table": actual_table_name,
                                    "column": column,
                                    "error": null_result.get('error')
                                }
                            ))
                    
                    except Exception as e:
                        print(f"      [ERROR] Exception checking {column}: {str(e)[:50]}")
                        results.append(QAResult(
                            check_name=f"data_quality.column_exception.{table_name}.{column}",
                            status="ERROR",
                            risk_level="MEDIUM",
                            message=f"Exception checking {table_name}.{column}: {str(e)[:50]}",
                            details={
                                "database": "dbcp",
                                "table": actual_table_name,
                                "column": column,
                                "error": str(e)
                            }
                        ))
                
                # Additional data quality checks
                print(f"    Running additional quality checks...")
                
                # Check for completely empty rows (all critical columns NULL)
                if len(critical_columns) > 1:
                    empty_rows_query = f'''
                    SELECT COUNT(*) as empty_rows
                    FROM {{table}}
                    WHERE {' AND '.join([f'{col} IS NULL' for col in critical_columns])}
                    '''
                    
                    empty_result = db_manager.execute_performance_query_safe(
                        'dbcp', table_name, empty_rows_query, f"Empty rows check for {actual_table_name}"
                    )
                    
                    if empty_result['status'] == 'success' and empty_result['rows']:
                        empty_rows = empty_result['rows'][0]['empty_rows']
                        empty_pct = (empty_rows / total_rows * 100) if total_rows > 0 else 0
                        
                        if empty_rows > 0:
                            print(f"    [WARN] {empty_rows:,} completely empty rows ({empty_pct:.1f}%)")
                            
                            risk_level = "HIGH" if empty_pct > 10 else "MEDIUM" if empty_pct > 1 else "LOW"
                            
                            results.append(QAResult(
                                check_name=f"data_quality.empty_rows.{table_name}",
                                status="WARNING",
                                risk_level=risk_level,
                                message=f"{empty_rows:,} completely empty rows in {table_name} ({empty_pct:.1f}%)",
                                details={
                                    "database": "dbcp",
                                    "table": actual_table_name,
                                    "empty_rows": empty_rows,
                                    "empty_percentage": empty_pct,
                                    "total_rows": total_rows
                                }
                            ))
                        else:
                            print(f"    [PASS] No completely empty rows")
                            results.append(QAResult(
                                check_name=f"data_quality.no_empty_rows.{table_name}",
                                status="PASS",
                                risk_level="LOW",
                                message=f"No completely empty rows in {table_name}",
                                details={"database": "dbcp", "table": actual_table_name}
                            ))
                
                # Check for data type consistency (numeric columns should not have text)
                numeric_columns = [col for col in critical_columns if col in ['price', 'market_cap', 'volume', 'open', 'high', 'low', 'close']]
                
                for num_col in numeric_columns:
                    # Check if column exists and has reasonable values
                    numeric_check_query = f'''
                    SELECT 
                        MIN({num_col}) as min_val,
                        MAX({num_col}) as max_val,
                        AVG({num_col}) as avg_val,
                        COUNT(CASE WHEN {num_col} < 0 THEN 1 END) as negative_count
                    FROM {{table}}
                    WHERE {num_col} IS NOT NULL
                    '''
                    
                    numeric_result = db_manager.execute_performance_query_safe(
                        'dbcp', table_name, numeric_check_query, f"Numeric validation for {actual_table_name}.{num_col}"
                    )
                    
                    if numeric_result['status'] == 'success' and numeric_result['rows']:
                        data = numeric_result['rows'][0]
                        min_val = data['min_val']
                        max_val = data['max_val']
                        avg_val = data['avg_val']
                        negative_count = data['negative_count']
                        
                        # Check for negative values in price/market_cap (usually invalid)
                        if num_col in ['price', 'market_cap'] and negative_count > 0:
                            print(f"    [WARN] {num_col}: {negative_count:,} negative values")
                            results.append(QAResult(
                                check_name=f"data_quality.negative_values.{table_name}.{num_col}",
                                status="WARNING",
                                risk_level="MEDIUM",
                                message=f"Negative values in {table_name}.{num_col}: {negative_count:,}",
                                details={
                                    "database": "dbcp",
                                    "table": actual_table_name,
                                    "column": num_col,
                                    "negative_count": negative_count,
                                    "min_value": min_val
                                }
                            ))
                        
                        # Check for extreme values (potential outliers)
                        if max_val and avg_val and max_val > avg_val * 1000:
                            print(f"    [INFO] {num_col}: Potential outliers detected (max: {max_val:.2e}, avg: {avg_val:.2e})")
                            results.append(QAResult(
                                check_name=f"data_quality.outliers.{table_name}.{num_col}",
                                status="WARNING",
                                risk_level="LOW",
                                message=f"Potential outliers in {table_name}.{num_col}",
                                details={
                                    "database": "dbcp",
                                    "table": actual_table_name,
                                    "column": num_col,
                                    "max_value": max_val,
                                    "avg_value": avg_val,
                                    "outlier_ratio": max_val / avg_val if avg_val > 0 else None
                                }
                            ))
                
            except Exception as e:
                print(f"  [ERROR] Exception analyzing {table_name}: {str(e)[:100]}")
                results.append(QAResult(
                    check_name=f"data_quality.table_exception.{table_name}",
                    status="ERROR",
                    risk_level="MEDIUM",
                    message=f"Exception during data quality analysis: {str(e)[:100]}",
                    details={"database": "dbcp", "table": table_name, "error": str(e)}
                ))
            
            print()  # Blank line between tables
        
        # Summary
        print("=== DATA QUALITY SUMMARY ===")
        total_checks = len(results)
        passed = len([r for r in results if r.status == "PASS"])
        failed = len([r for r in results if r.status == "FAIL"])
        warnings = len([r for r in results if r.status == "WARNING"])
        errors = len([r for r in results if r.status == "ERROR"])
        
        critical_issues = len([r for r in results if r.risk_level == "CRITICAL"])
        high_issues = len([r for r in results if r.risk_level == "HIGH"])
        
        print(f"Total Checks: {total_checks}")
        print(f"Passed: {passed}, Failed: {failed}, Warnings: {warnings}, Errors: {errors}")
        print(f"Critical Issues: {critical_issues}, High Risk Issues: {high_issues}")
        
        # Overall assessment
        if critical_issues > 0:
            print("Overall Status: CRITICAL - Data corruption detected")
        elif failed > 0 or high_issues > 0:
            print("Overall Status: ISSUES - Significant data quality problems")
        elif warnings > 0:
            print("Overall Status: ACCEPTABLE - Minor data quality issues")
        else:
            print("Overall Status: EXCELLENT - High data quality confirmed")
        
        # Send Telegram notification
        try:
            notification_system = NotificationSystem(config)
            notification_system.send_individual_test_alert(
                test_name="Data Quality Analysis",
                results=results,
                test_type="data_quality"
            )
        except Exception as e:
            print(f"[WARN] Failed to send Telegram notification: {e}")
        
        return results
        
    except Exception as e:
        print(f"[ERROR] Data quality test failed: {e}")
        import traceback
        traceback.print_exc()
        error_result = [QAResult(
            check_name="data_quality.test_failed",
            status="ERROR",
            risk_level="CRITICAL",
            message=f"Data quality test failed: {str(e)[:100]}",
            details={"error": str(e)}
        )]
        
        # Send error notification
        try:
            config = QAConfig()
            notification_system = NotificationSystem(config)
            notification_system.send_individual_test_alert(
                test_name="Data Quality Analysis", 
                results=error_result,
                test_type="data_quality"
            )
        except:
            pass
        
        return error_result

if __name__ == "__main__":
    print("Individual Data Quality Test")
    print("Checking for excessive NULL values and data integrity issues")
    print()
    
    results = test_data_quality()
    
    print(f"\nTest completed with {len(results)} checks performed.")
    print("Review results above for detailed findings.")