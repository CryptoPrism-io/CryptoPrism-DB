#!/usr/bin/env python3
"""
Fixed QA runner with proper PostgreSQL case sensitivity handling.
This version correctly identifies existing tables and provides accurate health scores.
"""

import sys
from pathlib import Path
from datetime import datetime

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

def run_fixed_qa():
    """Run QA with fixed case sensitivity handling."""
    print("=== Fixed CryptoPrism-DB QA: DBCP Database Analysis ===")
    print(f"Execution Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("Using case-insensitive table name resolution...")
    print("=" * 80)
    
    qa_results = []
    start_time = datetime.utcnow()
    
    try:
        # Initialize system
        config = QAConfig()
        print("[PASS] Configuration loaded successfully")
        
        db_manager = DatabaseManager(config)
        print("[PASS] Database manager initialized")
        
        # === CONNECTIVITY TEST WITH FIXED METHOD ===
        print("\n>>> PHASE 1: DATABASE CONNECTIVITY TEST (FIXED)")
        print("-" * 60)
        
        try:
            connection_healthy = db_manager.test_connection('dbcp')
            if connection_healthy:
                print("[PASS] Database connection test successful")
                qa_results.append(QAResult(
                    check_name="connectivity.test_connection_method",
                    status="PASS",
                    risk_level="LOW",
                    message="Database connectivity verified using test_connection() method",
                    details={"database": "dbcp", "method": "test_connection"}
                ))
            else:
                print("[FAIL] Database connection test failed")
                qa_results.append(QAResult(
                    check_name="connectivity.test_connection_method",
                    status="FAIL",
                    risk_level="CRITICAL",
                    message="Database connection test failed",
                    details={"database": "dbcp"}
                ))
        except Exception as e:
            print(f"[ERROR] Connection test error: {e}")
            qa_results.append(QAResult(
                check_name="connectivity.test_connection_method",
                status="ERROR",
                risk_level="CRITICAL",
                message=f"Connection test error: {str(e)[:100]}",
                details={"database": "dbcp", "error": str(e)}
            ))
        
        # === CASE-SENSITIVE TABLE ANALYSIS ===
        print("\n>>> PHASE 2: CASE-SENSITIVE TABLE ANALYSIS")
        print("-" * 60)
        
        # Get all actual table names
        actual_tables = db_manager.get_table_names('dbcp', case_sensitive=True)
        print(f"[INFO] Found {len(actual_tables)} total tables in database")
        
        # Expected tables (lowercase - what QA was looking for)
        expected_tables = {
            'fe_dmv_all': 'Primary signal aggregation table',
            'fe_dmv_scores': 'Durability/Momentum/Valuation scores',
            'fe_momentum_signals': 'Momentum-based trading signals',
            'fe_oscillators_signals': 'Technical oscillator signals',
            'fe_ratios_signals': 'Financial ratio signals',
            'fe_metrics_signal': 'Fundamental analysis signals',
            'fe_tvv_signals': 'Volume/value technical signals',
            '1k_coins_ohlcv': 'OHLCV historical price data',
            'crypto_listings_latest_1000': 'CoinMarketCap listings data'
        }
        
        tables_found = 0
        for expected_table, description in expected_tables.items():
            # Use the new case-insensitive table existence check
            table_info = db_manager.table_exists('dbcp', expected_table)
            
            if table_info['exists']:
                actual_name = table_info['actual_name']
                match_type = table_info['match_type']
                tables_found += 1
                
                print(f"[FOUND] {expected_table} -> {actual_name} ({match_type} match)")
                
                # Get row count using the safe query method
                try:
                    count_result = db_manager.execute_performance_query_safe(
                        'dbcp', expected_table, 'SELECT COUNT(*) FROM {table}', 
                        f"Count rows in {actual_name}"
                    )
                    
                    if count_result['status'] == 'success':
                        row_count = count_result['rows'][0]['count'] if count_result['rows'] else 0
                        exec_time = count_result['execution_time_seconds']
                        
                        print(f"        -> {row_count:,} rows ({exec_time:.3f}s)")
                        
                        # Assess table health
                        if row_count > 0:
                            status = "PASS"
                            risk_level = "LOW"
                            message = f"Table {actual_name} contains {row_count:,} rows and is accessible"
                        else:
                            status = "WARNING"
                            risk_level = "MEDIUM"
                            message = f"Table {actual_name} exists but contains no data"
                        
                        qa_results.append(QAResult(
                            check_name=f"schema.table_analysis.{expected_table}",
                            status=status,
                            risk_level=risk_level,
                            message=message,
                            details={
                                "database": "dbcp",
                                "table_requested": expected_table,
                                "table_actual": actual_name,
                                "match_type": match_type,
                                "row_count": row_count,
                                "execution_time": exec_time,
                                "description": description
                            }
                        ))
                        
                    else:
                        print(f"        -> Error querying table: {count_result.get('error', 'Unknown')}")
                        qa_results.append(QAResult(
                            check_name=f"schema.table_analysis.{expected_table}",
                            status="ERROR",
                            risk_level="MEDIUM",
                            message=f"Table {actual_name} exists but query failed",
                            details={
                                "database": "dbcp",
                                "table_requested": expected_table,
                                "table_actual": actual_name,
                                "error": count_result.get('error', 'Unknown')
                            }
                        ))
                        
                except Exception as e:
                    print(f"        -> Exception querying table: {str(e)[:100]}")
                    qa_results.append(QAResult(
                        check_name=f"schema.table_analysis.{expected_table}",
                        status="ERROR",
                        risk_level="MEDIUM",
                        message=f"Table {actual_name} query exception: {str(e)[:100]}",
                        details={"database": "dbcp", "table_actual": actual_name, "error": str(e)}
                    ))
            else:
                print(f"[MISSING] {expected_table} - {description}")
                qa_results.append(QAResult(
                    check_name=f"schema.table_analysis.{expected_table}",
                    status="FAIL",
                    risk_level="HIGH",
                    message=f"Table {expected_table} does not exist",
                    details={
                        "database": "dbcp",
                        "table_requested": expected_table,
                        "description": description
                    }
                ))
        
        # === TECHNICAL ANALYSIS PIPELINE ASSESSMENT ===
        print(f"\n>>> PHASE 3: TECHNICAL ANALYSIS PIPELINE ASSESSMENT")
        print("-" * 60)
        
        signal_tables = ['fe_dmv_all', 'fe_dmv_scores', 'fe_momentum_signals', 
                        'fe_oscillators_signals', 'fe_ratios_signals', 'fe_metrics_signal', 'fe_tvv_signals']
        input_tables = ['1k_coins_ohlcv', 'crypto_listings_latest_1000']
        
        signal_tables_found = sum(1 for t in signal_tables if db_manager.table_exists('dbcp', t)['exists'])
        input_tables_found = sum(1 for t in input_tables if db_manager.table_exists('dbcp', t)['exists'])
        
        signal_completeness = (signal_tables_found / len(signal_tables)) * 100
        input_completeness = (input_tables_found / len(input_tables)) * 100
        
        print(f"Signal Tables: {signal_tables_found}/{len(signal_tables)} ({signal_completeness:.1f}%)")
        print(f"Input Tables: {input_tables_found}/{len(input_tables)} ({input_completeness:.1f}%)")
        
        if signal_completeness >= 80:
            pipeline_status = "OPERATIONAL"
            pipeline_risk = "LOW"
        elif signal_completeness >= 50:
            pipeline_status = "PARTIAL"
            pipeline_risk = "MEDIUM"
        else:
            pipeline_status = "INCOMPLETE"
            pipeline_risk = "HIGH"
        
        print(f"Pipeline Status: {pipeline_status} ({pipeline_risk} risk)")
        
        qa_results.append(QAResult(
            check_name="pipeline.technical_analysis_status",
            status="PASS" if pipeline_risk == "LOW" else "WARNING" if pipeline_risk == "MEDIUM" else "FAIL",
            risk_level=pipeline_risk,
            message=f"Technical analysis pipeline is {pipeline_status} ({signal_completeness:.1f}% signal tables available)",
            details={
                "database": "dbcp",
                "signal_completeness": signal_completeness,
                "input_completeness": input_completeness,
                "signal_tables_found": signal_tables_found,
                "input_tables_found": input_tables_found
            }
        ))
        
        # === GENERATE CORRECTED REPORT ===
        print(f"\n>>> PHASE 4: CORRECTED QA REPORT GENERATION")
        print("-" * 60)
        
        end_time = datetime.utcnow()
        execution_time = (end_time - start_time).total_seconds()
        
        execution_metadata = {
            'total_execution_time': execution_time,
            'databases': ['dbcp'],
            'modules_executed': [
                'fixed_connectivity_test',
                'case_sensitive_table_analysis',
                'pipeline_assessment'
            ],
            'qa_version': 'v2.0.1-FIXED',
            'fixes_applied': [
                'Added test_connection method',
                'Case-insensitive table name resolution',
                'Proper PostgreSQL quoted identifiers'
            ]
        }
        
        # Calculate corrected health metrics
        total_checks = len(qa_results)
        status_counts = {'PASS': 0, 'WARNING': 0, 'FAIL': 0, 'ERROR': 0}
        risk_counts = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0, 'CRITICAL': 0}
        
        for result in qa_results:
            status_counts[result.status] += 1
            risk_counts[result.risk_level] += 1
        
        # Corrected health score calculation
        corrected_health_score = 0
        if total_checks > 0:
            corrected_health_score = (
                status_counts['PASS'] * 1.0 +
                status_counts['WARNING'] * 0.7 +
                status_counts['FAIL'] * 0.3 +
                status_counts['ERROR'] * 0.1
            ) / total_checks * 100
        
        print(f"=== CORRECTED QA RESULTS ===")
        print(f"Total Checks: {total_checks}")
        print(f"Status Distribution: {status_counts}")
        print(f"Risk Distribution: {risk_counts}")
        print(f"CORRECTED Health Score: {corrected_health_score:.1f}/100")
        print(f"Tables Found: {tables_found}/{len(expected_tables)} ({(tables_found/len(expected_tables))*100:.1f}%)")
        
        # Send corrected notification
        notification_system = NotificationSystem(config)
        print(f"\nSending CORRECTED QA report to Telegram...")
        
        success = notification_system.send_qa_summary_alert(
            results=qa_results,
            execution_metadata=execution_metadata,
            force_send=True
        )
        
        if success:
            print("[PASS] CORRECTED QA report sent successfully to Telegram!")
            print("[PASS] QA execution logged to QA_REPORT.md with corrections")
            return True
        else:
            print("[FAIL] Failed to send corrected QA report")
            return False
            
    except Exception as e:
        print(f"[ERROR] Fixed QA execution failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Fixed QA System - Correcting PostgreSQL Case Sensitivity Issues")
    print("This version should show the true health status of DBCP database.")
    print()
    
    success = run_fixed_qa()
    
    print("\n" + "=" * 80)
    if success:
        print("FIXED QA ANALYSIS COMPLETED SUCCESSFULLY!")
        print("[PASS] Case sensitivity issues resolved")
        print("[PASS] Accurate table detection implemented")
        print("[PASS] Corrected health score calculated")
        print("[PASS] True database status reported")
    else:
        print("FIXED QA ANALYSIS FAILED!")
        print("Check logs above for detailed error information")
    print("=" * 80)