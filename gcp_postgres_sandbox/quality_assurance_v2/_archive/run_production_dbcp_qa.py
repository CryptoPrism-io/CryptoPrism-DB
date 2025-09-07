#!/usr/bin/env python3
"""
Production QA execution for DBCP database - comprehensive analysis.
Generates detailed report and sends real notifications.
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

def run_comprehensive_dbcp_qa():
    """Run comprehensive QA analysis for DBCP database."""
    print("=== CryptoPrism-DB Production QA: DBCP Database Comprehensive Analysis ===")
    print(f"Execution Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 80)
    
    qa_results = []
    start_time = datetime.utcnow()
    
    try:
        # Initialize system
        config = QAConfig()
        print("Configuration loaded successfully")
        
        db_manager = DatabaseManager(config)
        print("Database manager initialized")
        
        # === CONNECTIVITY TESTS ===
        print("\n>>> PHASE 1: DATABASE CONNECTIVITY ANALYSIS")
        print("-" * 50)
        
        connectivity_issues = []
        try:
            # Test basic connection
            connection = db_manager.get_connection('dbcp')
            if connection:
                print("[PASS] Database connection established")
                qa_results.append(QAResult(
                    check_name="connectivity.basic_connection",
                    status="PASS",
                    risk_level="LOW",
                    message="DBCP database connection successful",
                    details={"database": "dbcp", "connection_method": "SQLAlchemy"}
                ))
                connection.close()
            else:
                print("[FAIL] Database connection failed")
                qa_results.append(QAResult(
                    check_name="connectivity.basic_connection",
                    status="FAIL",
                    risk_level="CRITICAL",
                    message="DBCP database connection failed",
                    details={"database": "dbcp"}
                ))
                connectivity_issues.append("Basic connection failure")
        except Exception as e:
            error_msg = str(e)[:200]
            print(f"[FAIL] Connection error: {error_msg}")
            qa_results.append(QAResult(
                check_name="connectivity.basic_connection",
                status="ERROR",
                risk_level="CRITICAL",
                message=f"Database connection error: {error_msg}",
                details={"database": "dbcp", "error": str(e)}
            ))
            connectivity_issues.append(f"Connection exception: {error_msg}")
        
        # === SCHEMA ANALYSIS ===
        print("\n>>> PHASE 2: DATABASE SCHEMA ANALYSIS")
        print("-" * 50)
        
        # Core technical analysis tables
        core_tables = {
            'FE_DMV_ALL': 'Primary signal aggregation table',
            'FE_DMV_SCORES': 'Durability/Momentum/Valuation scores',
            'FE_MOMENTUM_SIGNALS': 'Momentum-based trading signals',
            'FE_OSCILLATORS_SIGNALS': 'Technical oscillator signals',
            'FE_RATIOS_SIGNALS': 'Financial ratio signals',
            'FE_METRICS_SIGNAL': 'Fundamental analysis signals',
            'FE_TVV_SIGNALS': 'Volume/value technical signals'
        }
        
        # Input data tables
        input_tables = {
            '1K_coins_ohlcv': 'OHLCV historical price data',
            'crypto_listings_latest_1000': 'CoinMarketCap listings data'
        }
        
        all_tables = {**core_tables, **input_tables}
        table_results = {}
        
        for table, description in all_tables.items():
            try:
                # Test table existence and accessibility
                query = f"SELECT COUNT(*) as row_count FROM {table}"
                result = db_manager.execute_performance_query('dbcp', query, f"Analyze table {table}")
                
                if result['status'] == 'success' and 'rows' in result:
                    row_count = result['rows'][0][0] if result['rows'] else 0
                    exec_time = result['execution_time_seconds']
                    
                    print(f"[PASS] {table}: {row_count:,} rows ({exec_time:.3f}s) - {description}")
                    
                    # Determine risk level based on row count and table type
                    risk_level = "LOW"
                    status = "PASS"
                    
                    if table in core_tables and row_count == 0:
                        risk_level = "HIGH"
                        status = "WARNING"
                    elif row_count == 0:
                        risk_level = "MEDIUM"
                        status = "WARNING"
                    
                    qa_results.append(QAResult(
                        check_name=f"schema.table_analysis.{table}",
                        status=status,
                        risk_level=risk_level,
                        message=f"Table {table} contains {row_count:,} rows",
                        details={
                            "database": "dbcp",
                            "table": table,
                            "row_count": row_count,
                            "execution_time": exec_time,
                            "description": description
                        }
                    ))
                    
                    table_results[table] = {
                        'status': 'exists',
                        'row_count': row_count,
                        'execution_time': exec_time
                    }
                    
                else:
                    print(f"[FAIL] {table}: Query failed - {description}")
                    qa_results.append(QAResult(
                        check_name=f"schema.table_analysis.{table}",
                        status="FAIL",
                        risk_level="HIGH" if table in core_tables else "MEDIUM",
                        message=f"Table {table} query failed or returned no data",
                        details={"database": "dbcp", "table": table, "description": description}
                    ))
                    table_results[table] = {'status': 'query_failed'}
                    
            except Exception as e:
                if "does not exist" in str(e).lower():
                    print(f"[FAIL] {table}: Table does not exist - {description}")
                    qa_results.append(QAResult(
                        check_name=f"schema.table_analysis.{table}",
                        status="FAIL",
                        risk_level="CRITICAL" if table in core_tables else "HIGH",
                        message=f"Table {table} does not exist",
                        details={"database": "dbcp", "table": table, "description": description}
                    ))
                    table_results[table] = {'status': 'missing'}
                else:
                    error_msg = str(e)[:150]
                    print(f"[FAIL] {table}: Error - {error_msg}")
                    qa_results.append(QAResult(
                        check_name=f"schema.table_analysis.{table}",
                        status="ERROR",
                        risk_level="HIGH",
                        message=f"Table {table} analysis error: {error_msg}",
                        details={"database": "dbcp", "table": table, "error": str(e)}
                    ))
                    table_results[table] = {'status': 'error', 'error': str(e)}
        
        # === DATA QUALITY ANALYSIS ===
        print("\n>>> PHASE 3: DATA QUALITY ANALYSIS")
        print("-" * 50)
        
        # Analyze data freshness for key tables
        freshness_checks = ['crypto_listings_latest_1000', '1K_coins_ohlcv']
        
        for table in freshness_checks:
            if table_results.get(table, {}).get('status') == 'exists':
                try:
                    # Check for timestamp columns
                    freshness_query = f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table.lower()}' 
                    AND (column_name LIKE '%time%' OR column_name LIKE '%date%' OR column_name LIKE '%updated%')
                    """
                    
                    result = db_manager.execute_performance_query('dbcp', freshness_query, f"Check freshness columns for {table}")
                    
                    if result['status'] == 'success' and result.get('rows'):
                        timestamp_columns = [row[0] for row in result['rows']]
                        print(f"[PASS] {table}: Found timestamp columns: {', '.join(timestamp_columns)}")
                        
                        qa_results.append(QAResult(
                            check_name=f"data_quality.freshness_check.{table}",
                            status="PASS",
                            risk_level="LOW",
                            message=f"Table {table} has timestamp tracking capabilities",
                            details={"database": "dbcp", "table": table, "timestamp_columns": timestamp_columns}
                        ))
                    else:
                        print(f"[INFO] {table}: No timestamp columns found")
                        qa_results.append(QAResult(
                            check_name=f"data_quality.freshness_check.{table}",
                            status="WARNING",
                            risk_level="MEDIUM",
                            message=f"Table {table} lacks timestamp tracking",
                            details={"database": "dbcp", "table": table}
                        ))
                        
                except Exception as e:
                    print(f"[FAIL] {table}: Freshness check failed - {str(e)[:100]}")
                    qa_results.append(QAResult(
                        check_name=f"data_quality.freshness_check.{table}",
                        status="ERROR",
                        risk_level="LOW",
                        message=f"Unable to check data freshness for {table}",
                        details={"database": "dbcp", "table": table, "error": str(e)}
                    ))
        
        # === PERFORMANCE ANALYSIS ===
        print("\n>>> PHASE 4: DATABASE PERFORMANCE ANALYSIS")
        print("-" * 50)
        
        # Test query performance on existing tables
        performance_queries = []
        
        for table, table_info in table_results.items():
            if table_info.get('status') == 'exists':
                try:
                    # Simple performance test
                    perf_query = f"SELECT COUNT(*) FROM {table}"
                    result = db_manager.execute_performance_query('dbcp', perf_query, f"Performance test {table}")
                    
                    exec_time = result.get('execution_time_seconds', 0)
                    performance_queries.append({
                        'table': table,
                        'execution_time': exec_time,
                        'query_type': 'count'
                    })
                    
                    # Performance assessment
                    if exec_time < 1.0:
                        status = "PASS"
                        risk_level = "LOW"
                        message = f"Table {table} shows good performance ({exec_time:.3f}s)"
                    elif exec_time < 5.0:
                        status = "WARNING"
                        risk_level = "MEDIUM"
                        message = f"Table {table} shows moderate performance ({exec_time:.3f}s)"
                    else:
                        status = "FAIL"
                        risk_level = "HIGH"
                        message = f"Table {table} shows poor performance ({exec_time:.3f}s)"
                    
                    print(f"{'[PASS]' if status == 'PASS' else '[WARN]' if status == 'WARNING' else '[FAIL]'} {table}: {exec_time:.3f}s query time")
                    
                    qa_results.append(QAResult(
                        check_name=f"performance.query_speed.{table}",
                        status=status,
                        risk_level=risk_level,
                        message=message,
                        details={"database": "dbcp", "table": table, "execution_time": exec_time}
                    ))
                    
                except Exception as e:
                    print(f"[FAIL] {table}: Performance test failed")
                    qa_results.append(QAResult(
                        check_name=f"performance.query_speed.{table}",
                        status="ERROR",
                        risk_level="MEDIUM",
                        message=f"Performance test failed for {table}",
                        details={"database": "dbcp", "table": table, "error": str(e)}
                    ))
        
        # === PIPELINE STATUS ANALYSIS ===
        print("\n>>> PHASE 5: TECHNICAL ANALYSIS PIPELINE STATUS")
        print("-" * 50)
        
        # Assess pipeline completeness
        pipeline_status = {
            'input_data': len([t for t in input_tables.keys() if table_results.get(t, {}).get('status') == 'exists']),
            'signal_tables': len([t for t in core_tables.keys() if table_results.get(t, {}).get('status') == 'exists']),
            'total_input': len(input_tables),
            'total_signals': len(core_tables)
        }
        
        input_completeness = (pipeline_status['input_data'] / pipeline_status['total_input']) * 100
        signal_completeness = (pipeline_status['signal_tables'] / pipeline_status['total_signals']) * 100
        
        print(f"Input Data Pipeline: {pipeline_status['input_data']}/{pipeline_status['total_input']} tables ({input_completeness:.1f}%)")
        print(f"Signal Generation Pipeline: {pipeline_status['signal_tables']}/{pipeline_status['total_signals']} tables ({signal_completeness:.1f}%)")
        
        # Pipeline assessment
        if input_completeness >= 80 and signal_completeness >= 80:
            pipeline_risk = "LOW"
            pipeline_status_text = "OPERATIONAL"
        elif input_completeness >= 50 and signal_completeness >= 50:
            pipeline_risk = "MEDIUM"
            pipeline_status_text = "PARTIAL"
        else:
            pipeline_risk = "CRITICAL"
            pipeline_status_text = "INCOMPLETE"
        
        qa_results.append(QAResult(
            check_name="pipeline.technical_analysis_completeness",
            status="PASS" if pipeline_risk == "LOW" else "WARNING" if pipeline_risk == "MEDIUM" else "FAIL",
            risk_level=pipeline_risk,
            message=f"Technical analysis pipeline is {pipeline_status_text} ({signal_completeness:.1f}% complete)",
            details={
                "database": "dbcp",
                "input_completeness": input_completeness,
                "signal_completeness": signal_completeness,
                "pipeline_status": pipeline_status
            }
        ))
        
        print(f"Overall Pipeline Status: {pipeline_status_text} ({pipeline_risk} RISK)")
        
        # === GENERATE COMPREHENSIVE REPORT ===
        print("\n>>> PHASE 6: REPORT GENERATION AND NOTIFICATION")
        print("-" * 50)
        
        end_time = datetime.utcnow()
        execution_time = (end_time - start_time).total_seconds()
        
        execution_metadata = {
            'total_execution_time': execution_time,
            'databases': ['dbcp'],
            'modules_executed': [
                'connectivity_analysis',
                'schema_analysis', 
                'data_quality_analysis',
                'performance_analysis',
                'pipeline_status_analysis'
            ],
            'execution_timestamp': end_time.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'qa_version': 'v2.0.0',
            'table_analysis_results': table_results,
            'pipeline_status': pipeline_status
        }
        
        # Initialize notification system and send comprehensive report
        notification_system = NotificationSystem(config)
        print("Notification system initialized")
        
        # Generate final summary
        print(f"\n=== COMPREHENSIVE QA SUMMARY ===")
        print(f"Execution Time: {execution_time:.1f} seconds")
        print(f"Total Checks Performed: {len(qa_results)}")
        
        status_counts = {'PASS': 0, 'WARNING': 0, 'FAIL': 0, 'ERROR': 0}
        risk_counts = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0, 'CRITICAL': 0}
        
        for result in qa_results:
            status_counts[result.status] += 1
            risk_counts[result.risk_level] += 1
        
        print(f"Status Distribution: {status_counts}")
        print(f"Risk Distribution: {risk_counts}")
        
        # Calculate health score
        total_checks = len(qa_results)
        health_score = 0
        if total_checks > 0:
            health_score = (
                status_counts['PASS'] * 1.0 +
                status_counts['WARNING'] * 0.7 +
                status_counts['FAIL'] * 0.3 +
                status_counts['ERROR'] * 0.1
            ) / total_checks * 100
        
        print(f"Overall Health Score: {health_score:.1f}/100")
        
        # Send production notification
        print(f"\nSending production QA report to Telegram...")
        success = notification_system.send_qa_summary_alert(
            results=qa_results,
            execution_metadata=execution_metadata,
            force_send=True  # This is a production report
        )
        
        if success:
            print("[PASS] Production QA report sent successfully to Telegram!")
            print("[PASS] QA execution logged to QA_REPORT.md")
            return True
        else:
            print("[FAIL] Failed to send production QA report")
            return False
            
    except Exception as e:
        print(f"[FAIL] Production QA execution failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Starting Production QA Analysis for DBCP Database")
    print("This will generate a real report and send actual notifications.")
    print()
    
    success = run_comprehensive_dbcp_qa()
    
    print("\n" + "=" * 80)
    if success:
        print("PRODUCTION QA COMPLETED SUCCESSFULLY!")
        print("[PASS] Comprehensive analysis performed")
        print("[PASS] Real Telegram notification sent")
        print("[PASS] QA_REPORT.md updated with detailed findings")
        print("[PASS] System awareness updated")
    else:
        print("PRODUCTION QA FAILED!")
        print("Check logs above for detailed error information")
    print("=" * 80)