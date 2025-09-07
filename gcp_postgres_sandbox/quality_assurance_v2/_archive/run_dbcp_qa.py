#!/usr/bin/env python3
"""
Run QA specifically for dbcp database with timeout handling.
"""

import sys
from pathlib import Path
import signal
from contextlib import contextmanager

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

@contextmanager
def timeout(seconds):
    """Context manager for timeout handling."""
    def signal_handler(signum, frame):
        raise TimeoutError(f"Operation timed out after {seconds} seconds")
    
    # Set the signal handler
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    
    try:
        yield
    finally:
        signal.alarm(0)

def run_dbcp_qa():
    """Run QA checks for dbcp database with timeout protection."""
    print("=== CryptoPrism-DB QA v2: DBCP Database Analysis ===")
    
    qa_results = []
    
    try:
        # Initialize configuration
        config = QAConfig()
        print("Configuration loaded successfully")
        
        # Test database connectivity first
        db_manager = DatabaseManager(config)
        print("Database manager initialized")
        
        # Quick connectivity test with timeout
        try:
            with timeout(30):  # 30 second timeout
                health_status = db_manager.test_connection('dbcp')
                if health_status:
                    print("DBCP database connection successful")
                    qa_results.append(QAResult(
                        check_name="database.connectivity_test",
                        status="PASS",
                        risk_level="LOW",
                        message="DBCP database connection successful",
                        details={"database": "dbcp", "response_time": "< 30s"}
                    ))
                else:
                    print("DBCP database connection failed")
                    qa_results.append(QAResult(
                        check_name="database.connectivity_test",
                        status="FAIL",
                        risk_level="CRITICAL",
                        message="DBCP database connection failed",
                        details={"database": "dbcp"}
                    ))
        except TimeoutError:
            print("Database connection test timed out (>30s)")
            qa_results.append(QAResult(
                check_name="database.connectivity_test",
                status="FAIL",
                risk_level="HIGH",
                message="Database connection timed out (>30 seconds)",
                details={"database": "dbcp", "timeout": "30s"}
            ))
        
        # Test key tables existence
        key_tables = [
            'FE_DMV_ALL', 'FE_DMV_SCORES', 'FE_MOMENTUM_SIGNALS',
            '1K_coins_ohlcv', 'crypto_listings_latest_1000'
        ]
        
        for table in key_tables:
            try:
                with timeout(15):  # 15 second timeout per table
                    query = f"SELECT COUNT(*) FROM {table} LIMIT 1"
                    result = db_manager.execute_performance_query('dbcp', query, f"Check table {table}")
                    
                    if result['status'] == 'success':
                        print(f"Table {table} accessible")
                        qa_results.append(QAResult(
                            check_name=f"schema.table_accessibility.{table}",
                            status="PASS",
                            risk_level="LOW",
                            message=f"Table {table} is accessible and queryable",
                            details={"database": "dbcp", "table": table, "execution_time": result['execution_time_seconds']}
                        ))
                    else:
                        print(f" Table {table} query failed")
                        qa_results.append(QAResult(
                            check_name=f"schema.table_accessibility.{table}",
                            status="FAIL",
                            risk_level="MEDIUM",
                            message=f"Table {table} query failed",
                            details={"database": "dbcp", "table": table}
                        ))
            except TimeoutError:
                print(f" Table {table} query timed out")
                qa_results.append(QAResult(
                    check_name=f"schema.table_accessibility.{table}",
                    status="FAIL",
                    risk_level="HIGH",
                    message=f"Table {table} query timed out (>15s)",
                    details={"database": "dbcp", "table": table, "timeout": "15s"}
                ))
            except Exception as e:
                print(f" Table {table} error: {str(e)[:100]}")
                qa_results.append(QAResult(
                    check_name=f"schema.table_accessibility.{table}",
                    status="ERROR",
                    risk_level="MEDIUM",
                    message=f"Table {table} error: {str(e)[:100]}",
                    details={"database": "dbcp", "table": table}
                ))
        
        # Send QA results via notification
        notification_system = NotificationSystem(config)
        print("Notification system initialized")
        
        execution_metadata = {
            'total_execution_time': 60.0,  # Estimated
            'databases': ['dbcp'],
            'modules_executed': ['connectivity_test', 'table_accessibility']
        }
        
        print(f"\n=== QA Results Summary ===")
        print(f"Total Checks: {len(qa_results)}")
        
        status_counts = {'PASS': 0, 'WARNING': 0, 'FAIL': 0, 'ERROR': 0}
        risk_counts = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0, 'CRITICAL': 0}
        
        for result in qa_results:
            status_counts[result.status] += 1
            risk_counts[result.risk_level] += 1
            print(f"• {result.check_name}: {result.status} ({result.risk_level}) - {result.message}")
        
        print(f"\nStatus Distribution: {status_counts}")
        print(f"Risk Distribution: {risk_counts}")
        
        # Send comprehensive alert
        print(f"\n=== Sending QA Alert to Telegram ===")
        success = notification_system.send_qa_summary_alert(
            results=qa_results,
            execution_metadata=execution_metadata,
            force_send=True
        )
        
        if success:
            print("QA alert sent successfully to Telegram!")
            return True
        else:
            print("Failed to send QA alert")
            return False
            
    except Exception as e:
        print(f"QA execution failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_dbcp_qa()
    if success:
        print("\nDBCP QA completed successfully!")
        print("Check Telegram for the comprehensive QA report with AI analysis.")
    else:
        print("\nDBCP QA failed - check logs above")