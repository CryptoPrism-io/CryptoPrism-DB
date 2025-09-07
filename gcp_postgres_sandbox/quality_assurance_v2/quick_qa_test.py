#!/usr/bin/env python3
"""
Quick QA Test - Schema-Aligned Version
Basic database health and data quality checks using actual database schema.
"""

import sys
from pathlib import Path
from datetime import datetime
from sqlalchemy import text

# Add current directory to path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Go up one level to reach the parent directory containing quality_assurance_v2
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))

try:
    from quality_assurance_v2.core.config import QAConfig
    from quality_assurance_v2.core.database import DatabaseManager
    from quality_assurance_v2.core.base_qa import QAResult
    from quality_assurance_v2.reporting.notification_system import NotificationSystem
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure you run this from the correct directory")
    sys.exit(1)

def run_quick_qa():
    """Run quick QA tests using actual database schema."""
    print("=== CryptoPrism-DB Quick QA Test (Schema-Aligned) ===")
    print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()
    
    results = []
    
    try:
        config = QAConfig()
        db_manager = DatabaseManager(config)
        
        # Test 1: Database Connection
        print("Test 1: Database Connectivity")
        connection_healthy = db_manager.test_connection('dbcp')
        if connection_healthy:
            print("  [PASS] Database connection healthy")
            results.append(QAResult(
                check_name="connectivity.database_health",
                status="PASS",
                risk_level="LOW",
                message="Database connection verified",
                details={"database": "dbcp"}
            ))
        else:
            print("  [FAIL] Database connection failed")
            results.append(QAResult(
                check_name="connectivity.database_health",
                status="FAIL",
                risk_level="CRITICAL",
                message="Database connection failed",
                details={"database": "dbcp"}
            ))
            return results
        
        # Test 2: Table Existence and Row Counts
        print("\nTest 2: Table Health Check")
        key_tables = {
            'FE_DMV_ALL': 'DMV aggregated signals',
            'FE_DMV_SCORES': 'Durability/Momentum/Valuation scores',
            '1K_coins_ohlcv': 'OHLCV historical data',
            'crypto_listings_latest_1000': 'CoinMarketCap listings'
        }
        
        engine = db_manager.engines['dbcp']
        with engine.connect() as conn:
            for table_name, description in key_tables.items():
                try:
                    result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
                    count = result.scalar()
                    print(f"  {table_name}: {count:,} rows - {description}")
                    
                    if count > 0:
                        results.append(QAResult(
                            check_name=f"table_health.row_count.{table_name}",
                            status="PASS",
                            risk_level="LOW",
                            message=f"{table_name} contains {count:,} rows",
                            details={"database": "dbcp", "table": table_name, "row_count": count}
                        ))
                    else:
                        results.append(QAResult(
                            check_name=f"table_health.row_count.{table_name}",
                            status="FAIL",
                            risk_level="HIGH",
                            message=f"{table_name} is empty",
                            details={"database": "dbcp", "table": table_name, "row_count": count}
                        ))
                        
                except Exception as e:
                    print(f"  {table_name}: ERROR - {str(e)[:50]}")
                    results.append(QAResult(
                        check_name=f"table_health.accessibility.{table_name}",
                        status="ERROR",
                        risk_level="MEDIUM",
                        message=f"Cannot access {table_name}: {str(e)[:50]}",
                        details={"database": "dbcp", "table": table_name, "error": str(e)}
                    ))
        
        # Test 3: Basic Data Quality (using actual columns)
        print("\nTest 3: Basic Data Quality")
        
        # FE_DMV_ALL - check for NULL slugs and timestamps
        try:
            with engine.connect() as conn:
                result = conn.execute(text('''
                    SELECT 
                        COUNT(CASE WHEN slug IS NULL THEN 1 END) as null_slugs,
                        COUNT(CASE WHEN timestamp IS NULL THEN 1 END) as null_timestamps,
                        COUNT(*) as total_rows
                    FROM "FE_DMV_ALL"
                '''))
                data = result.fetchone()
                null_slugs, null_timestamps, total_rows = data[0], data[1], data[2]
                
                print(f"  FE_DMV_ALL: {null_slugs} null slugs, {null_timestamps} null timestamps out of {total_rows:,} rows")
                
                if null_slugs == 0 and null_timestamps == 0:
                    results.append(QAResult(
                        check_name="data_quality.fe_dmv_all.critical_columns",
                        status="PASS",
                        risk_level="LOW",
                        message="All critical columns (slug, timestamp) populated in FE_DMV_ALL",
                        details={"database": "dbcp", "table": "FE_DMV_ALL", "total_rows": total_rows}
                    ))
                else:
                    results.append(QAResult(
                        check_name="data_quality.fe_dmv_all.critical_columns",
                        status="FAIL",
                        risk_level="HIGH",
                        message=f"Missing critical data: {null_slugs} null slugs, {null_timestamps} null timestamps",
                        details={"database": "dbcp", "table": "FE_DMV_ALL", "null_slugs": null_slugs, "null_timestamps": null_timestamps}
                    ))
        except Exception as e:
            print(f"  FE_DMV_ALL quality check: ERROR - {str(e)[:50]}")
        
        # Test 4: DMV Scores Validation (using actual column names)
        print("\nTest 4: DMV Scores Validation")
        try:
            with engine.connect() as conn:
                result = conn.execute(text('''
                    SELECT 
                        COUNT(CASE WHEN "Durability_Score" IS NULL THEN 1 END) as null_durability,
                        COUNT(CASE WHEN "Momentum_Score" IS NULL THEN 1 END) as null_momentum,
                        COUNT(CASE WHEN "Valuation_Score" IS NULL THEN 1 END) as null_valuation,
                        COUNT(*) as total_rows,
                        AVG("Durability_Score") as avg_durability,
                        AVG("Momentum_Score") as avg_momentum,
                        AVG("Valuation_Score") as avg_valuation
                    FROM "FE_DMV_SCORES"
                '''))
                data = result.fetchone()
                null_d, null_m, null_v, total, avg_d, avg_m, avg_v = data[0], data[1], data[2], data[3], data[4], data[5], data[6]
                
                print(f"  DMV Scores: D:{null_d} nulls, M:{null_m} nulls, V:{null_v} nulls out of {total:,} rows")
                print(f"  Average Scores: D:{avg_d:.1f}, M:{avg_m:.1f}, V:{avg_v:.1f}")
                
                if null_d + null_m + null_v == 0:
                    results.append(QAResult(
                        check_name="data_quality.dmv_scores.completeness",
                        status="PASS",
                        risk_level="LOW",
                        message="All DMV scores populated",
                        details={"database": "dbcp", "table": "FE_DMV_SCORES", "total_rows": total, "avg_scores": {"D": avg_d, "M": avg_m, "V": avg_v}}
                    ))
                else:
                    results.append(QAResult(
                        check_name="data_quality.dmv_scores.completeness",
                        status="WARNING",
                        risk_level="MEDIUM",
                        message=f"Missing DMV scores: D:{null_d}, M:{null_m}, V:{null_v}",
                        details={"database": "dbcp", "table": "FE_DMV_SCORES", "null_counts": {"D": null_d, "M": null_m, "V": null_v}}
                    ))
        except Exception as e:
            print(f"  DMV Scores validation: ERROR - {str(e)[:50]}")
        
        # Test 5: Duplicate Detection (using actual key columns)
        print("\nTest 5: Duplicate Detection")
        for table in ['FE_DMV_ALL', 'FE_DMV_SCORES']:
            try:
                with engine.connect() as conn:
                    result = conn.execute(text(f'''
                        SELECT COUNT(*) - COUNT(DISTINCT (slug, timestamp)) as duplicates
                        FROM "{table}"
                        WHERE slug IS NOT NULL AND timestamp IS NOT NULL
                    '''))
                    duplicates = result.scalar()
                    
                    print(f"  {table}: {duplicates} duplicate (slug, timestamp) combinations")
                    
                    if duplicates == 0:
                        results.append(QAResult(
                            check_name=f"data_integrity.duplicates.{table}",
                            status="PASS",
                            risk_level="LOW",
                            message=f"No duplicate records in {table}",
                            details={"database": "dbcp", "table": table}
                        ))
                    else:
                        results.append(QAResult(
                            check_name=f"data_integrity.duplicates.{table}",
                            status="FAIL",
                            risk_level="HIGH",
                            message=f"{duplicates} duplicate records found in {table}",
                            details={"database": "dbcp", "table": table, "duplicate_count": duplicates}
                        ))
            except Exception as e:
                print(f"  {table} duplicate check: ERROR - {str(e)[:50]}")
        
        # Calculate summary
        print("\n=== QUICK QA SUMMARY ===")
        total_checks = len(results)
        passed = len([r for r in results if r.status == "PASS"])
        failed = len([r for r in results if r.status == "FAIL"])
        warnings = len([r for r in results if r.status == "WARNING"])
        errors = len([r for r in results if r.status == "ERROR"])
        
        # Calculate health score
        health_score = 0
        if total_checks > 0:
            health_score = (
                passed * 1.0 +
                warnings * 0.7 +
                failed * 0.3 +
                errors * 0.1
            ) / total_checks * 100
        
        print(f"Total Checks: {total_checks}")
        print(f"Passed: {passed}, Failed: {failed}, Warnings: {warnings}, Errors: {errors}")
        print(f"Health Score: {health_score:.1f}/100")
        
        # Overall assessment
        if failed == 0 and errors == 0:
            if warnings == 0:
                print("Overall Status: EXCELLENT - All checks passed")
            else:
                print("Overall Status: GOOD - Minor issues detected")
        elif failed > 0:
            print("Overall Status: ISSUES - Critical problems detected")
        else:
            print("Overall Status: PARTIAL - Some checks could not complete")
        
        # Send Telegram notification
        try:
            notification_system = NotificationSystem(config)
            notification_system.send_individual_test_alert(
                test_name="Quick QA Test (Schema-Aligned)",
                results=results,
                test_type="quick_qa"
            )
            print("\n[INFO] Telegram notification sent")
        except Exception as e:
            print(f"\n[WARN] Failed to send Telegram notification: {e}")
        
        return results, health_score
        
    except Exception as e:
        print(f"[ERROR] Quick QA test failed: {e}")
        import traceback
        traceback.print_exc()
        return [], 0

if __name__ == "__main__":
    print("Running Quick QA Test with Schema-Aligned Checks")
    print()
    
    results, health_score = run_quick_qa()
    
    print(f"\n[COMPLETE] Quick QA finished with {len(results)} checks and {health_score:.1f}/100 health score")