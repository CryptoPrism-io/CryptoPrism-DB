#!/usr/bin/env python3
"""
Business Logic Validation Test - Individual QA Module
Tests business-specific validation rules and requirements.
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

def test_business_logic():
    """Test business logic validation rules specific to CryptoPrism-DB."""
    print("=== Business Logic Validation Test ===")
    print("Checking business rules and requirements...")
    print()
    
    results = []
    
    try:
        config = QAConfig()
        db_manager = DatabaseManager(config)
        
        # Focus on key tables with business requirements
        key_tables = ['FE_DMV_ALL', 'FE_DMV_SCORES', '1K_coins_ohlcv', 'crypto_listings_latest_1000']
        
        for table_name in key_tables:
            print(f"Validating business logic for: {table_name}")
            
            # Check if table exists
            table_info = db_manager.table_exists('dbcp', table_name)
            if not table_info['exists']:
                print(f"[SKIP] {table_name}: Table not found")
                results.append(QAResult(
                    check_name=f"business_logic.table_missing.{table_name}",
                    status="FAIL",
                    risk_level="HIGH",
                    message=f"Cannot validate business logic - table {table_name} not found",
                    details={"database": "dbcp", "table": table_name}
                ))
                continue
            
            actual_table_name = table_info['actual_name']
            
            try:
                # Get basic table info
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
                    print(f"  [FAIL] Critical table {table_name} is empty")
                    results.append(QAResult(
                        check_name=f"business_logic.empty_critical_table.{table_name}",
                        status="FAIL",
                        risk_level="CRITICAL",
                        message=f"Critical business table {table_name} is empty",
                        details={"database": "dbcp", "table": actual_table_name}
                    ))
                    continue
                
                # Business Logic Test 1: FE Tables Minimum Row Requirements
                if table_name.startswith('FE_'):
                    min_expected_rows = 100  # Minimum coins expected in FE tables
                    if total_rows < min_expected_rows:
                        print(f"  [WARN] FE table has insufficient data ({total_rows:,} < {min_expected_rows})")
                        results.append(QAResult(
                            check_name=f"business_logic.fe_min_rows.{table_name}",
                            status="WARNING",
                            risk_level="MEDIUM",
                            message=f"FE table has insufficient rows: {total_rows:,} < {min_expected_rows}",
                            details={
                                "database": "dbcp",
                                "table": actual_table_name,
                                "actual_rows": total_rows,
                                "minimum_expected": min_expected_rows
                            }
                        ))
                    else:
                        print(f"  [PASS] FE table meets minimum row requirement")
                        results.append(QAResult(
                            check_name=f"business_logic.fe_min_rows.{table_name}",
                            status="PASS",
                            risk_level="LOW",
                            message=f"FE table meets row requirements: {total_rows:,} rows",
                            details={"database": "dbcp", "table": actual_table_name, "rows": total_rows}
                        ))
                
                # Business Logic Test 2: OHLCV Data Completeness
                if table_name == '1K_coins_ohlcv':
                    ohlcv_completeness_query = '''
                    SELECT 
                        COUNT(CASE WHEN open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL OR volume IS NULL THEN 1 END) as incomplete_ohlcv,
                        COUNT(*) as total_rows
                    FROM {table}
                    '''
                    
                    ohlcv_result = db_manager.execute_performance_query_safe(
                        'dbcp', table_name, ohlcv_completeness_query, f"OHLCV completeness for {actual_table_name}"
                    )
                    
                    if ohlcv_result['status'] == 'success' and ohlcv_result['rows']:
                        incomplete_count = ohlcv_result['rows'][0]['incomplete_ohlcv']
                        incomplete_pct = (incomplete_count / total_rows * 100) if total_rows > 0 else 0
                        
                        if incomplete_count > 0:
                            print(f"  [WARN] {incomplete_count:,} incomplete OHLCV records ({incomplete_pct:.1f}%)")
                            risk_level = "HIGH" if incomplete_pct > 10 else "MEDIUM"
                            results.append(QAResult(
                                check_name=f"business_logic.ohlcv_completeness.{table_name}",
                                status="WARNING",
                                risk_level=risk_level,
                                message=f"Incomplete OHLCV data: {incomplete_count:,} records ({incomplete_pct:.1f}%)",
                                details={
                                    "database": "dbcp",
                                    "table": actual_table_name,
                                    "incomplete_records": incomplete_count,
                                    "incomplete_percentage": incomplete_pct
                                }
                            ))
                        else:
                            print(f"  [PASS] All OHLCV records complete")
                            results.append(QAResult(
                                check_name=f"business_logic.ohlcv_completeness.{table_name}",
                                status="PASS",
                                risk_level="LOW",
                                message="All OHLCV records are complete",
                                details={"database": "dbcp", "table": actual_table_name}
                            ))
                
                # Business Logic Test 3: DMV Score Validation
                if table_name == 'FE_DMV_SCORES':
                    score_validation_query = '''
                    SELECT 
                        COUNT(CASE WHEN durability_score < 0 OR durability_score > 100 THEN 1 END) as invalid_durability,
                        COUNT(CASE WHEN momentum_score < 0 OR momentum_score > 100 THEN 1 END) as invalid_momentum,
                        COUNT(CASE WHEN valuation_score < 0 OR valuation_score > 100 THEN 1 END) as invalid_valuation,
                        COUNT(*) as total_rows
                    FROM {table}
                    WHERE durability_score IS NOT NULL AND momentum_score IS NOT NULL AND valuation_score IS NOT NULL
                    '''
                    
                    score_result = db_manager.execute_performance_query_safe(
                        'dbcp', table_name, score_validation_query, f"DMV score validation for {actual_table_name}"
                    )
                    
                    if score_result['status'] == 'success' and score_result['rows']:
                        data = score_result['rows'][0]
                        invalid_scores = data['invalid_durability'] + data['invalid_momentum'] + data['invalid_valuation']
                        
                        if invalid_scores > 0:
                            print(f"  [FAIL] {invalid_scores} invalid DMV scores (outside 0-100 range)")
                            results.append(QAResult(
                                check_name=f"business_logic.dmv_score_range.{table_name}",
                                status="FAIL",
                                risk_level="HIGH",
                                message=f"Invalid DMV scores detected: {invalid_scores} scores outside 0-100 range",
                                details={
                                    "database": "dbcp",
                                    "table": actual_table_name,
                                    "invalid_durability": data['invalid_durability'],
                                    "invalid_momentum": data['invalid_momentum'],
                                    "invalid_valuation": data['invalid_valuation']
                                }
                            ))
                        else:
                            print(f"  [PASS] All DMV scores within valid range (0-100)")
                            results.append(QAResult(
                                check_name=f"business_logic.dmv_score_range.{table_name}",
                                status="PASS",
                                risk_level="LOW",
                                message="All DMV scores within valid range",
                                details={"database": "dbcp", "table": actual_table_name}
                            ))
                
                # Business Logic Test 4: Market Cap Reasonableness
                if 'market_cap' in ['price', 'market_cap']:  # Tables with market cap data
                    market_cap_query = '''
                    SELECT 
                        COUNT(CASE WHEN market_cap <= 0 THEN 1 END) as zero_market_cap,
                        COUNT(CASE WHEN market_cap > 1000000000000 THEN 1 END) as extreme_market_cap,
                        AVG(market_cap) as avg_market_cap,
                        COUNT(*) as total_rows
                    FROM {table}
                    WHERE market_cap IS NOT NULL
                    '''
                    
                    mc_result = db_manager.execute_performance_query_safe(
                        'dbcp', table_name, market_cap_query, f"Market cap validation for {actual_table_name}"
                    )
                    
                    if mc_result['status'] == 'success' and mc_result['rows']:
                        data = mc_result['rows'][0]
                        zero_mc = data['zero_market_cap']
                        extreme_mc = data['extreme_market_cap']
                        
                        if zero_mc > 0:
                            print(f"  [WARN] {zero_mc:,} coins with zero/negative market cap")
                            results.append(QAResult(
                                check_name=f"business_logic.market_cap_validation.{table_name}",
                                status="WARNING",
                                risk_level="MEDIUM",
                                message=f"{zero_mc:,} coins with invalid market cap (≤0)",
                                details={"database": "dbcp", "table": actual_table_name, "zero_market_cap": zero_mc}
                            ))
                        
                        if extreme_mc > 0:
                            print(f"  [INFO] {extreme_mc:,} coins with extreme market cap (>$1T)")
                            results.append(QAResult(
                                check_name=f"business_logic.extreme_market_cap.{table_name}",
                                status="WARNING",
                                risk_level="LOW",
                                message=f"{extreme_mc:,} coins with extreme market cap (>$1T)",
                                details={"database": "dbcp", "table": actual_table_name, "extreme_market_cap": extreme_mc}
                            ))
                
                # Business Logic Test 5: Symbol and Slug Consistency
                symbol_slug_query = '''
                SELECT 
                    COUNT(CASE WHEN slug IS NULL OR slug = '' THEN 1 END) as empty_slugs,
                    COUNT(CASE WHEN symbol IS NULL OR symbol = '' THEN 1 END) as empty_symbols,
                    COUNT(DISTINCT slug) as unique_slugs,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    COUNT(*) as total_rows
                FROM {table}
                '''
                
                symbol_result = db_manager.execute_performance_query_safe(
                    'dbcp', table_name, symbol_slug_query, f"Symbol/slug validation for {actual_table_name}"
                )
                
                if symbol_result['status'] == 'success' and symbol_result['rows']:
                    data = symbol_result['rows'][0]
                    empty_slugs = data['empty_slugs']
                    empty_symbols = data['empty_symbols']
                    unique_slugs = data['unique_slugs']
                    unique_symbols = data['unique_symbols']
                    
                    if empty_slugs > 0 or empty_symbols > 0:
                        print(f"  [WARN] Missing identifiers - Slugs: {empty_slugs:,}, Symbols: {empty_symbols:,}")
                        results.append(QAResult(
                            check_name=f"business_logic.missing_identifiers.{table_name}",
                            status="WARNING",
                            risk_level="MEDIUM",
                            message=f"Missing identifiers - Empty slugs: {empty_slugs:,}, Empty symbols: {empty_symbols:,}",
                            details={
                                "database": "dbcp",
                                "table": actual_table_name,
                                "empty_slugs": empty_slugs,
                                "empty_symbols": empty_symbols
                            }
                        ))
                    else:
                        print(f"  [PASS] All records have valid identifiers")
                        results.append(QAResult(
                            check_name=f"business_logic.identifiers_complete.{table_name}",
                            status="PASS",
                            risk_level="LOW",
                            message="All records have valid slug and symbol identifiers",
                            details={
                                "database": "dbcp",
                                "table": actual_table_name,
                                "unique_slugs": unique_slugs,
                                "unique_symbols": unique_symbols
                            }
                        ))
                
            except Exception as e:
                print(f"  [ERROR] Exception validating {table_name}: {str(e)[:100]}")
                results.append(QAResult(
                    check_name=f"business_logic.validation_exception.{table_name}",
                    status="ERROR",
                    risk_level="MEDIUM",
                    message=f"Exception during business logic validation: {str(e)[:100]}",
                    details={"database": "dbcp", "table": table_name, "error": str(e)}
                ))
            
            print()  # Blank line between tables
        
        # Summary
        print("=== BUSINESS LOGIC VALIDATION SUMMARY ===")
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
                print("Overall Status: EXCELLENT - All business rules satisfied")
            else:
                print("Overall Status: GOOD - Minor business rule violations")
        elif failed > 0:
            print("Overall Status: ISSUES - Critical business rule violations detected")
        else:
            print("Overall Status: PARTIAL - Some business rule checks could not complete")
        
        # Send Telegram notification
        try:
            notification_system = NotificationSystem(config)
            notification_system.send_individual_test_alert(
                test_name="Business Logic Validation",
                results=results,
                test_type="business_logic"
            )
        except Exception as e:
            print(f"[WARN] Failed to send Telegram notification: {e}")
        
        return results
        
    except Exception as e:
        print(f"[ERROR] Business logic validation test failed: {e}")
        import traceback
        traceback.print_exc()
        error_result = [QAResult(
            check_name="business_logic.test_failed",
            status="ERROR",
            risk_level="CRITICAL",
            message=f"Business logic validation test failed: {str(e)[:100]}",
            details={"error": str(e)}
        )]
        
        # Send error notification
        try:
            config = QAConfig()
            notification_system = NotificationSystem(config)
            notification_system.send_individual_test_alert(
                test_name="Business Logic Validation",
                results=error_result,
                test_type="business_logic"
            )
        except:
            pass
        
        return error_result

if __name__ == "__main__":
    print("Individual Business Logic Validation Test")
    print("Checking business rules and requirements for CryptoPrism-DB")
    print()
    
    results = test_business_logic()
    
    print(f"\nTest completed with {len(results)} checks performed.")
    print("Review results above for detailed findings.")