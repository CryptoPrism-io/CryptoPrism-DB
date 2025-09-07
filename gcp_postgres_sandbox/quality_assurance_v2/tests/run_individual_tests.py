#!/usr/bin/env python3
"""
Individual QA Test Runner
Runs all individual QA tests for easier debugging and focused analysis.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add paths for imports - go up one level to reach quality_assurance_v2 root
current_dir = Path(__file__).parent
qa_root = current_dir.parent
sys.path.insert(0, str(qa_root))

try:
    from tests.individual.test_data_quality import test_data_quality
    from tests.individual.test_timestamp_validation import test_timestamp_validation
    from tests.individual.test_duplicate_detection import test_duplicate_detection
    from tests.individual.test_business_logic import test_business_logic
    from quality_assurance_v2.core.config import QAConfig
    from quality_assurance_v2.reporting.notification_system import NotificationSystem
except ImportError:
    try:
        from individual.test_data_quality import test_data_quality
        from individual.test_timestamp_validation import test_timestamp_validation
        from individual.test_duplicate_detection import test_duplicate_detection
        from individual.test_business_logic import test_business_logic
        from core.config import QAConfig
        from reporting.notification_system import NotificationSystem
    except ImportError as e:
        print(f"Import error: {e}")
        sys.exit(1)

def run_all_individual_tests():
    """Run all individual QA tests and compile results."""
    print("=== CryptoPrism-DB Individual QA Tests ===")
    print(f"Execution Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("Running all individual test modules...")
    print("=" * 80)
    
    all_results = []
    test_modules = []
    
    # Test 1: Data Quality Analysis
    print("\n" + "="*60)
    print("RUNNING: Data Quality Test")
    print("="*60)
    try:
        data_quality_results = test_data_quality()
        all_results.extend(data_quality_results)
        test_modules.append(("Data Quality", len(data_quality_results), "COMPLETED"))
        print(f"✓ Data Quality Test completed: {len(data_quality_results)} checks")
    except Exception as e:
        print(f"✗ Data Quality Test failed: {str(e)[:100]}")
        test_modules.append(("Data Quality", 0, "FAILED"))
    
    # Test 2: Timestamp Validation
    print("\n" + "="*60)
    print("RUNNING: Timestamp Validation Test")
    print("="*60)
    try:
        timestamp_results = test_timestamp_validation()
        all_results.extend(timestamp_results)
        test_modules.append(("Timestamp Validation", len(timestamp_results), "COMPLETED"))
        print(f"✓ Timestamp Validation completed: {len(timestamp_results)} checks")
    except Exception as e:
        print(f"✗ Timestamp Validation failed: {str(e)[:100]}")
        test_modules.append(("Timestamp Validation", 0, "FAILED"))
    
    # Test 3: Duplicate Detection
    print("\n" + "="*60)
    print("RUNNING: Duplicate Detection Test")
    print("="*60)
    try:
        duplicate_results = test_duplicate_detection()
        all_results.extend(duplicate_results)
        test_modules.append(("Duplicate Detection", len(duplicate_results), "COMPLETED"))
        print(f"✓ Duplicate Detection completed: {len(duplicate_results)} checks")
    except Exception as e:
        print(f"✗ Duplicate Detection failed: {str(e)[:100]}")
        test_modules.append(("Duplicate Detection", 0, "FAILED"))
    
    # Test 4: Business Logic Validation
    print("\n" + "="*60)
    print("RUNNING: Business Logic Validation Test")
    print("="*60)
    try:
        business_results = test_business_logic()
        all_results.extend(business_results)
        test_modules.append(("Business Logic Validation", len(business_results), "COMPLETED"))
        print(f"✓ Business Logic Validation completed: {len(business_results)} checks")
    except Exception as e:
        print(f"✗ Business Logic Validation failed: {str(e)[:100]}")
        test_modules.append(("Business Logic Validation", 0, "FAILED"))
    
    # Comprehensive Summary
    print("\n" + "=" * 80)
    print("COMPREHENSIVE QA RESULTS SUMMARY")
    print("=" * 80)
    
    # Module Summary
    print("\nTEST MODULE SUMMARY:")
    for module_name, check_count, status in test_modules:
        status_symbol = "✓" if status == "COMPLETED" else "✗"
        print(f"  {status_symbol} {module_name:<25} {check_count:>3} checks  [{status}]")
    
    # Overall Statistics
    total_checks = len(all_results)
    if total_checks > 0:
        status_counts = {'PASS': 0, 'WARNING': 0, 'FAIL': 0, 'ERROR': 0}
        risk_counts = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0, 'CRITICAL': 0}
        
        for result in all_results:
            status_counts[result.status] += 1
            risk_counts[result.risk_level] += 1
        
        # Calculate health score
        health_score = (
            status_counts['PASS'] * 1.0 +
            status_counts['WARNING'] * 0.7 +
            status_counts['FAIL'] * 0.3 +
            status_counts['ERROR'] * 0.1
        ) / total_checks * 100
        
        print(f"\nOVERALL STATISTICS:")
        print(f"  Total Checks Performed: {total_checks}")
        print(f"  Status Distribution:")
        print(f"    ✓ PASS:    {status_counts['PASS']:>3}")
        print(f"    ⚠ WARNING: {status_counts['WARNING']:>3}")
        print(f"    ✗ FAIL:    {status_counts['FAIL']:>3}")
        print(f"    ⚡ ERROR:   {status_counts['ERROR']:>3}")
        
        print(f"  Risk Level Distribution:")
        print(f"    🟢 LOW:      {risk_counts['LOW']:>3}")
        print(f"    🟡 MEDIUM:   {risk_counts['MEDIUM']:>3}")
        print(f"    🟠 HIGH:     {risk_counts['HIGH']:>3}")
        print(f"    🔴 CRITICAL: {risk_counts['CRITICAL']:>3}")
        
        print(f"\n  Database Health Score: {health_score:.1f}/100")
        
        # Overall Assessment
        if risk_counts['CRITICAL'] > 0:
            print("  Overall Status: 🚨 CRITICAL - Immediate attention required")
        elif status_counts['FAIL'] > 0 or risk_counts['HIGH'] > 0:
            print("  Overall Status: ⚠️  ISSUES - Problems detected")
        elif status_counts['WARNING'] > 0:
            print("  Overall Status: ⚡ ACCEPTABLE - Minor issues")
        else:
            print("  Overall Status: ✅ EXCELLENT - All checks passed")
    
    else:
        print("\n  No QA checks completed successfully.")
        health_score = 0
    
    # Send combined summary notification
    try:
        config = QAConfig()
        notification_system = NotificationSystem(config)
        
        # Create execution metadata for summary
        execution_metadata = {
            'total_execution_time': 0,  # Individual tests don't track overall time
            'databases': ['dbcp'],
            'modules_executed': [module[0] for module in test_modules if module[2] == "COMPLETED"],
            'qa_version': 'v2.1.0-INDIVIDUAL',
            'test_approach': 'Individual test modules for focused debugging'
        }
        
        print("\n📤 Sending combined summary notification...")
        notification_success = notification_system.send_qa_summary_alert(
            results=all_results,
            execution_metadata=execution_metadata,
            force_send=True
        )
        
        if notification_success:
            print("✅ Combined summary sent to Telegram")
        else:
            print("⚠️ Failed to send combined summary")
            
    except Exception as e:
        print(f"⚠️ Failed to send combined summary: {e}")
    
    print("\n" + "=" * 80)
    print("Individual QA Test Execution Complete")
    print("All test modules run independently for easier debugging")
    print("=" * 80)
    
    return all_results, health_score

if __name__ == "__main__":
    print("CryptoPrism-DB Individual QA Test Runner")
    print("Each test runs as a separate module for focused debugging")
    print()
    
    results, health_score = run_all_individual_tests()
    
    print(f"\n🎯 FINAL SUMMARY:")
    print(f"   Total Results: {len(results)} QA checks")
    print(f"   Health Score: {health_score:.1f}/100")
    print(f"   Execution completed at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")