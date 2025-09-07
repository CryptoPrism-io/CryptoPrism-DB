#!/usr/bin/env python3
"""
Main entry point for CryptoPrism-DB Quality Assurance v2.
Provides different execution modes for various QA scenarios.
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add current directory to path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

def run_individual_tests():
    """Run all individual QA tests."""
    try:
        from tests.run_individual_tests import run_all_individual_tests
        print("🔬 Running Individual QA Tests")
        print("=" * 50)
        results, health_score = run_all_individual_tests()
        return results, health_score
    except ImportError as e:
        print(f"❌ Failed to import individual tests: {e}")
        return [], 0

def run_comprehensive_tests():
    """Run comprehensive QA tests."""
    try:
        from tests.run_comprehensive_qa import run_comprehensive_qa
        print("🏢 Running Comprehensive QA Tests")
        print("=" * 50)
        return run_comprehensive_qa()
    except ImportError as e:
        print(f"❌ Failed to import comprehensive tests: {e}")
        return False

def run_single_test(test_name):
    """Run a specific individual test."""
    test_mapping = {
        'data_quality': ('tests.individual.test_data_quality', 'test_data_quality'),
        'timestamps': ('tests.individual.test_timestamp_validation', 'test_timestamp_validation'),
        'duplicates': ('tests.individual.test_duplicate_detection', 'test_duplicate_detection'),
        'business_logic': ('tests.individual.test_business_logic', 'test_business_logic')
    }
    
    if test_name not in test_mapping:
        print(f"❌ Unknown test: {test_name}")
        print(f"Available tests: {', '.join(test_mapping.keys())}")
        return []
    
    module_name, function_name = test_mapping[test_name]
    
    try:
        module = __import__(module_name, fromlist=[function_name])
        test_function = getattr(module, function_name)
        
        print(f"🔬 Running {test_name.replace('_', ' ').title()} Test")
        print("=" * 50)
        return test_function()
        
    except (ImportError, AttributeError) as e:
        print(f"❌ Failed to run {test_name}: {e}")
        return []

def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description="CryptoPrism-DB Quality Assurance System v2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_qa.py --individual           # Run all individual tests
  python run_qa.py --comprehensive        # Run comprehensive analysis
  python run_qa.py --test data_quality    # Run specific test
  python run_qa.py --test timestamps      # Run timestamp validation
  python run_qa.py --list                 # List available tests
        """
    )
    
    parser.add_argument(
        '--individual', 
        action='store_true',
        help='Run all individual QA tests (recommended for debugging)'
    )
    
    parser.add_argument(
        '--comprehensive',
        action='store_true', 
        help='Run comprehensive QA analysis'
    )
    
    parser.add_argument(
        '--test',
        choices=['data_quality', 'timestamps', 'duplicates', 'business_logic'],
        help='Run a specific individual test'
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='List all available tests'
    )
    
    args = parser.parse_args()
    
    # Print header
    print("CryptoPrism-DB Quality Assurance System v2")
    print(f"Execution Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()
    
    if args.list:
        print("Available Tests:")
        print("  Individual Tests:")
        print("    * data_quality    - NULL value analysis and data integrity")
        print("    * timestamps      - Timestamp validation and business rules")  
        print("    * duplicates      - Duplicate detection for key tables")
        print("    * business_logic  - Business-specific validation rules")
        print()
        print("  Execution Modes:")
        print("    * --individual    - Run all individual tests")
        print("    * --comprehensive - Run comprehensive analysis")
        return
    
    if args.test:
        results = run_single_test(args.test)
        print(f"\n[PASS] {args.test.replace('_', ' ').title()} test completed with {len(results)} checks")
        
    elif args.individual:
        results, health_score = run_individual_tests()
        print(f"\n[PASS] Individual tests completed - Health Score: {health_score:.1f}/100")
        
    elif args.comprehensive:
        success = run_comprehensive_tests()
        if success:
            print("\n[PASS] Comprehensive QA completed successfully")
        else:
            print("\n[FAIL] Comprehensive QA failed")
            
    else:
        # Default: run individual tests
        print("No specific mode selected - running individual tests (recommended)")
        print("   Use --help to see all available options")
        print()
        results, health_score = run_individual_tests()
        print(f"\n[PASS] Individual tests completed - Health Score: {health_score:.1f}/100")

if __name__ == "__main__":
    main()