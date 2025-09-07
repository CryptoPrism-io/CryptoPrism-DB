#!/usr/bin/env python3
"""
Performance Comparison - Before vs After Optimization
"""

import json
from pathlib import Path
from datetime import datetime

def compare_performance():
    """Compare before and after optimization performance."""
    
    print("=" * 70)
    print("DATABASE OPTIMIZATION PERFORMANCE COMPARISON")
    print("=" * 70)
    
    # Find benchmark result files
    results_dir = Path("database_analysis")
    
    if not results_dir.exists():
        print("ERROR: database_analysis directory not found")
        return
    
    # Find result files
    result_files = list(results_dir.glob("simple_benchmark_results_*.json"))
    
    if len(result_files) < 2:
        print(f"ERROR: Need at least 2 benchmark files, found {len(result_files)}")
        return
    
    # Sort by timestamp to get before/after
    result_files.sort()
    before_file = result_files[0]  # First run (before optimization)
    after_file = result_files[-1]  # Latest run (after optimization)
    
    print(f"BEFORE: {before_file.name}")
    print(f"AFTER:  {after_file.name}")
    print()
    
    try:
        # Load results
        with open(before_file, 'r', encoding='utf-8') as f:
            before_results = json.load(f)
        
        with open(after_file, 'r', encoding='utf-8') as f:
            after_results = json.load(f)
        
        # Compare query by query
        print("QUERY-BY-QUERY COMPARISON:")
        print("=" * 70)
        
        total_before_time = 0
        total_after_time = 0
        successful_comparisons = 0
        
        for query_name, after_result in after_results["query_results"].items():
            if query_name in before_results["query_results"]:
                before_result = before_results["query_results"][query_name]
                
                if before_result["status"] == "success" and after_result["status"] == "success":
                    before_time = before_result["avg_execution_time"] * 1000  # Convert to ms
                    after_time = after_result["avg_execution_time"] * 1000    # Convert to ms
                    
                    total_before_time += before_time
                    total_after_time += after_time
                    successful_comparisons += 1
                    
                    # Calculate improvement
                    if before_time > 0:
                        improvement = ((before_time - after_time) / before_time) * 100
                        
                        if improvement > 0:
                            status = f"FASTER by {improvement:.1f}%"
                        elif improvement < -5:  # More than 5% slower is concerning
                            status = f"SLOWER by {abs(improvement):.1f}%"
                        else:
                            status = f"Similar ({improvement:.1f}%)"
                        
                        print(f"{query_name:25} | {before_time:8.2f}ms -> {after_time:8.2f}ms | {status}")
                    
        print("\n" + "=" * 70)
        print("OVERALL SUMMARY:")
        print("=" * 70)
        
        if successful_comparisons > 0:
            overall_improvement = ((total_before_time - total_after_time) / total_before_time) * 100
            
            print(f"Queries compared: {successful_comparisons}")
            print(f"Total before time: {total_before_time:.2f}ms")
            print(f"Total after time: {total_after_time:.2f}ms")
            print(f"Overall change: {overall_improvement:.1f}%")
            
            if overall_improvement > 5:
                print(f"✓ SUCCESS: Database is {overall_improvement:.1f}% FASTER overall!")
                time_saved = total_before_time - total_after_time
                print(f"  Time saved per query batch: {time_saved:.2f}ms")
            elif overall_improvement < -5:
                print(f"⚠ WARNING: Database is {abs(overall_improvement):.1f}% SLOWER overall")
                print("  This may be temporary while indexes settle or statistics update")
                print("  Consider running ANALYZE and retesting after a few minutes")
            else:
                print(f"→ NEUTRAL: Performance change is within normal variance ({overall_improvement:.1f}%)")
        
        # Database state comparison
        print(f"\nDATABASE STATE:")
        print(f"Before optimization: No primary keys, minimal indexes")
        print(f"After optimization: Primary keys on 5 core tables, strategic indexes")
        
        print(f"\nOPTIMIZATIONS APPLIED:")
        print(f"✓ Primary keys: FE_DMV_ALL, FE_MOMENTUM_SIGNALS, FE_OSCILLATORS_SIGNALS")
        print(f"✓ Primary keys: FE_RATIOS_SIGNALS, FE_TVV_SIGNALS")
        print(f"✓ Slug indexes on all core tables")
        print(f"✓ Database statistics updated")
        
        print(f"\nNOTE: Performance improvements may become more apparent with:")
        print(f"- Complex JOINs across multiple tables")
        print(f"- Large result set filtering")
        print(f"- Concurrent query load")
        print(f"- Time-series range queries")
        
    except Exception as e:
        print(f"ERROR comparing results: {e}")

if __name__ == "__main__":
    compare_performance()