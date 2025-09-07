#!/usr/bin/env python3
"""
Performance Analyzer for CryptoPrism-DB Optimization Results

This script compares query performance before and after database optimizations,
calculating time savings, improvement percentages, and ROI metrics. It provides
comprehensive analysis of optimization effectiveness.

Features:
- Compares before/after benchmark results
- Calculates performance improvement percentages
- Identifies most improved queries
- Generates time savings reports
- Provides ROI analysis for optimizations
- Creates detailed comparison reports

Requirements:
- sqlalchemy>=2.0.0
- python-dotenv
"""

import os
import sys
import json
import logging
import statistics
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
# Optional matplotlib import for visualization
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
from collections import defaultdict

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class PerformanceAnalyzer:
    """Analyze database optimization performance improvements."""
    
    def __init__(self):
        """Initialize performance analyzer."""
        self.improvement_thresholds = {
            'excellent': 75,   # >75% improvement
            'very_good': 50,   # 50-75% improvement  
            'good': 25,        # 25-50% improvement
            'moderate': 10,    # 10-25% improvement
            'minimal': 5,      # 5-10% improvement
            'negligible': 0    # <5% improvement
        }
    
    def load_benchmark_results(self, filepath: str) -> Dict[str, Any]:
        """Load benchmark results JSON file."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Benchmark results file not found: {filepath}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in benchmark results file: {e}")
    
    def extract_query_metrics(self, benchmark_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Extract query performance metrics from benchmark data."""
        query_metrics = {}
        
        for db_key, db_benchmark in benchmark_data.get('database_benchmarks', {}).items():
            if 'query_results' not in db_benchmark:
                continue
            
            for query_name, query_result in db_benchmark['query_results'].items():
                if query_result.get('status') != 'success':
                    continue
                
                # Create unique query identifier
                query_id = f"{db_key}.{query_name}"
                
                stats = query_result.get('statistics', {})
                query_metrics[query_id] = {
                    'database': db_key,
                    'query_name': query_name,
                    'description': query_result.get('description', ''),
                    'average_ms': stats.get('average_ms', 0),
                    'median_ms': stats.get('median_ms', 0),
                    'min_ms': stats.get('min_ms', 0),
                    'max_ms': stats.get('max_ms', 0),
                    'std_dev_ms': stats.get('std_dev_ms', 0),
                    'row_count': query_result.get('results_info', {}).get('row_count', 0),
                    'execution_times': query_result.get('execution_times_ms', []),
                    'timestamp': benchmark_data.get('benchmark_timestamp')
                }
        
        return query_metrics
    
    def calculate_improvement_metrics(self, before_metrics: Dict[str, Any], 
                                    after_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate performance improvement metrics between before and after."""
        improvements = {}
        
        # Find common queries between before and after
        common_queries = set(before_metrics.keys()) & set(after_metrics.keys())
        
        for query_id in common_queries:
            before = before_metrics[query_id]
            after = after_metrics[query_id]
            
            # Calculate improvement percentages
            before_avg = before['average_ms']
            after_avg = after['average_ms']
            
            if before_avg > 0:
                time_saved_ms = before_avg - after_avg
                improvement_percent = (time_saved_ms / before_avg) * 100
                speedup_factor = before_avg / after_avg if after_avg > 0 else float('inf')
            else:
                time_saved_ms = 0
                improvement_percent = 0
                speedup_factor = 1
            
            # Categorize improvement level
            improvement_category = self._categorize_improvement(improvement_percent)
            
            # Calculate statistical significance
            statistical_significance = self._calculate_statistical_significance(
                before['execution_times'], after['execution_times']
            )
            
            improvements[query_id] = {
                'database': before['database'],
                'query_name': before['query_name'],
                'description': before['description'],
                'before_avg_ms': round(before_avg, 2),
                'after_avg_ms': round(after_avg, 2),
                'time_saved_ms': round(time_saved_ms, 2),
                'improvement_percent': round(improvement_percent, 2),
                'speedup_factor': round(speedup_factor, 2),
                'improvement_category': improvement_category,
                'statistical_significance': statistical_significance,
                'row_count': before['row_count'],
                'before_std_dev': before['std_dev_ms'],
                'after_std_dev': after['std_dev_ms']
            }
        
        return improvements
    
    def _categorize_improvement(self, improvement_percent: float) -> str:
        """Categorize improvement percentage into performance categories."""
        if improvement_percent >= self.improvement_thresholds['excellent']:
            return 'excellent'
        elif improvement_percent >= self.improvement_thresholds['very_good']:
            return 'very_good'
        elif improvement_percent >= self.improvement_thresholds['good']:
            return 'good'
        elif improvement_percent >= self.improvement_thresholds['moderate']:
            return 'moderate'
        elif improvement_percent >= self.improvement_thresholds['minimal']:
            return 'minimal'
        elif improvement_percent > -5:  # Small regression
            return 'negligible'
        else:
            return 'regression'
    
    def _calculate_statistical_significance(self, before_times: List[float], 
                                          after_times: List[float]) -> Dict[str, Any]:
        """Calculate statistical significance of performance improvement."""
        try:
            if len(before_times) < 2 or len(after_times) < 2:
                return {'significant': False, 'reason': 'insufficient_data'}
            
            # Simple t-test approximation
            before_mean = statistics.mean(before_times)
            after_mean = statistics.mean(after_times)
            before_std = statistics.stdev(before_times)
            after_std = statistics.stdev(after_times)
            
            # Calculate pooled standard error
            n1, n2 = len(before_times), len(after_times)
            pooled_se = ((before_std ** 2 / n1) + (after_std ** 2 / n2)) ** 0.5
            
            if pooled_se > 0:
                t_stat = abs(before_mean - after_mean) / pooled_se
                # Rough significance threshold (t > 2 indicates significance)
                significant = t_stat > 2.0
            else:
                significant = False
                t_stat = 0
            
            return {
                'significant': significant,
                't_statistic': round(t_stat, 3),
                'confidence_level': 'high' if t_stat > 2.5 else 'moderate' if t_stat > 2.0 else 'low'
            }
            
        except Exception as e:
            return {'significant': False, 'reason': f'calculation_error: {str(e)}'}
    
    def generate_improvement_summary(self, improvements: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive improvement summary statistics."""
        if not improvements:
            return {'error': 'No improvement data available'}
        
        # Collect improvement metrics
        improvement_percentages = [imp['improvement_percent'] for imp in improvements.values()]
        time_savings = [imp['time_saved_ms'] for imp in improvements.values()]
        
        # Category counts
        category_counts = defaultdict(int)
        for imp in improvements.values():
            category_counts[imp['improvement_category']] += 1
        
        # Statistical summary
        summary = {
            'total_queries_analyzed': len(improvements),
            'overall_improvement_percent': round(statistics.mean(improvement_percentages), 2),
            'median_improvement_percent': round(statistics.median(improvement_percentages), 2),
            'best_improvement_percent': round(max(improvement_percentages), 2),
            'worst_improvement_percent': round(min(improvement_percentages), 2),
            'total_time_saved_ms': round(sum(time_savings), 2),
            'average_time_saved_ms': round(statistics.mean(time_savings), 2),
            'improvement_categories': dict(category_counts),
            'queries_improved': len([imp for imp in improvements.values() if imp['improvement_percent'] > 0]),
            'queries_regressed': len([imp for imp in improvements.values() if imp['improvement_percent'] < 0]),
            'statistically_significant': len([imp for imp in improvements.values() 
                                            if imp['statistical_significance'].get('significant', False)])
        }
        
        # Calculate success rate
        summary['success_rate_percent'] = round((summary['queries_improved'] / summary['total_queries_analyzed']) * 100, 2)
        
        return summary
    
    def identify_top_improvements(self, improvements: Dict[str, Any], limit: int = 10) -> List[Dict[str, Any]]:
        """Identify queries with the most significant improvements."""
        sorted_improvements = sorted(
            improvements.values(),
            key=lambda x: x['improvement_percent'],
            reverse=True
        )
        
        return sorted_improvements[:limit]
    
    def identify_performance_regressions(self, improvements: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify queries that performed worse after optimization."""
        regressions = [
            imp for imp in improvements.values()
            if imp['improvement_percent'] < -5  # More than 5% slower
        ]
        
        return sorted(regressions, key=lambda x: x['improvement_percent'])
    
    def calculate_roi_metrics(self, improvements: Dict[str, Any], 
                            optimization_time_hours: float = 8) -> Dict[str, Any]:
        """Calculate Return on Investment metrics for optimization effort."""
        summary = self.generate_improvement_summary(improvements)
        
        if 'error' in summary:
            return summary
        
        # Estimate query frequency (daily executions)
        # This could be enhanced with actual query frequency data
        estimated_daily_queries = {
            'join_': 100,      # JOIN queries run frequently
            'filter_': 200,    # Filtering queries are very common  
            'aggregate_': 50,  # Aggregation queries are less frequent
            'range_': 150,     # Range queries are common for analytics
            'top_': 25,        # Complex analytics queries
            'market_': 10      # Market overview queries
        }
        
        # Calculate daily time savings
        daily_time_saved_ms = 0
        for query_id, imp in improvements.items():
            query_type = query_id.split('.')[-1]  # Get query name part
            
            # Estimate frequency based on query type
            frequency = 10  # Default frequency
            for pattern, freq in estimated_daily_queries.items():
                if pattern in query_type:
                    frequency = freq
                    break
            
            daily_time_saved_ms += imp['time_saved_ms'] * frequency
        
        # Convert to more meaningful units
        daily_time_saved_seconds = daily_time_saved_ms / 1000
        daily_time_saved_minutes = daily_time_saved_seconds / 60
        yearly_time_saved_hours = (daily_time_saved_minutes * 365) / 60
        
        # ROI calculation
        optimization_cost_hours = optimization_time_hours
        roi_ratio = yearly_time_saved_hours / optimization_cost_hours if optimization_cost_hours > 0 else 0
        
        return {
            'daily_time_saved_ms': round(daily_time_saved_ms, 2),
            'daily_time_saved_seconds': round(daily_time_saved_seconds, 2),
            'daily_time_saved_minutes': round(daily_time_saved_minutes, 2),
            'yearly_time_saved_hours': round(yearly_time_saved_hours, 2),
            'optimization_effort_hours': optimization_cost_hours,
            'roi_ratio': round(roi_ratio, 2),
            'payback_period_days': round(optimization_cost_hours * 60 / daily_time_saved_minutes, 2) if daily_time_saved_minutes > 0 else float('inf'),
            'roi_category': 'excellent' if roi_ratio > 50 else 'good' if roi_ratio > 10 else 'moderate' if roi_ratio > 2 else 'poor'
        }
    
    def generate_detailed_analysis_report(self, before_file: str, after_file: str, 
                                        output_dir: str = "database_analysis") -> str:
        """Generate comprehensive performance analysis report."""
        logger.info("Loading benchmark results for comparison")
        
        # Load benchmark data
        before_data = self.load_benchmark_results(before_file)
        after_data = self.load_benchmark_results(after_file)
        
        # Extract metrics
        before_metrics = self.extract_query_metrics(before_data)
        after_metrics = self.extract_query_metrics(after_data)
        
        logger.info(f"Comparing {len(before_metrics)} before metrics with {len(after_metrics)} after metrics")
        
        # Calculate improvements
        improvements = self.calculate_improvement_metrics(before_metrics, after_metrics)
        summary = self.generate_improvement_summary(improvements)
        
        if 'error' in summary:
            raise ValueError(f"Could not generate improvement summary: {summary['error']}")
        
        # Generate detailed analysis
        analysis = {
            'analysis_timestamp': datetime.now().isoformat(),
            'analyzer_version': '1.0.0',
            'before_benchmark': {
                'file': before_file,
                'timestamp': before_data.get('benchmark_timestamp'),
                'total_queries': len(before_metrics)
            },
            'after_benchmark': {
                'file': after_file, 
                'timestamp': after_data.get('benchmark_timestamp'),
                'total_queries': len(after_metrics)
            },
            'improvement_summary': summary,
            'top_improvements': self.identify_top_improvements(improvements),
            'performance_regressions': self.identify_performance_regressions(improvements),
            'roi_analysis': self.calculate_roi_metrics(improvements),
            'detailed_improvements': improvements
        }
        
        # Save analysis to JSON
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        analysis_filename = f"performance_analysis_{timestamp}.json"
        analysis_filepath = output_path / analysis_filename
        
        with open(analysis_filepath, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Performance analysis saved to: {analysis_filepath}")
        return str(analysis_filepath)
    
    def generate_performance_report(self, analysis_filepath: str) -> str:
        """Generate human-readable performance analysis report."""
        with open(analysis_filepath, 'r', encoding='utf-8') as f:
            analysis = json.load(f)
        
        report = []
        report.append("=" * 80)
        report.append("DATABASE OPTIMIZATION PERFORMANCE ANALYSIS REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {analysis['analysis_timestamp']}")
        report.append("")
        
        # Analysis period
        before_time = analysis['before_benchmark']['timestamp']
        after_time = analysis['after_benchmark']['timestamp']
        report.append("📊 ANALYSIS PERIOD:")
        report.append(f"   Before Optimization: {before_time}")
        report.append(f"   After Optimization:  {after_time}")
        report.append("")
        
        # Overall summary
        summary = analysis['improvement_summary']
        report.append("🎯 OVERALL PERFORMANCE IMPROVEMENT:")
        report.append(f"   Queries Analyzed: {summary['total_queries_analyzed']}")
        report.append(f"   Average Improvement: {summary['overall_improvement_percent']}%")
        report.append(f"   Median Improvement: {summary['median_improvement_percent']}%")
        report.append(f"   Success Rate: {summary['success_rate_percent']}%")
        report.append(f"   Total Time Saved: {summary['total_time_saved_ms']:,.0f}ms")
        report.append("")
        
        # Category breakdown
        report.append("📋 IMPROVEMENT CATEGORIES:")
        categories = summary['improvement_categories']
        category_order = ['excellent', 'very_good', 'good', 'moderate', 'minimal', 'negligible', 'regression']
        
        for category in category_order:
            if category in categories:
                count = categories[category]
                percentage = (count / summary['total_queries_analyzed']) * 100
                emoji = self._get_category_emoji(category)
                report.append(f"   {emoji} {category.replace('_', ' ').title()}: {count} queries ({percentage:.1f}%)")
        report.append("")
        
        # Top improvements
        top_improvements = analysis.get('top_improvements', [])
        if top_improvements:
            report.append("🏆 TOP PERFORMANCE IMPROVEMENTS:")
            for i, imp in enumerate(top_improvements[:5], 1):
                report.append(f"   {i}. {imp['query_name']}: {imp['improvement_percent']}% faster "
                             f"({imp['before_avg_ms']:.1f}ms → {imp['after_avg_ms']:.1f}ms)")
            report.append("")
        
        # ROI Analysis
        roi = analysis.get('roi_analysis', {})
        if roi and 'error' not in roi:
            report.append("💰 RETURN ON INVESTMENT:")
            report.append(f"   Daily Time Saved: {roi['daily_time_saved_minutes']:.1f} minutes")
            report.append(f"   Yearly Time Saved: {roi['yearly_time_saved_hours']:.1f} hours")
            report.append(f"   ROI Ratio: {roi['roi_ratio']:.1f}x")
            report.append(f"   Payback Period: {roi['payback_period_days']:.1f} days")
            report.append(f"   ROI Category: {roi['roi_category'].title()}")
            report.append("")
        
        # Performance regressions (if any)
        regressions = analysis.get('performance_regressions', [])
        if regressions:
            report.append("⚠️  PERFORMANCE REGRESSIONS:")
            for reg in regressions[:3]:
                report.append(f"   • {reg['query_name']}: {abs(reg['improvement_percent']):.1f}% slower "
                             f"({reg['before_avg_ms']:.1f}ms → {reg['after_avg_ms']:.1f}ms)")
            report.append("")
        
        # Recommendations
        report.append("💡 RECOMMENDATIONS:")
        if summary['overall_improvement_percent'] > 50:
            report.append("   ✅ Optimization was highly successful - proceed with production deployment")
        elif summary['overall_improvement_percent'] > 25:
            report.append("   ✅ Good optimization results - monitor performance in production")
        elif summary['overall_improvement_percent'] > 10:
            report.append("   ⚠️  Moderate improvements - consider additional optimization")
        else:
            report.append("   ❌ Limited improvement - review optimization strategy")
        
        if regressions:
            report.append("   ⚠️  Investigate performance regressions before deployment")
        
        if summary['statistically_significant'] / summary['total_queries_analyzed'] > 0.8:
            report.append("   ✅ Results are statistically significant")
        else:
            report.append("   ⚠️  Consider running more benchmark iterations for statistical significance")
        
        return "\n".join(report)
    
    def _get_category_emoji(self, category: str) -> str:
        """Get emoji for improvement category."""
        emoji_map = {
            'excellent': '🚀',
            'very_good': '⚡',
            'good': '✅',
            'moderate': '📈',
            'minimal': '📊',
            'negligible': '➡️',
            'regression': '❌'
        }
        return emoji_map.get(category, '📊')


def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze database optimization performance')
    parser.add_argument('before_file', help='Path to before optimization benchmark results')
    parser.add_argument('after_file', help='Path to after optimization benchmark results')
    parser.add_argument('--output', '-o', default='database_analysis', 
                       help='Output directory for analysis results')
    
    args = parser.parse_args()
    
    try:
        analyzer = PerformanceAnalyzer()
        
        # Generate detailed analysis
        analysis_filepath = analyzer.generate_detailed_analysis_report(
            args.before_file, args.after_file, args.output
        )
        
        # Generate and display human-readable report
        report = analyzer.generate_performance_report(analysis_filepath)
        print(report)
        
        # Save report as well
        report_path = Path(analysis_filepath).parent / f"performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"Performance report saved to: {report_path}")
        logger.info("Performance analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"Performance analysis failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()