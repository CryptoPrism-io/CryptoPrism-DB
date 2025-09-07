#!/usr/bin/env python3
"""
Query Performance Benchmarker for CryptoPrism-DB Optimization

This script runs a comprehensive suite of performance tests on critical database queries
to establish baseline performance metrics before optimization and measure improvements after.
It focuses on real-world query patterns used in the CryptoPrism-DB system.

Features:
- Benchmarks JOIN, filtering, aggregation, and range queries
- Measures execution time with high precision
- Captures query execution plans (EXPLAIN ANALYZE)
- Tests queries against FE_* signal tables
- Generates detailed performance reports
- Supports before/after comparison analysis

Requirements:
- sqlalchemy>=2.0.0
- psycopg2-binary>=2.9.0
- python-dotenv
"""

import os
import sys
import json
import time
import logging
import statistics
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from collections import defaultdict

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class QueryBenchmarker:
    """Comprehensive query performance benchmarking for database optimization."""
    
    def __init__(self):
        """Initialize benchmarker with environment variables."""
        load_dotenv()
        
        # Database connection parameters
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'), 
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # Target databases for benchmarking - Focus on dbcp only for testing
        self.databases = {
            'main': os.getenv('DB_NAME', 'dbcp')
        }
        
        # Validate environment variables
        required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise EnvironmentError(f"Missing required environment variables: {missing_vars}")
        
        # Performance test configuration
        self.test_config = {
            'warmup_runs': 2,      # Number of warmup executions
            'test_runs': 5,        # Number of timed executions for averaging
            'timeout_seconds': 300  # Query timeout (5 minutes)
        }
    
    def create_connection_string(self, database_name: str) -> str:
        """Create PostgreSQL connection string."""
        return (f"postgresql+psycopg2://{self.db_config['user']}:"
                f"{self.db_config['password']}@{self.db_config['host']}:"
                f"{self.db_config['port']}/{database_name}")
    
    def execute_with_timing(self, engine, query: str, params: Dict = None) -> Tuple[float, Optional[Any], Optional[str]]:
        """Execute query and measure execution time with error handling."""
        try:
            with engine.connect() as conn:
                # Set query timeout
                conn.execute(text(f"SET statement_timeout = {self.test_config['timeout_seconds'] * 1000}"))
                
                start_time = time.perf_counter()
                result = conn.execute(text(query), params or {})
                
                # Fetch all results to ensure query completion
                rows = result.fetchall()
                end_time = time.perf_counter()
                
                execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
                return execution_time, rows, None
                
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"Query execution failed: {error_msg[:200]}...")
            return 0.0, None, error_msg
    
    def get_query_plan(self, engine, query: str, params: Dict = None) -> Optional[str]:
        """Get query execution plan using EXPLAIN ANALYZE."""
        try:
            with engine.connect() as conn:
                # Set query timeout
                conn.execute(text(f"SET statement_timeout = {self.test_config['timeout_seconds'] * 1000}"))
                
                explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
                result = conn.execute(text(explain_query), params or {})
                plan_data = result.fetchone()[0]
                
                return json.dumps(plan_data, indent=2)
                
        except Exception as e:
            logger.warning(f"Could not get query plan: {e}")
            return None
    
    def benchmark_query(self, engine, query_name: str, query: str, params: Dict = None) -> Dict[str, Any]:
        """Benchmark a single query with multiple runs and statistical analysis."""
        logger.info(f"Benchmarking query: {query_name}")
        
        # Warmup runs
        for i in range(self.test_config['warmup_runs']):
            self.execute_with_timing(engine, query, params)
        
        # Timed runs
        execution_times = []
        results_info = None
        error_msg = None
        
        for i in range(self.test_config['test_runs']):
            exec_time, result_rows, error = self.execute_with_timing(engine, query, params)
            
            if error:
                error_msg = error
                break
            
            execution_times.append(exec_time)
            
            if results_info is None and result_rows:
                results_info = {
                    'row_count': len(result_rows),
                    'column_count': len(result_rows[0]) if result_rows else 0
                }
        
        # Calculate statistics if we have successful runs
        if execution_times:
            avg_time = statistics.mean(execution_times)
            median_time = statistics.median(execution_times)
            min_time = min(execution_times)
            max_time = max(execution_times)
            std_dev = statistics.stdev(execution_times) if len(execution_times) > 1 else 0
            
            # Get query execution plan
            query_plan = self.get_query_plan(engine, query, params)
            
            benchmark_result = {
                'query_name': query_name,
                'status': 'success',
                'execution_times_ms': execution_times,
                'statistics': {
                    'average_ms': round(avg_time, 2),
                    'median_ms': round(median_time, 2),
                    'min_ms': round(min_time, 2),
                    'max_ms': round(max_time, 2),
                    'std_dev_ms': round(std_dev, 2),
                    'coefficient_of_variation': round((std_dev / avg_time) * 100, 2) if avg_time > 0 else 0
                },
                'results_info': results_info or {'row_count': 0, 'column_count': 0},
                'query_plan': query_plan,
                'error': None
            }
        else:
            benchmark_result = {
                'query_name': query_name,
                'status': 'failed',
                'execution_times_ms': [],
                'statistics': {},
                'results_info': {'row_count': 0, 'column_count': 0},
                'query_plan': None,
                'error': error_msg
            }
        
        return benchmark_result
    
    def get_production_query_suite(self) -> Dict[str, Dict[str, str]]:
        """Define the comprehensive query test suite based on CryptoPrism-DB usage patterns."""
        
        # Calculate date parameters for realistic testing
        today = datetime.now().date()
        week_ago = today - timedelta(days=7)
        month_ago = today - timedelta(days=30)
        
        queries = {
            # JOIN Performance Tests - Critical for signal correlation
            "join_dmv_momentum_signals": {
                "description": "Join FE_DMV_ALL with FE_MOMENTUM_SIGNALS for signal correlation",
                "query": """
                    SELECT f.slug, f.timestamp, f.bullish, f.bearish, f.neutral,
                           m.rsi_14, m.roc_21, m.williams_r_14
                    FROM "FE_DMV_ALL" f 
                    JOIN "FE_MOMENTUM_SIGNALS" m ON f.slug = m.slug AND f.timestamp = m.timestamp
                    WHERE f.timestamp >= :start_date
                    LIMIT 1000
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            "join_dmv_oscillators": {
                "description": "Join FE_DMV_ALL with FE_OSCILLATORS_SIGNALS",
                "query": """
                    SELECT f.slug, f.timestamp, f.momentum_score, f.durability_score,
                           o.macd_signal, o.cci_14, o.adx_14
                    FROM "FE_DMV_ALL" f 
                    JOIN "FE_OSCILLATORS_SIGNALS" o ON f.slug = m.slug AND f.timestamp = o.timestamp
                    WHERE f.timestamp >= :start_date
                    LIMIT 1000
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            "join_all_signals": {
                "description": "Complex join across multiple signal tables",
                "query": """
                    SELECT f.slug, f.timestamp, f.bullish, 
                           m.rsi_14, o.macd_signal, r.sharpe_ratio
                    FROM "FE_DMV_ALL" f 
                    LEFT JOIN "FE_MOMENTUM_SIGNALS" m ON f.slug = m.slug AND f.timestamp = m.timestamp
                    LEFT JOIN "FE_OSCILLATORS_SIGNALS" o ON f.slug = o.slug AND f.timestamp = o.timestamp  
                    LEFT JOIN "FE_RATIOS_SIGNALS" r ON f.slug = r.slug AND f.timestamp = r.timestamp
                    WHERE f.timestamp >= :start_date
                    LIMIT 500
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            # Filtering Tests - Common WHERE clause patterns
            "filter_by_slug_and_date": {
                "description": "Filter by specific cryptocurrency and date range",
                "query": """
                    SELECT * FROM "FE_DMV_ALL" 
                    WHERE slug = 'bitcoin' 
                    AND timestamp >= :start_date 
                    AND timestamp <= :end_date
                """,
                "params": {"start_date": str(month_ago), "end_date": str(today)}
            },
            
            "filter_bullish_signals": {
                "description": "Find cryptocurrencies with strong bullish signals",
                "query": """
                    SELECT slug, timestamp, bullish, bearish, momentum_score
                    FROM "FE_DMV_ALL" 
                    WHERE bullish > bearish 
                    AND momentum_score > 0.7
                    AND timestamp >= :start_date
                    ORDER BY bullish DESC, momentum_score DESC
                    LIMIT 100
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            "filter_high_volume": {
                "description": "Filter high-volume cryptocurrencies from OHLCV data",
                "query": """
                    SELECT slug, timestamp, close, volume 
                    FROM "1K_coins_ohlcv"
                    WHERE volume > 1000000 
                    AND timestamp >= :start_date
                    ORDER BY volume DESC
                    LIMIT 200
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            # Aggregation Tests - GROUP BY operations for analytics
            "aggregate_signals_by_slug": {
                "description": "Aggregate signals by cryptocurrency over time period",
                "query": """
                    SELECT slug, 
                           COUNT(*) as total_signals,
                           AVG(bullish) as avg_bullish,
                           AVG(bearish) as avg_bearish,
                           MAX(momentum_score) as max_momentum,
                           MIN(durability_score) as min_durability
                    FROM "FE_DMV_ALL" 
                    WHERE timestamp >= :start_date
                    GROUP BY slug
                    HAVING COUNT(*) > 5
                    ORDER BY avg_bullish DESC
                    LIMIT 50
                """,
                "params": {"start_date": str(month_ago)}
            },
            
            "aggregate_daily_performance": {
                "description": "Daily performance aggregation across all cryptocurrencies",
                "query": """
                    SELECT DATE(timestamp) as trade_date,
                           COUNT(DISTINCT slug) as active_coins,
                           AVG(bullish) as avg_bullish_signals,
                           AVG(momentum_score) as avg_momentum,
                           COUNT(*) as total_signals
                    FROM "FE_DMV_ALL" 
                    WHERE timestamp >= :start_date
                    GROUP BY DATE(timestamp)
                    ORDER BY trade_date DESC
                """,
                "params": {"start_date": str(month_ago)}
            },
            
            # Range Query Tests - Time-series analysis patterns
            "range_momentum_analysis": {
                "description": "RSI momentum analysis over date range",
                "query": """
                    SELECT slug, timestamp, rsi_14, roc_21
                    FROM "FE_MOMENTUM_SIGNALS" 
                    WHERE timestamp BETWEEN :start_date AND :end_date
                    AND rsi_14 IS NOT NULL
                    ORDER BY timestamp DESC, rsi_14 DESC
                    LIMIT 1000
                """,
                "params": {"start_date": str(month_ago), "end_date": str(today)}
            },
            
            "range_price_analysis": {
                "description": "Price range analysis for trending cryptocurrencies", 
                "query": """
                    SELECT slug, timestamp, open, high, low, close, volume
                    FROM "1K_coins_ohlcv"
                    WHERE timestamp BETWEEN :start_date AND :end_date
                    AND slug IN ('bitcoin', 'ethereum', 'cardano', 'solana', 'chainlink')
                    ORDER BY timestamp DESC, volume DESC
                """,
                "params": {"start_date": str(week_ago), "end_date": str(today)}
            },
            
            # Complex Analytics Queries
            "top_performers_analysis": {
                "description": "Identify top-performing cryptocurrencies with multiple criteria",
                "query": """
                    SELECT d.slug, 
                           AVG(d.bullish) as avg_bullish,
                           AVG(d.momentum_score) as avg_momentum,
                           AVG(m.rsi_14) as avg_rsi,
                           COUNT(*) as signal_count
                    FROM "FE_DMV_ALL" d
                    JOIN "FE_MOMENTUM_SIGNALS" m ON d.slug = m.slug AND d.timestamp = m.timestamp
                    WHERE d.timestamp >= :start_date
                    GROUP BY d.slug
                    HAVING COUNT(*) >= 5 AND AVG(d.bullish) > 3
                    ORDER BY avg_bullish DESC, avg_momentum DESC
                    LIMIT 25
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            "market_overview": {
                "description": "Comprehensive market overview query",
                "query": """
                    SELECT 
                        COUNT(DISTINCT slug) as total_coins,
                        AVG(bullish) as market_bullish_avg,
                        AVG(bearish) as market_bearish_avg,
                        COUNT(CASE WHEN bullish > bearish THEN 1 END) as bullish_majority_coins,
                        MAX(timestamp) as latest_update
                    FROM "FE_DMV_ALL" 
                    WHERE timestamp >= :start_date
                """,
                "params": {"start_date": str(week_ago)}
            }
        }
        
        return queries
    
    def check_table_existence(self, engine, required_tables: List[str]) -> Dict[str, bool]:
        """Check which required tables exist in the database."""
        table_status = {}
        
        for table in required_tables:
            try:
                with engine.connect() as conn:
                    if table.startswith('"') and table.endswith('"'):
                        # Quoted table names (like "1K_coins_ohlcv")
                        query = f"SELECT 1 FROM {table} LIMIT 1"
                    else:
                        query = f'SELECT 1 FROM "{table}" LIMIT 1'
                    
                    conn.execute(text(query))
                    table_status[table] = True
                    
            except Exception:
                table_status[table] = False
        
        return table_status
    
    def benchmark_database(self, database_key: str) -> Dict[str, Any]:
        """Benchmark all queries for a single database."""
        database_name = self.databases[database_key]
        logger.info(f"Starting benchmark for {database_name} ({database_key})")
        
        try:
            # Create database connection
            conn_string = self.create_connection_string(database_name)
            engine = create_engine(conn_string)
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            # Check table existence
            required_tables = [
                "FE_DMV_ALL", "FE_MOMENTUM_SIGNALS", "FE_OSCILLATORS_SIGNALS", 
                "FE_RATIOS_SIGNALS", "1K_coins_ohlcv"
            ]
            table_status = self.check_table_existence(engine, required_tables)
            
            # Get query suite
            query_suite = self.get_production_query_suite()
            
            # Run benchmarks
            benchmark_results = {}
            successful_tests = 0
            
            for query_name, query_info in query_suite.items():
                try:
                    result = self.benchmark_query(
                        engine, 
                        query_name, 
                        query_info["query"], 
                        query_info.get("params", {})
                    )
                    result['description'] = query_info["description"]
                    benchmark_results[query_name] = result
                    
                    if result['status'] == 'success':
                        successful_tests += 1
                        logger.info(f"✅ {query_name}: {result['statistics'].get('average_ms', 0):.2f}ms avg")
                    else:
                        logger.warning(f"❌ {query_name}: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    logger.error(f"Error benchmarking {query_name}: {e}")
                    benchmark_results[query_name] = {
                        'query_name': query_name,
                        'status': 'error',
                        'error': str(e),
                        'description': query_info["description"]
                    }
            
            # Compile database benchmark summary
            total_tests = len(query_suite)
            success_rate = (successful_tests / total_tests) * 100 if total_tests > 0 else 0
            
            # Calculate overall performance metrics
            successful_results = [r for r in benchmark_results.values() if r['status'] == 'success']
            if successful_results:
                avg_times = [r['statistics']['average_ms'] for r in successful_results]
                overall_avg_time = statistics.mean(avg_times)
                overall_median_time = statistics.median(avg_times)
            else:
                overall_avg_time = 0
                overall_median_time = 0
            
            database_benchmark = {
                'database_name': database_name,
                'database_key': database_key,
                'benchmark_timestamp': datetime.now().isoformat(),
                'table_status': table_status,
                'test_summary': {
                    'total_tests': total_tests,
                    'successful_tests': successful_tests,
                    'success_rate_percent': round(success_rate, 2),
                    'overall_avg_time_ms': round(overall_avg_time, 2),
                    'overall_median_time_ms': round(overall_median_time, 2)
                },
                'query_results': benchmark_results,
                'test_config': self.test_config
            }
            
            engine.dispose()
            return database_benchmark
            
        except Exception as e:
            logger.error(f"Database benchmark failed for {database_name}: {e}")
            return {
                'database_name': database_name,
                'database_key': database_key,
                'benchmark_timestamp': datetime.now().isoformat(),
                'error': str(e),
                'query_results': {}
            }
    
    def run_full_benchmark_suite(self) -> Dict[str, Any]:
        """Run comprehensive benchmarks across all databases."""
        logger.info("Starting full benchmark suite across all databases")
        
        benchmarks = {}
        for db_key in self.databases.keys():
            benchmarks[db_key] = self.benchmark_database(db_key)
        
        # Create comprehensive benchmark analysis
        analysis = {
            'benchmark_timestamp': datetime.now().isoformat(),
            'benchmarker_version': '1.0.0',
            'test_configuration': self.test_config,
            'database_benchmarks': benchmarks,
            'cross_database_analysis': self.analyze_cross_database_performance(benchmarks)
        }
        
        return analysis
    
    def analyze_cross_database_performance(self, benchmarks: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze performance patterns across databases."""
        analysis = {
            'slowest_queries': [],
            'fastest_queries': [],
            'performance_summary': {},
            'optimization_recommendations': []
        }
        
        all_query_results = []
        
        # Collect all successful query results
        for db_key, db_benchmark in benchmarks.items():
            if 'query_results' in db_benchmark:
                for query_name, result in db_benchmark['query_results'].items():
                    if result['status'] == 'success':
                        all_query_results.append({
                            'database': db_key,
                            'query_name': query_name,
                            'avg_time_ms': result['statistics']['average_ms'],
                            'description': result.get('description', ''),
                            'row_count': result['results_info']['row_count']
                        })
        
        if all_query_results:
            # Find slowest and fastest queries
            sorted_by_time = sorted(all_query_results, key=lambda x: x['avg_time_ms'])
            
            analysis['fastest_queries'] = sorted_by_time[:5]
            analysis['slowest_queries'] = sorted_by_time[-5:]
            
            # Performance summary
            times = [r['avg_time_ms'] for r in all_query_results]
            analysis['performance_summary'] = {
                'total_successful_queries': len(all_query_results),
                'average_query_time_ms': round(statistics.mean(times), 2),
                'median_query_time_ms': round(statistics.median(times), 2),
                'slowest_query_time_ms': max(times),
                'fastest_query_time_ms': min(times)
            }
            
            # Generate optimization recommendations
            slow_queries = [r for r in all_query_results if r['avg_time_ms'] > 1000]  # > 1 second
            if slow_queries:
                analysis['optimization_recommendations'].append(
                    f"HIGH PRIORITY: {len(slow_queries)} queries take >1 second to execute"
                )
            
            join_queries = [r for r in all_query_results if 'join' in r['query_name'].lower()]
            if join_queries:
                avg_join_time = statistics.mean([r['avg_time_ms'] for r in join_queries])
                if avg_join_time > 500:  # > 500ms
                    analysis['optimization_recommendations'].append(
                        f"JOIN optimization needed - average JOIN query time: {avg_join_time:.1f}ms"
                    )
        
        return analysis
    
    def save_benchmark_results(self, results: Dict[str, Any], output_dir: str = "database_analysis") -> str:
        """Save benchmark results to JSON file."""
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"query_benchmark_results_{timestamp}.json"
        filepath = output_path / filename
        
        # Save to JSON file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Benchmark results saved to: {filepath}")
        return str(filepath)
    
    def generate_benchmark_report(self, results: Dict[str, Any]) -> str:
        """Generate human-readable benchmark report."""
        report = []
        report.append("=" * 80)
        report.append("DATABASE QUERY PERFORMANCE BENCHMARK REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {results['benchmark_timestamp']}")
        report.append(f"Test Configuration: {results['test_configuration']['test_runs']} runs, "
                     f"{results['test_configuration']['warmup_runs']} warmup")
        report.append("")
        
        # Database summaries
        for db_key, db_benchmark in results['database_benchmarks'].items():
            if 'error' in db_benchmark:
                report.append(f"❌ {db_benchmark['database_name'].upper()} ({db_key}): ERROR - {db_benchmark['error']}")
                continue
            
            summary = db_benchmark['test_summary']
            report.append(f"📊 {db_benchmark['database_name'].upper()} ({db_key}):")
            report.append(f"   Success Rate: {summary['success_rate_percent']}% ({summary['successful_tests']}/{summary['total_tests']})")
            report.append(f"   Average Query Time: {summary['overall_avg_time_ms']}ms")
            report.append(f"   Median Query Time: {summary['overall_median_time_ms']}ms")
            report.append("")
        
        # Cross-database analysis
        cross_analysis = results['cross_database_analysis']
        if 'performance_summary' in cross_analysis:
            perf = cross_analysis['performance_summary']
            report.append("🎯 OVERALL PERFORMANCE:")
            report.append(f"   Total Queries: {perf['total_successful_queries']}")
            report.append(f"   Average Time: {perf['average_query_time_ms']}ms")
            report.append(f"   Median Time: {perf['median_query_time_ms']}ms")
            report.append(f"   Range: {perf['fastest_query_time_ms']}ms - {perf['slowest_query_time_ms']}ms")
            report.append("")
        
        # Slowest queries
        if cross_analysis.get('slowest_queries'):
            report.append("🐌 SLOWEST QUERIES (Need Optimization):")
            for query in cross_analysis['slowest_queries']:
                report.append(f"   ⚠️  {query['database']}.{query['query_name']}: {query['avg_time_ms']}ms ({query['row_count']} rows)")
            report.append("")
        
        # Optimization recommendations
        if cross_analysis.get('optimization_recommendations'):
            report.append("💡 OPTIMIZATION RECOMMENDATIONS:")
            for rec in cross_analysis['optimization_recommendations']:
                report.append(f"   🔧 {rec}")
        
        return "\n".join(report)


def main():
    """Main execution function."""
    try:
        # Initialize benchmarker
        benchmarker = QueryBenchmarker()
        
        # Run comprehensive benchmark suite
        results = benchmarker.run_full_benchmark_suite()
        
        # Save results to JSON file
        json_filepath = benchmarker.save_benchmark_results(results)
        
        # Generate and display benchmark report
        report = benchmarker.generate_benchmark_report(results)
        print(report)
        
        # Save report as well
        report_path = Path(json_filepath).parent / f"benchmark_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"Benchmark report saved to: {report_path}")
        logger.info("Query benchmarking completed successfully!")
        
        return json_filepath
        
    except Exception as e:
        logger.error(f"Benchmark execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()