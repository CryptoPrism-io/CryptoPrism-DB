"""
Query Performance Monitor for CryptoPrism-DB QA system v2.
Detects slow queries, measures execution times, and identifies performance bottlenecks.
"""

import logging
from typing import Dict, List, Any, Optional
import statistics
import time

from ..core.base_qa import BaseQAModule, QAResult
from ..core.config import QAConfig
from ..core.database import DatabaseManager

logger = logging.getLogger(__name__)

class PerformanceMonitor(BaseQAModule):
    """
    Monitors database query performance and identifies slow queries.
    Provides detailed performance analytics and trend analysis.
    """
    
    def __init__(self, config: QAConfig, db_manager: DatabaseManager):
        """Initialize performance monitor."""
        super().__init__(config, db_manager, "performance_monitor")
        self.performance_history: List[Dict[str, Any]] = []
    
    def run_checks(self, databases: Optional[List[str]] = None) -> List[QAResult]:
        """
        Run all performance monitoring checks.
        
        Args:
            databases: List of databases to check, or None for all configured databases
            
        Returns:
            List of QA results
        """
        self.start_module()
        
        target_databases = databases or list(self.config.database_configs.keys())
        
        for database in target_databases:
            self.logger.info(f"🔍 Running performance checks for database: {database}")
            
            # Basic connectivity performance check
            self._check_database_connectivity_performance(database)
            
            # Standard performance test queries
            self._run_performance_test_suite(database)
            
            # Table scan performance checks
            self._check_table_scan_performance(database)
            
            # Query plan analysis for slow queries
            self._analyze_query_plans(database)
        
        # Cross-database performance comparison
        if len(target_databases) > 1:
            self._compare_cross_database_performance(target_databases)
        
        self.end_module()
        return self.results
    
    def _check_database_connectivity_performance(self, database: str):
        """Check basic database connectivity performance."""
        self.logger.debug(f"Checking connectivity performance for {database}")
        
        try:
            start_time = time.time()
            health_results = self.db_manager.health_check(database)
            
            if database in health_results:
                response_time_ms = health_results[database].get('response_time_ms', 0)
                
                # Classify response time
                if response_time_ms > 5000:  # 5 seconds
                    risk_level = 'CRITICAL'
                    status = 'FAIL'
                elif response_time_ms > 2000:  # 2 seconds
                    risk_level = 'HIGH'
                    status = 'WARNING'
                elif response_time_ms > 1000:  # 1 second
                    risk_level = 'MEDIUM'
                    status = 'WARNING'
                else:
                    risk_level = 'LOW'
                    status = 'PASS'
                
                self.create_result(
                    check_name=f"connectivity_performance_{database}",
                    status=status,
                    message=f"Database connectivity: {response_time_ms}ms response time",
                    risk_level=risk_level,
                    metrics={'response_time_ms': response_time_ms},
                    details={
                        'database': database,
                        'threshold_slow_ms': 1000,
                        'threshold_critical_ms': 5000
                    }
                )
            else:
                self.create_result(
                    check_name=f"connectivity_performance_{database}",
                    status='FAIL',
                    message=f"Failed to get health check for database {database}",
                    risk_level='CRITICAL'
                )
                
        except Exception as e:
            self.create_result(
                check_name=f"connectivity_performance_{database}",
                status='ERROR',
                message=f"Connectivity performance check failed: {str(e)}",
                risk_level='CRITICAL'
            )
    
    def _run_performance_test_suite(self, database: str):
        """Run comprehensive performance test suite."""
        self.logger.debug(f"Running performance test suite for {database}")
        
        test_queries = self.config.get_performance_queries()
        query_results = []
        
        for query_config in test_queries:
            query_name = query_config['name']
            query_sql = query_config['query']
            expected_max_time = query_config.get('expected_max_time', 1.0)
            description = query_config.get('description', '')
            
            try:
                # Execute query and measure performance
                result = self.db_manager.execute_performance_query(
                    database, query_sql, description
                )
                
                execution_time = result['execution_time_seconds']
                query_results.append({
                    'query_name': query_name,
                    'execution_time': execution_time,
                    'expected_max_time': expected_max_time,
                    'row_count': result.get('row_count', 0)
                })
                
                # Evaluate performance
                if result['status'] == 'error':
                    self.create_result(
                        check_name=f"query_performance_{database}_{query_name}",
                        status='ERROR',
                        message=f"Query failed: {result.get('error', 'Unknown error')}",
                        risk_level='CRITICAL',
                        details={'database': database, 'query_name': query_name}
                    )
                else:
                    # Performance evaluation
                    if execution_time > self.config.thresholds['critical_query_threshold']:
                        status = 'FAIL'
                        risk_level = 'CRITICAL'
                    elif execution_time > expected_max_time * 2:  # 2x expected time
                        status = 'FAIL'
                        risk_level = 'HIGH'
                    elif execution_time > expected_max_time:
                        status = 'WARNING'
                        risk_level = 'MEDIUM'
                    else:
                        status = 'PASS'
                        risk_level = 'LOW'
                    
                    self.create_result(
                        check_name=f"query_performance_{database}_{query_name}",
                        status=status,
                        message=f"Query '{query_name}': {execution_time:.3f}s (expected ≤{expected_max_time}s)",
                        risk_level=risk_level,
                        metrics={
                            'execution_time_seconds': execution_time,
                            'expected_max_time_seconds': expected_max_time,
                            'row_count': result.get('row_count', 0)
                        },
                        details={
                            'database': database,
                            'query_name': query_name,
                            'description': description
                        }
                    )
            
            except Exception as e:
                self.create_result(
                    check_name=f"query_performance_{database}_{query_name}",
                    status='ERROR',
                    message=f"Performance test failed for {query_name}: {str(e)}",
                    risk_level='CRITICAL',
                    details={'database': database, 'query_name': query_name}
                )
        
        # Overall performance summary
        if query_results:
            execution_times = [r['execution_time'] for r in query_results]
            avg_time = statistics.mean(execution_times)
            max_time = max(execution_times)
            
            # Overall performance assessment
            if max_time > self.config.thresholds['critical_query_threshold']:
                overall_risk = 'CRITICAL'
                overall_status = 'FAIL'
            elif avg_time > self.config.thresholds['slow_query_threshold']:
                overall_risk = 'HIGH'
                overall_status = 'WARNING'
            else:
                overall_risk = 'LOW'
                overall_status = 'PASS'
            
            self.create_result(
                check_name=f"overall_performance_{database}",
                status=overall_status,
                message=f"Overall performance: avg={avg_time:.3f}s, max={max_time:.3f}s",
                risk_level=overall_risk,
                metrics={
                    'average_execution_time': round(avg_time, 3),
                    'max_execution_time': round(max_time, 3),
                    'total_queries_tested': len(query_results)
                },
                details={
                    'database': database,
                    'query_breakdown': query_results
                }
            )
    
    def _check_table_scan_performance(self, database: str):
        """Check table scan performance for key tables."""
        self.logger.debug(f"Checking table scan performance for {database}")
        
        key_tables = self.config.key_tables
        
        for table_name in key_tables:
            if not self.check_table_exists(database, table_name):
                continue
            
            try:
                # Simple table scan performance test
                scan_query = f'SELECT COUNT(*) FROM public."{table_name}"'
                result = self.db_manager.execute_performance_query(
                    database, scan_query, f"Full table scan: {table_name}"
                )
                
                execution_time = result['execution_time_seconds']
                row_count = result.get('rows', [{}])[0].get('count', 0) if result.get('rows') else 0
                
                # Performance evaluation based on table size
                if row_count > 100000:  # Large table
                    threshold = 2.0
                elif row_count > 10000:  # Medium table
                    threshold = 1.0
                else:  # Small table
                    threshold = 0.5
                
                if execution_time > threshold * 3:  # 3x threshold
                    status = 'FAIL'
                    risk_level = 'HIGH'
                elif execution_time > threshold:
                    status = 'WARNING'
                    risk_level = 'MEDIUM'
                else:
                    status = 'PASS'
                    risk_level = 'LOW'
                
                self.create_result(
                    check_name=f"table_scan_performance_{database}_{table_name}",
                    status=status,
                    message=f"Table scan {table_name}: {execution_time:.3f}s for {row_count:,} rows",
                    risk_level=risk_level,
                    metrics={
                        'execution_time_seconds': execution_time,
                        'row_count': row_count,
                        'rows_per_second': int(row_count / execution_time) if execution_time > 0 else 0
                    },
                    details={
                        'database': database,
                        'table_name': table_name,
                        'threshold_seconds': threshold
                    }
                )
                
            except Exception as e:
                self.create_result(
                    check_name=f"table_scan_performance_{database}_{table_name}",
                    status='ERROR',
                    message=f"Table scan performance check failed for {table_name}: {str(e)}",
                    risk_level='HIGH',
                    details={'database': database, 'table_name': table_name}
                )
    
    def _analyze_query_plans(self, database: str):
        """Analyze query execution plans for optimization opportunities."""
        self.logger.debug(f"Analyzing query plans for {database}")
        
        try:
            with self.db_manager.get_connection(database) as conn:
                from sqlalchemy import text
                
                # Get slow queries from pg_stat_statements (if available)
                slow_queries_query = text("""
                    SELECT 
                        query,
                        calls,
                        total_exec_time,
                        mean_exec_time,
                        rows
                    FROM pg_stat_statements 
                    WHERE mean_exec_time > :threshold_ms
                    ORDER BY mean_exec_time DESC
                    LIMIT 10
                """)
                
                try:
                    result = conn.execute(slow_queries_query, {
                        'threshold_ms': self.config.thresholds['slow_query_threshold'] * 1000
                    })
                    slow_queries = result.fetchall()
                    
                    if slow_queries:
                        query_details = []
                        for query in slow_queries:
                            query_details.append({
                                'query_text': str(query[0])[:200] + '...' if len(str(query[0])) > 200 else str(query[0]),
                                'calls': int(query[1]),
                                'total_time_ms': float(query[2]),
                                'mean_time_ms': float(query[3]),
                                'rows': int(query[4]) if query[4] else 0
                            })
                        
                        self.create_result(
                            check_name=f"slow_queries_analysis_{database}",
                            status='WARNING',
                            message=f"Found {len(slow_queries)} slow queries requiring optimization",
                            risk_level='MEDIUM' if len(slow_queries) < 5 else 'HIGH',
                            details={
                                'database': database,
                                'slow_queries': query_details,
                                'threshold_ms': self.config.thresholds['slow_query_threshold'] * 1000
                            }
                        )
                    else:
                        self.create_result(
                            check_name=f"slow_queries_analysis_{database}",
                            status='PASS',
                            message="No slow queries detected in pg_stat_statements",
                            risk_level='LOW',
                            details={'database': database}
                        )
                
                except Exception as e:
                    if "pg_stat_statements" in str(e):
                        self.create_result(
                            check_name=f"slow_queries_analysis_{database}",
                            status='WARNING',
                            message="pg_stat_statements extension not available",
                            risk_level='LOW',
                            details={'database': database, 'reason': 'extension_missing'}
                        )
                    else:
                        raise
                        
        except Exception as e:
            self.create_result(
                check_name=f"query_plan_analysis_{database}",
                status='ERROR',
                message=f"Query plan analysis failed: {str(e)}",
                risk_level='MEDIUM',
                details={'database': database}
            )
    
    def _compare_cross_database_performance(self, databases: List[str]):
        """Compare performance metrics across databases."""
        self.logger.debug("Running cross-database performance comparison")
        
        # Extract performance metrics from results
        db_metrics = {}
        
        for result in self.results:
            if result.check_name.startswith('overall_performance_'):
                db_name = result.check_name.split('_')[-1]
                if db_name in databases:
                    db_metrics[db_name] = {
                        'avg_time': result.metrics.get('average_execution_time', 0),
                        'max_time': result.metrics.get('max_execution_time', 0),
                        'queries_tested': result.metrics.get('total_queries_tested', 0)
                    }
        
        if len(db_metrics) >= 2:
            # Find performance differences
            avg_times = {db: metrics['avg_time'] for db, metrics in db_metrics.items()}
            fastest_db = min(avg_times.items(), key=lambda x: x[1])
            slowest_db = max(avg_times.items(), key=lambda x: x[1])
            
            performance_ratio = slowest_db[1] / fastest_db[1] if fastest_db[1] > 0 else 1
            
            if performance_ratio > 3:  # 3x difference
                risk_level = 'HIGH'
                status = 'WARNING'
            elif performance_ratio > 2:  # 2x difference
                risk_level = 'MEDIUM'
                status = 'WARNING'
            else:
                risk_level = 'LOW'
                status = 'PASS'
            
            self.create_result(
                check_name="cross_database_performance_comparison",
                status=status,
                message=f"Performance variation: {fastest_db[0]} (fastest: {fastest_db[1]:.3f}s) vs {slowest_db[0]} (slowest: {slowest_db[1]:.3f}s)",
                risk_level=risk_level,
                metrics={
                    'performance_ratio': round(performance_ratio, 2),
                    'fastest_db': fastest_db[0],
                    'fastest_avg_time': fastest_db[1],
                    'slowest_db': slowest_db[0],
                    'slowest_avg_time': slowest_db[1]
                },
                details={
                    'databases_compared': list(databases),
                    'all_metrics': db_metrics
                }
            )