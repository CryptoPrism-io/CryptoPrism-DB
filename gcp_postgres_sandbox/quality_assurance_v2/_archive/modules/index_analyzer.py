"""
Index Utilization Analyzer for CryptoPrism-DB QA system v2.
Analyzes index usage, identifies unused indexes, and suggests optimization opportunities.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple

from ..core.base_qa import BaseQAModule, QAResult
from ..core.config import QAConfig
from ..core.database import DatabaseManager

logger = logging.getLogger(__name__)

class IndexAnalyzer(BaseQAModule):
    """
    Analyzes database index utilization and provides optimization recommendations.
    Identifies unused indexes and suggests missing indexes for slow queries.
    """
    
    def __init__(self, config: QAConfig, db_manager: DatabaseManager):
        """Initialize index analyzer."""
        super().__init__(config, db_manager, "index_analyzer")
    
    def run_checks(self, databases: Optional[List[str]] = None) -> List[QAResult]:
        """
        Run all index analysis checks.
        
        Args:
            databases: List of databases to check, or None for all configured databases
            
        Returns:
            List of QA results
        """
        self.start_module()
        
        target_databases = databases or list(self.config.database_configs.keys())
        
        for database in target_databases:
            self.logger.info(f"🔍 Running index analysis for database: {database}")
            
            # Basic index inventory
            self._analyze_index_inventory(database)
            
            # Index usage statistics
            self._analyze_index_usage(database)
            
            # Unused index detection
            self._detect_unused_indexes(database)
            
            # Missing index suggestions
            self._suggest_missing_indexes(database)
            
            # Index size and maintenance analysis
            self._analyze_index_maintenance(database)
        
        # Cross-database index comparison
        if len(target_databases) > 1:
            self._compare_cross_database_indexes(target_databases)
        
        self.end_module()
        return self.results
    
    def _analyze_index_inventory(self, database: str):
        """Analyze the overall index inventory and distribution."""
        self.logger.debug(f"Analyzing index inventory for {database}")
        
        try:
            with self.db_manager.get_connection(database) as conn:
                from sqlalchemy import text
                
                # Get comprehensive index information
                index_query = text("""
                    SELECT 
                        schemaname,
                        tablename,
                        indexname,
                        indexdef,
                        CASE 
                            WHEN indexdef LIKE '%UNIQUE%' THEN 'UNIQUE'
                            WHEN indexdef LIKE '%PRIMARY KEY%' THEN 'PRIMARY'
                            WHEN indexdef LIKE '%btree%' THEN 'BTREE'
                            WHEN indexdef LIKE '%gin%' THEN 'GIN'
                            WHEN indexdef LIKE '%gist%' THEN 'GIST'
                            ELSE 'OTHER'
                        END as index_type
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                    ORDER BY tablename, indexname
                """)
                
                indexes = conn.execute(index_query).fetchall()
                
                if not indexes:
                    self.create_result(
                        check_name=f"index_inventory_{database}",
                        status='WARNING',
                        message=f"No indexes found in database {database}",
                        risk_level='HIGH',
                        details={'database': database}
                    )
                    return
                
                # Analyze index distribution
                index_by_table = {}
                index_types = {}
                total_indexes = len(indexes)
                
                for index in indexes:
                    table = index[1]
                    index_type = index[4]
                    
                    if table not in index_by_table:
                        index_by_table[table] = 0
                    index_by_table[table] += 1
                    
                    if index_type not in index_types:
                        index_types[index_type] = 0
                    index_types[index_type] += 1
                
                # Identify tables with no indexes (excluding system defaults)
                key_tables = [t for t in self.config.key_tables if self.check_table_exists(database, t)]
                tables_without_indexes = [t for t in key_tables if t not in index_by_table]
                
                if tables_without_indexes:
                    self.create_result(
                        check_name=f"tables_without_indexes_{database}",
                        status='WARNING',
                        message=f"{len(tables_without_indexes)} key tables have no indexes",
                        risk_level='HIGH',
                        details={
                            'database': database,
                            'tables_without_indexes': tables_without_indexes
                        }
                    )
                
                # Overall index summary
                avg_indexes_per_table = total_indexes / len(index_by_table) if index_by_table else 0
                
                self.create_result(
                    check_name=f"index_inventory_{database}",
                    status='PASS',
                    message=f"Found {total_indexes} indexes across {len(index_by_table)} tables",
                    risk_level='LOW',
                    metrics={
                        'total_indexes': total_indexes,
                        'tables_with_indexes': len(index_by_table),
                        'avg_indexes_per_table': round(avg_indexes_per_table, 1)
                    },
                    details={
                        'database': database,
                        'index_types': index_types,
                        'indexes_per_table': index_by_table
                    }
                )
        
        except Exception as e:
            self.create_result(
                check_name=f"index_inventory_{database}",
                status='ERROR',
                message=f"Index inventory analysis failed: {str(e)}",
                risk_level='MEDIUM',
                details={'database': database}
            )
    
    def _analyze_index_usage(self, database: str):
        """Analyze index usage statistics and efficiency."""
        self.logger.debug(f"Analyzing index usage for {database}")
        
        try:
            index_stats = self.db_manager.get_index_usage_stats(database)
            
            if not index_stats:
                self.create_result(
                    check_name=f"index_usage_{database}",
                    status='WARNING',
                    message=f"No index usage statistics available for {database}",
                    risk_level='MEDIUM',
                    details={'database': database, 'reason': 'no_stats_available'}
                )
                return
            
            # Categorize indexes by usage
            usage_categories = {'UNUSED': 0, 'LOW_USAGE': 0, 'MEDIUM_USAGE': 0, 'HIGH_USAGE': 0}
            high_efficiency_indexes = []
            low_efficiency_indexes = []
            
            for index_stat in index_stats:
                usage_category = index_stat['usage_category']
                usage_categories[usage_category] += 1
                
                # Calculate efficiency (tuples fetched / tuples read)
                if index_stat['tuples_read'] > 0:
                    efficiency = index_stat['tuples_fetched'] / index_stat['tuples_read']
                    
                    if efficiency > 0.8:  # High efficiency
                        high_efficiency_indexes.append({
                            'index': index_stat['index'],
                            'table': index_stat['table'],
                            'efficiency': round(efficiency, 3)
                        })
                    elif efficiency < 0.2:  # Low efficiency
                        low_efficiency_indexes.append({
                            'index': index_stat['index'],
                            'table': index_stat['table'],
                            'efficiency': round(efficiency, 3),
                            'scans': index_stat['scan_count']
                        })
            
            # Overall usage assessment
            total_indexes = len(index_stats)
            unused_count = usage_categories['UNUSED']
            unused_percentage = unused_count / total_indexes * 100 if total_indexes > 0 else 0
            
            if unused_percentage > 50:
                status, risk_level = 'WARNING', 'HIGH'
            elif unused_percentage > 25:
                status, risk_level = 'WARNING', 'MEDIUM'
            else:
                status, risk_level = 'PASS', 'LOW'
            
            self.create_result(
                check_name=f"index_usage_{database}",
                status=status,
                message=f"Index usage: {unused_percentage:.1f}% unused ({unused_count}/{total_indexes})",
                risk_level=risk_level,
                metrics={
                    'total_indexes': total_indexes,
                    'unused_count': unused_count,
                    'unused_percentage': round(unused_percentage, 1)
                },
                details={
                    'database': database,
                    'usage_distribution': usage_categories,
                    'high_efficiency_indexes': high_efficiency_indexes[:5],  # Top 5
                    'low_efficiency_indexes': low_efficiency_indexes[:5]     # Bottom 5
                }
            )
        
        except Exception as e:
            self.create_result(
                check_name=f"index_usage_{database}",
                status='ERROR',
                message=f"Index usage analysis failed: {str(e)}",
                risk_level='MEDIUM',
                details={'database': database}
            )
    
    def _detect_unused_indexes(self, database: str):
        """Detect and report unused indexes that could be dropped."""
        self.logger.debug(f"Detecting unused indexes for {database}")
        
        try:
            index_stats = self.db_manager.get_index_usage_stats(database)
            unused_indexes = [stat for stat in index_stats if stat['usage_category'] == 'UNUSED']
            
            if not unused_indexes:
                self.create_result(
                    check_name=f"unused_indexes_{database}",
                    status='PASS',
                    message="No unused indexes detected",
                    risk_level='LOW',
                    details={'database': database}
                )
                return
            
            # Categorize unused indexes by importance
            critical_table_unused = []
            regular_table_unused = []
            
            for unused in unused_indexes:
                table_name = unused['table']
                index_name = unused['index']
                
                # Skip system-generated primary key indexes
                if '_pkey' in index_name:
                    continue
                    
                index_info = {
                    'table': table_name,
                    'index': index_name,
                    'tuples_read': unused['tuples_read'],
                    'scan_count': unused['scan_count']
                }
                
                if table_name in self.config.key_tables:
                    critical_table_unused.append(index_info)
                else:
                    regular_table_unused.append(index_info)
            
            total_unused = len(critical_table_unused) + len(regular_table_unused)
            
            if critical_table_unused:
                risk_level = 'HIGH'
                status = 'WARNING'
            elif len(regular_table_unused) > 10:  # Many unused indexes
                risk_level = 'MEDIUM'
                status = 'WARNING'
            else:
                risk_level = 'LOW'
                status = 'PASS'
            
            self.create_result(
                check_name=f"unused_indexes_{database}",
                status=status,
                message=f"Found {total_unused} unused indexes ({len(critical_table_unused)} on critical tables)",
                risk_level=risk_level,
                details={
                    'database': database,
                    'critical_table_unused': critical_table_unused,
                    'regular_table_unused': regular_table_unused[:10],  # Limit output
                    'optimization_suggestion': 'Consider dropping unused indexes to improve write performance'
                }
            )
        
        except Exception as e:
            self.create_result(
                check_name=f"unused_indexes_{database}",
                status='ERROR',
                message=f"Unused index detection failed: {str(e)}",
                risk_level='MEDIUM',
                details={'database': database}
            )
    
    def _suggest_missing_indexes(self, database: str):
        """Suggest potentially missing indexes based on table structure and common patterns."""
        self.logger.debug(f"Suggesting missing indexes for {database}")
        
        missing_index_suggestions = []
        
        # Check key tables for common index patterns
        key_tables = [t for t in self.config.key_tables if self.check_table_exists(database, t)]
        
        for table_name in key_tables:
            try:
                with self.db_manager.get_connection(database) as conn:
                    from sqlalchemy import text
                    
                    # Get existing indexes for this table
                    existing_indexes_query = text("""
                        SELECT indexname, indexdef
                        FROM pg_indexes
                        WHERE tablename = :table_name AND schemaname = 'public'
                    """)
                    existing_indexes = conn.execute(existing_indexes_query, {'table_name': table_name}).fetchall()
                    existing_index_defs = [idx[1].lower() for idx in existing_indexes]
                    
                    # Check for common missing index patterns
                    columns = self.get_column_names(database, table_name)
                    
                    # Suggest slug+timestamp composite index if missing
                    if 'slug' in columns and 'timestamp' in columns:
                        slug_timestamp_pattern = f'(slug, timestamp)'
                        if not any(slug_timestamp_pattern in idx_def for idx_def in existing_index_defs):
                            missing_index_suggestions.append({
                                'table': table_name,
                                'suggested_index': f'CREATE INDEX idx_{table_name}_slug_timestamp ON "{table_name}" (slug, timestamp)',
                                'reason': 'Common query pattern for time-series data',
                                'priority': 'HIGH'
                            })
                    
                    # Suggest timestamp index if missing
                    if 'timestamp' in columns:
                        if not any('timestamp' in idx_def for idx_def in existing_index_defs):
                            missing_index_suggestions.append({
                                'table': table_name,
                                'suggested_index': f'CREATE INDEX idx_{table_name}_timestamp ON "{table_name}" (timestamp)',
                                'reason': 'Time-based filtering and ordering',
                                'priority': 'MEDIUM'
                            })
                    
                    # Suggest slug index if missing
                    if 'slug' in columns:
                        if not any('slug' in idx_def and 'timestamp' not in idx_def for idx_def in existing_index_defs):
                            missing_index_suggestions.append({
                                'table': table_name,
                                'suggested_index': f'CREATE INDEX idx_{table_name}_slug ON "{table_name}" (slug)',
                                'reason': 'Cryptocurrency-specific filtering',
                                'priority': 'MEDIUM'
                            })
            
            except Exception as e:
                self.logger.warning(f"Could not analyze indexes for table {table_name}: {e}")
        
        # Report missing index suggestions
        if missing_index_suggestions:
            high_priority = [s for s in missing_index_suggestions if s['priority'] == 'HIGH']
            medium_priority = [s for s in missing_index_suggestions if s['priority'] == 'MEDIUM']
            
            self.create_result(
                check_name=f"missing_indexes_{database}",
                status='WARNING',
                message=f"Suggest {len(missing_index_suggestions)} missing indexes ({len(high_priority)} high priority)",
                risk_level='MEDIUM' if high_priority else 'LOW',
                details={
                    'database': database,
                    'high_priority_suggestions': high_priority,
                    'medium_priority_suggestions': medium_priority,
                    'implementation_note': 'Review and test index suggestions before implementing'
                }
            )
        else:
            self.create_result(
                check_name=f"missing_indexes_{database}",
                status='PASS',
                message="No obvious missing indexes detected",
                risk_level='LOW',
                details={'database': database}
            )
    
    def _analyze_index_maintenance(self, database: str):
        """Analyze index size and maintenance requirements."""
        self.logger.debug(f"Analyzing index maintenance for {database}")
        
        try:
            with self.db_manager.get_connection(database) as conn:
                from sqlalchemy import text
                
                # Get index sizes and bloat information
                index_size_query = text("""
                    SELECT 
                        schemaname,
                        tablename,
                        indexname,
                        pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
                        pg_relation_size(indexrelid) as index_size_bytes
                    FROM pg_stat_user_indexes
                    WHERE schemaname = 'public'
                    ORDER BY pg_relation_size(indexrelid) DESC
                    LIMIT 20
                """)
                
                index_sizes = conn.execute(index_size_query).fetchall()
                
                if not index_sizes:
                    self.create_result(
                        check_name=f"index_maintenance_{database}",
                        status='WARNING',
                        message="No index size information available",
                        risk_level='LOW',
                        details={'database': database}
                    )
                    return
                
                # Analyze index sizes
                total_index_size = sum(row[4] for row in index_sizes)
                large_indexes = [
                    {
                        'table': row[1],
                        'index': row[2],
                        'size': row[3],
                        'size_bytes': row[4]
                    }
                    for row in index_sizes if row[4] > 100 * 1024 * 1024  # > 100MB
                ]
                
                # Calculate total size in MB
                total_size_mb = total_index_size / (1024 * 1024)
                
                # Assessment based on total index size
                if total_size_mb > 10240:  # > 10GB
                    status, risk_level = 'WARNING', 'MEDIUM'
                    message = f"Large index footprint: {total_size_mb:.1f}MB total"
                elif total_size_mb > 1024:  # > 1GB
                    status, risk_level = 'PASS', 'LOW'
                    message = f"Moderate index footprint: {total_size_mb:.1f}MB total"
                else:
                    status, risk_level = 'PASS', 'LOW'
                    message = f"Reasonable index footprint: {total_size_mb:.1f}MB total"
                
                self.create_result(
                    check_name=f"index_maintenance_{database}",
                    status=status,
                    message=message,
                    risk_level=risk_level,
                    metrics={
                        'total_index_size_mb': round(total_size_mb, 1),
                        'large_indexes_count': len(large_indexes)
                    },
                    details={
                        'database': database,
                        'large_indexes': large_indexes,
                        'maintenance_recommendations': [
                            'Consider REINDEX on heavily used indexes periodically',
                            'Monitor index bloat during high-volume operations',
                            'Review large indexes for optimization opportunities'
                        ]
                    }
                )
        
        except Exception as e:
            self.create_result(
                check_name=f"index_maintenance_{database}",
                status='ERROR',
                message=f"Index maintenance analysis failed: {str(e)}",
                risk_level='LOW',
                details={'database': database}
            )
    
    def _compare_cross_database_indexes(self, databases: List[str]):
        """Compare index strategies across databases."""
        self.logger.debug("Comparing cross-database index strategies")
        
        # Extract index metrics from results
        db_index_counts = {}
        
        for result in self.results:
            if result.check_name.startswith('index_inventory_'):
                db_name = result.check_name.split('_')[-1]
                if db_name in databases:
                    db_index_counts[db_name] = result.metrics.get('total_indexes', 0)
        
        if len(db_index_counts) >= 2:
            min_indexes = min(db_index_counts.values())
            max_indexes = max(db_index_counts.values())
            
            if min_indexes > 0:
                index_variance = (max_indexes - min_indexes) / min_indexes
                
                if index_variance > 1.0:  # 100% variance
                    risk_level = 'HIGH'
                    status = 'WARNING'
                elif index_variance > 0.5:  # 50% variance
                    risk_level = 'MEDIUM'
                    status = 'WARNING'
                else:
                    risk_level = 'LOW'
                    status = 'PASS'
                
                self.create_result(
                    check_name="cross_database_index_consistency",
                    status=status,
                    message=f"Index count variance: {index_variance:.1%} across databases",
                    risk_level=risk_level,
                    metrics={'index_variance': round(index_variance, 3)},
                    details={
                        'databases_compared': databases,
                        'index_counts': db_index_counts,
                        'recommendation': 'Consider standardizing index strategies across similar databases'
                    }
                )