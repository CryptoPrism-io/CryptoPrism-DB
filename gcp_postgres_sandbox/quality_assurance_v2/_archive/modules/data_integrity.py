"""
Data Integrity Checker for CryptoPrism-DB QA system v2.
Validates data quality, null ratios, duplicates, and cross-table consistency.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
import statistics

from ..core.base_qa import BaseQAModule, QAResult
from ..core.config import QAConfig
from ..core.database import DatabaseManager

logger = logging.getLogger(__name__)

class DataIntegrityChecker(BaseQAModule):
    """
    Comprehensive data integrity validation for cryptocurrency database.
    Checks null ratios, duplicates, referential integrity, and data consistency.
    """
    
    def __init__(self, config: QAConfig, db_manager: DatabaseManager):
        """Initialize data integrity checker."""
        super().__init__(config, db_manager, "data_integrity")
    
    def run_checks(self, databases: Optional[List[str]] = None) -> List[QAResult]:
        """
        Run all data integrity checks.
        
        Args:
            databases: List of databases to check, or None for all configured databases
            
        Returns:
            List of QA results
        """
        self.start_module()
        
        target_databases = databases or list(self.config.database_configs.keys())
        
        for database in target_databases:
            self.logger.info(f"🔍 Running data integrity checks for database: {database}")
            
            # Basic table existence and row count checks
            self._check_table_existence_and_counts(database)
            
            # Null ratio validation for critical columns
            self._check_null_ratios(database)
            
            # Duplicate detection across key tables
            self._check_for_duplicates(database)
            
            # Timestamp validation
            self._validate_timestamps(database)
            
            # Data freshness checks
            self._check_data_freshness(database)
            
            # Cross-table consistency validation
            self._validate_cross_table_consistency(database)
            
            # Statistical outlier detection
            self._detect_statistical_outliers(database)
        
        # Cross-database comparison (if multiple databases)
        if len(target_databases) > 1:
            self._compare_cross_database_integrity(target_databases)
        
        self.end_module()
        return self.results
    
    def _check_table_existence_and_counts(self, database: str):
        """Check that key tables exist and have reasonable row counts."""
        self.logger.debug(f"Checking table existence and counts for {database}")
        
        key_tables = self.config.key_tables
        min_rows = self.config.thresholds['min_expected_rows']
        
        existing_tables = []
        total_rows = 0
        
        for table_name in key_tables:
            if self.check_table_exists(database, table_name):
                row_count = self.get_table_row_count(database, table_name)
                existing_tables.append({'table': table_name, 'rows': row_count})
                total_rows += row_count
                
                # Individual table row count validation
                if row_count == 0:
                    self.create_result(
                        check_name=f"table_row_count_{database}_{table_name}",
                        status='FAIL',
                        message=f"Table {table_name} is empty (0 rows)",
                        risk_level='HIGH',
                        details={'database': database, 'table': table_name, 'row_count': row_count}
                    )
                elif row_count < min_rows:
                    self.create_result(
                        check_name=f"table_row_count_{database}_{table_name}",
                        status='WARNING',
                        message=f"Table {table_name} has only {row_count} rows (expected ≥{min_rows})",
                        risk_level='MEDIUM',
                        details={'database': database, 'table': table_name, 'row_count': row_count}
                    )
                else:
                    self.create_result(
                        check_name=f"table_row_count_{database}_{table_name}",
                        status='PASS',
                        message=f"Table {table_name} has {row_count:,} rows",
                        risk_level='LOW',
                        metrics={'row_count': row_count},
                        details={'database': database, 'table': table_name}
                    )
            else:
                self.create_result(
                    check_name=f"table_existence_{database}_{table_name}",
                    status='FAIL',
                    message=f"Critical table {table_name} does not exist",
                    risk_level='CRITICAL',
                    details={'database': database, 'table': table_name}
                )
        
        # Overall table summary
        self.create_result(
            check_name=f"table_summary_{database}",
            status='PASS' if len(existing_tables) == len(key_tables) else 'WARNING',
            message=f"Database has {len(existing_tables)}/{len(key_tables)} key tables with {total_rows:,} total rows",
            risk_level='LOW' if len(existing_tables) == len(key_tables) else 'MEDIUM',
            metrics={'total_tables': len(existing_tables), 'total_rows': total_rows},
            details={'database': database, 'table_details': existing_tables}
        )
    
    def _check_null_ratios(self, database: str):
        """Check null ratios for critical columns across key tables."""
        self.logger.debug(f"Checking null ratios for {database}")
        
        critical_columns = self.config.critical_columns
        key_tables = [t for t in self.config.key_tables if self.check_table_exists(database, t)]
        
        for table_name in key_tables:
            columns_in_table = self.get_column_names(database, table_name)
            
            for column in critical_columns:
                if column not in columns_in_table:
                    continue
                
                try:
                    with self.db_manager.get_connection(database) as conn:
                        from sqlalchemy import text
                        
                        # Get null statistics
                        null_query = text(f"""
                            SELECT 
                                COUNT(*) as total_rows,
                                COUNT(CASE WHEN "{column}" IS NULL THEN 1 END) as null_count
                            FROM public."{table_name}"
                        """)
                        result = conn.execute(null_query).fetchone()
                        
                        total_rows = result[0]
                        null_count = result[1]
                        null_ratio = null_count / total_rows if total_rows > 0 else 0
                        
                        # Classify null ratio risk
                        risk_level = self.config.classify_risk_level('null_ratio', null_ratio)
                        
                        if null_ratio > self.config.thresholds['null_ratio_high']:
                            status = 'FAIL'
                        elif null_ratio > self.config.thresholds['null_ratio_medium']:
                            status = 'WARNING'
                        else:
                            status = 'PASS'
                        
                        self.create_result(
                            check_name=f"null_ratio_{database}_{table_name}_{column}",
                            status=status,
                            message=f"Column {table_name}.{column}: {null_ratio:.1%} NULL values ({null_count:,}/{total_rows:,})",
                            risk_level=risk_level,
                            metrics={
                                'null_ratio': round(null_ratio, 4),
                                'null_count': null_count,
                                'total_rows': total_rows
                            },
                            details={
                                'database': database,
                                'table': table_name,
                                'column': column
                            }
                        )
                
                except Exception as e:
                    self.create_result(
                        check_name=f"null_ratio_{database}_{table_name}_{column}",
                        status='ERROR',
                        message=f"Failed to check null ratio for {table_name}.{column}: {str(e)}",
                        risk_level='MEDIUM',
                        details={'database': database, 'table': table_name, 'column': column}
                    )
    
    def _check_for_duplicates(self, database: str):
        """Check for duplicate records in key tables."""
        self.logger.debug(f"Checking for duplicates in {database}")
        
        key_tables = [t for t in self.config.key_tables if self.check_table_exists(database, t)]
        
        for table_name in key_tables:
            columns = self.get_column_names(database, table_name)
            
            # Check for duplicates based on slug+timestamp if both exist
            if 'slug' in columns and 'timestamp' in columns:
                try:
                    with self.db_manager.get_connection(database) as conn:
                        from sqlalchemy import text
                        
                        duplicate_query = text(f"""
                            SELECT 
                                COUNT(*) as total_rows,
                                COUNT(DISTINCT (slug, timestamp)) as unique_combinations,
                                COUNT(*) - COUNT(DISTINCT (slug, timestamp)) as duplicates
                            FROM public."{table_name}"
                        """)
                        result = conn.execute(duplicate_query).fetchone()
                        
                        total_rows = result[0]
                        unique_combinations = result[1]
                        duplicates = result[2]
                        
                        if duplicates > 0:
                            # Get sample duplicates for analysis
                            sample_query = text(f"""
                                SELECT slug, timestamp, COUNT(*) as duplicate_count
                                FROM public."{table_name}"
                                GROUP BY slug, timestamp
                                HAVING COUNT(*) > 1
                                ORDER BY COUNT(*) DESC
                                LIMIT 10
                            """)
                            sample_duplicates = conn.execute(sample_query).fetchall()
                            
                            duplicate_samples = [
                                {'slug': row[0], 'timestamp': str(row[1]), 'count': row[2]}
                                for row in sample_duplicates
                            ]
                            
                            self.create_result(
                                check_name=f"duplicates_{database}_{table_name}",
                                status='FAIL',
                                message=f"Found {duplicates:,} duplicate records in {table_name}",
                                risk_level='CRITICAL' if duplicates > total_rows * 0.1 else 'HIGH',
                                metrics={
                                    'duplicate_count': duplicates,
                                    'total_rows': total_rows,
                                    'duplicate_percentage': round(duplicates / total_rows * 100, 2)
                                },
                                details={
                                    'database': database,
                                    'table': table_name,
                                    'sample_duplicates': duplicate_samples
                                }
                            )
                        else:
                            self.create_result(
                                check_name=f"duplicates_{database}_{table_name}",
                                status='PASS',
                                message=f"No duplicates found in {table_name}",
                                risk_level='LOW',
                                metrics={'total_rows': total_rows, 'unique_combinations': unique_combinations},
                                details={'database': database, 'table': table_name}
                            )
                
                except Exception as e:
                    self.create_result(
                        check_name=f"duplicates_{database}_{table_name}",
                        status='ERROR',
                        message=f"Duplicate check failed for {table_name}: {str(e)}",
                        risk_level='MEDIUM',
                        details={'database': database, 'table': table_name}
                    )
    
    def _validate_timestamps(self, database: str):
        """Validate timestamp ranges and consistency across tables."""
        self.logger.debug(f"Validating timestamps for {database}")
        
        key_tables = [t for t in self.config.key_tables if self.check_table_exists(database, t)]
        
        for table_name in key_tables:
            validation_result = self.validate_timestamp_range(database, table_name)
            
            if validation_result['status'] == 'VALID':
                self.create_result(
                    check_name=f"timestamp_validation_{database}_{table_name}",
                    status='PASS',
                    message=validation_result['message'],
                    risk_level=validation_result['risk_level'],
                    details={
                        'database': database,
                        'table': table_name,
                        **{k: v for k, v in validation_result.items() if k not in ['status', 'message', 'risk_level']}
                    }
                )
            else:
                status = 'FAIL' if validation_result['risk_level'] == 'CRITICAL' else 'WARNING'
                self.create_result(
                    check_name=f"timestamp_validation_{database}_{table_name}",
                    status=status,
                    message=validation_result['message'],
                    risk_level=validation_result['risk_level'],
                    details={
                        'database': database,
                        'table': table_name,
                        **{k: v for k, v in validation_result.items() if k not in ['status', 'message', 'risk_level']}
                    }
                )
    
    def _check_data_freshness(self, database: str):
        """Check data freshness based on latest timestamps."""
        self.logger.debug(f"Checking data freshness for {database}")
        
        from datetime import datetime, timedelta
        
        key_tables = [t for t in self.config.key_tables if self.check_table_exists(database, t)]
        current_time = datetime.utcnow()
        
        for table_name in key_tables:
            try:
                with self.db_manager.get_connection(database) as conn:
                    from sqlalchemy import text
                    
                    # Find timestamp column
                    timestamp_col_query = text("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = :table_name 
                          AND table_schema = 'public'
                          AND data_type LIKE '%timestamp%'
                        ORDER BY ordinal_position
                        LIMIT 1
                    """)
                    timestamp_col_result = conn.execute(timestamp_col_query, {'table_name': table_name}).fetchone()
                    
                    if not timestamp_col_result:
                        continue
                    
                    timestamp_col = timestamp_col_result[0]
                    
                    # Get latest timestamp
                    latest_query = text(f"""
                        SELECT MAX("{timestamp_col}") as latest_timestamp
                        FROM public."{table_name}"
                    """)
                    latest_result = conn.execute(latest_query).fetchone()
                    
                    if not latest_result or not latest_result[0]:
                        self.create_result(
                            check_name=f"data_freshness_{database}_{table_name}",
                            status='FAIL',
                            message=f"No valid timestamps found in {table_name}",
                            risk_level='HIGH',
                            details={'database': database, 'table': table_name}
                        )
                        continue
                    
                    latest_timestamp = latest_result[0]
                    time_diff = current_time - latest_timestamp
                    hours_old = time_diff.total_seconds() / 3600
                    
                    # Freshness thresholds based on table type
                    if table_name.startswith('FE_'):
                        # Feature tables should be updated daily
                        if hours_old > 48:  # 2 days
                            status, risk_level = 'FAIL', 'HIGH'
                        elif hours_old > 24:  # 1 day
                            status, risk_level = 'WARNING', 'MEDIUM'
                        else:
                            status, risk_level = 'PASS', 'LOW'
                    else:
                        # Raw data tables should be updated more frequently
                        if hours_old > 24:  # 1 day
                            status, risk_level = 'FAIL', 'HIGH'
                        elif hours_old > 6:  # 6 hours
                            status, risk_level = 'WARNING', 'MEDIUM'
                        else:
                            status, risk_level = 'PASS', 'LOW'
                    
                    self.create_result(
                        check_name=f"data_freshness_{database}_{table_name}",
                        status=status,
                        message=f"Table {table_name}: data is {hours_old:.1f} hours old",
                        risk_level=risk_level,
                        metrics={'hours_since_update': round(hours_old, 1)},
                        details={
                            'database': database,
                            'table': table_name,
                            'latest_timestamp': str(latest_timestamp),
                            'current_time': current_time.isoformat()
                        }
                    )
            
            except Exception as e:
                self.create_result(
                    check_name=f"data_freshness_{database}_{table_name}",
                    status='ERROR',
                    message=f"Data freshness check failed for {table_name}: {str(e)}",
                    risk_level='MEDIUM',
                    details={'database': database, 'table': table_name}
                )
    
    def _validate_cross_table_consistency(self, database: str):
        """Validate data consistency across related tables."""
        self.logger.debug(f"Validating cross-table consistency for {database}")
        
        fe_tables = [t for t in self.config.fe_tables if self.check_table_exists(database, t)]
        
        if len(fe_tables) < 2:
            return
        
        # Check timestamp consistency across FE tables
        fe_timestamps = {}
        
        for table_name in fe_tables:
            try:
                with self.db_manager.get_connection(database) as conn:
                    from sqlalchemy import text
                    
                    timestamp_query = text(f"""
                        SELECT DISTINCT timestamp
                        FROM public."{table_name}"
                        ORDER BY timestamp DESC
                        LIMIT 5
                    """)
                    timestamps = [str(row[0]) for row in conn.execute(timestamp_query)]
                    fe_timestamps[table_name] = timestamps
            
            except Exception as e:
                self.logger.warning(f"Could not get timestamps for {table_name}: {e}")
        
        if len(fe_timestamps) > 1:
            # Find common latest timestamp
            all_latest_timestamps = [ts[0] for ts in fe_timestamps.values() if ts]
            
            if all_latest_timestamps:
                most_common_timestamp = max(set(all_latest_timestamps), key=all_latest_timestamps.count)
                mismatched_tables = []
                
                for table_name, timestamps in fe_timestamps.items():
                    if not timestamps or timestamps[0] != most_common_timestamp:
                        mismatched_tables.append({
                            'table': table_name,
                            'latest_timestamp': timestamps[0] if timestamps else None
                        })
                
                if mismatched_tables:
                    self.create_result(
                        check_name=f"fe_timestamp_consistency_{database}",
                        status='WARNING',
                        message=f"{len(mismatched_tables)} FE tables have mismatched timestamps",
                        risk_level='MEDIUM' if len(mismatched_tables) <= 2 else 'HIGH',
                        details={
                            'database': database,
                            'expected_timestamp': most_common_timestamp,
                            'mismatched_tables': mismatched_tables
                        }
                    )
                else:
                    self.create_result(
                        check_name=f"fe_timestamp_consistency_{database}",
                        status='PASS',
                        message=f"All {len(fe_timestamps)} FE tables have consistent timestamps",
                        risk_level='LOW',
                        details={'database': database, 'consistent_timestamp': most_common_timestamp}
                    )
    
    def _detect_statistical_outliers(self, database: str):
        """Detect statistical outliers in key numerical columns."""
        self.logger.debug(f"Detecting statistical outliers for {database}")
        
        # Focus on key tables with numerical data
        numerical_checks = [
            {'table': '1K_coins_ohlcv', 'columns': ['close', 'volume']},
            {'table': 'FE_DMV_ALL', 'columns': ['bullish', 'bearish', 'Momentum_Score']},
        ]
        
        for check_config in numerical_checks:
            table_name = check_config['table']
            columns = check_config['columns']
            
            if not self.check_table_exists(database, table_name):
                continue
                
            for column in columns:
                try:
                    with self.db_manager.get_connection(database) as conn:
                        from sqlalchemy import text
                        
                        # Get basic statistics
                        stats_query = text(f"""
                            SELECT 
                                COUNT(*) as count,
                                AVG("{column}") as mean,
                                STDDEV("{column}") as stddev,
                                MIN("{column}") as min_val,
                                MAX("{column}") as max_val,
                                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{column}") as q1,
                                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{column}") as q3
                            FROM public."{table_name}"
                            WHERE "{column}" IS NOT NULL
                        """)
                        result = conn.execute(stats_query).fetchone()
                        
                        if not result or result[0] == 0:
                            continue
                        
                        count, mean, stddev, min_val, max_val, q1, q3 = result
                        
                        # Calculate IQR-based outlier bounds
                        iqr = q3 - q1 if q1 and q3 else 0
                        lower_bound = q1 - 1.5 * iqr if q1 and iqr else None
                        upper_bound = q3 + 1.5 * iqr if q3 and iqr else None
                        
                        # Count outliers
                        if lower_bound is not None and upper_bound is not None:
                            outlier_query = text(f"""
                                SELECT COUNT(*) 
                                FROM public."{table_name}"
                                WHERE "{column}" < :lower OR "{column}" > :upper
                            """)
                            outlier_count = conn.execute(outlier_query, {
                                'lower': lower_bound,
                                'upper': upper_bound
                            }).scalar()
                            
                            outlier_percentage = outlier_count / count * 100 if count > 0 else 0
                            
                            # Risk assessment
                            if outlier_percentage > 10:  # More than 10% outliers
                                status, risk_level = 'WARNING', 'MEDIUM'
                            elif outlier_percentage > 20:  # More than 20% outliers
                                status, risk_level = 'FAIL', 'HIGH'
                            else:
                                status, risk_level = 'PASS', 'LOW'
                            
                            self.create_result(
                                check_name=f"outlier_detection_{database}_{table_name}_{column}",
                                status=status,
                                message=f"Column {table_name}.{column}: {outlier_percentage:.1f}% outliers ({outlier_count:,}/{count:,})",
                                risk_level=risk_level,
                                metrics={
                                    'outlier_count': outlier_count,
                                    'outlier_percentage': round(outlier_percentage, 2),
                                    'mean': round(float(mean), 4) if mean else None,
                                    'stddev': round(float(stddev), 4) if stddev else None,
                                    'min': float(min_val) if min_val else None,
                                    'max': float(max_val) if max_val else None
                                },
                                details={
                                    'database': database,
                                    'table': table_name,
                                    'column': column,
                                    'lower_bound': float(lower_bound),
                                    'upper_bound': float(upper_bound)
                                }
                            )
                
                except Exception as e:
                    self.create_result(
                        check_name=f"outlier_detection_{database}_{table_name}_{column}",
                        status='ERROR',
                        message=f"Outlier detection failed for {table_name}.{column}: {str(e)}",
                        risk_level='LOW',
                        details={'database': database, 'table': table_name, 'column': column}
                    )
    
    def _compare_cross_database_integrity(self, databases: List[str]):
        """Compare data integrity metrics across databases."""
        self.logger.debug("Comparing cross-database integrity metrics")
        
        # Extract row count metrics from results
        db_row_counts = {}
        
        for result in self.results:
            if result.check_name.startswith('table_summary_'):
                db_name = result.check_name.split('_')[-1]
                if db_name in databases:
                    db_row_counts[db_name] = result.metrics.get('total_rows', 0)
        
        if len(db_row_counts) >= 2:
            min_rows = min(db_row_counts.values())
            max_rows = max(db_row_counts.values())
            
            if min_rows > 0:
                row_variance = (max_rows - min_rows) / min_rows
                
                if row_variance > 0.5:  # 50% variance
                    risk_level = 'HIGH'
                    status = 'WARNING'
                elif row_variance > 0.2:  # 20% variance
                    risk_level = 'MEDIUM'
                    status = 'WARNING'
                else:
                    risk_level = 'LOW'
                    status = 'PASS'
                
                self.create_result(
                    check_name="cross_database_row_consistency",
                    status=status,
                    message=f"Row count variance: {row_variance:.1%} across databases",
                    risk_level=risk_level,
                    metrics={'row_variance': round(row_variance, 3)},
                    details={
                        'databases_compared': databases,
                        'row_counts': db_row_counts
                    }
                )