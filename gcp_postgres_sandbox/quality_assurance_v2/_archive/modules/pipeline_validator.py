"""
Pipeline Validation Module for CryptoPrism-DB QA system v2.
Validates ETL pipeline execution, data flow integrity, and processing sequence.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta

from ..core.base_qa import BaseQAModule, QAResult
from ..core.config import QAConfig
from ..core.database import DatabaseManager

logger = logging.getLogger(__name__)

class PipelineValidator(BaseQAModule):
    """
    Validates the CryptoPrism-DB data pipeline execution and integrity.
    Ensures proper ETL sequence, data flow, and processing completeness.
    """
    
    def __init__(self, config: QAConfig, db_manager: DatabaseManager):
        """Initialize pipeline validator."""
        super().__init__(config, db_manager, "pipeline_validator")
        
        # Define the expected pipeline sequence
        self.pipeline_sequence = [
            'gcp_dmv_met.py',    # Fundamental metrics
            'gcp_dmv_tvv.py',    # Volume/value analysis
            'gcp_dmv_pct.py',    # Risk metrics
            'gcp_dmv_mom.py',    # Momentum indicators
            'gcp_dmv_osc.py',    # Oscillators
            'gcp_dmv_rat.py',    # Financial ratios
            'gcp_dmv_core.py'    # Final aggregation (MUST run last)
        ]
        
        # Expected table outputs for each pipeline stage
        self.pipeline_outputs = {
            'gcp_dmv_met.py': ['FE_METRICS', 'FE_METRICS_SIGNAL'],
            'gcp_dmv_tvv.py': ['FE_TVV', 'FE_TVV_SIGNALS'],
            'gcp_dmv_pct.py': ['FE_PCT'],
            'gcp_dmv_mom.py': ['FE_MOMENTUM', 'FE_MOMENTUM_SIGNALS'],
            'gcp_dmv_osc.py': ['FE_OSCILLATORS', 'FE_OSCILLATORS_SIGNALS'],
            'gcp_dmv_rat.py': ['FE_RATIOS', 'FE_RATIOS_SIGNALS'],
            'gcp_dmv_core.py': ['FE_DMV_ALL', 'FE_DMV_SCORES']
        }
    
    def run_checks(self, databases: Optional[List[str]] = None) -> List[QAResult]:
        """
        Run all pipeline validation checks.
        
        Args:
            databases: List of databases to check, or None for all configured databases
            
        Returns:
            List of QA results
        """
        self.start_module()
        
        target_databases = databases or list(self.config.database_configs.keys())
        
        for database in target_databases:
            self.logger.info(f"🔍 Running pipeline validation for database: {database}")
            
            # Validate pipeline execution completeness
            self._validate_pipeline_execution(database)
            
            # Check processing sequence integrity
            self._validate_processing_sequence(database)
            
            # Validate data flow between stages
            self._validate_data_flow_integrity(database)
            
            # Check for data loss during processing
            self._detect_data_loss(database)
            
            # Validate signal generation consistency
            self._validate_signal_generation(database)
            
            # Cross-stage timestamp consistency
            self._validate_cross_stage_timestamps(database)
        
        # Cross-database pipeline comparison
        if len(target_databases) > 1:
            self._compare_cross_database_pipelines(target_databases)
        
        self.end_module()
        return self.results
    
    def _validate_pipeline_execution(self, database: str):
        """Validate that all pipeline stages have been executed successfully."""
        self.logger.debug(f"Validating pipeline execution for {database}")
        
        expected_tables = []
        for script, tables in self.pipeline_outputs.items():
            expected_tables.extend(tables)
        
        existing_tables = []
        missing_tables = []
        empty_tables = []
        
        for table_name in expected_tables:
            if self.check_table_exists(database, table_name):
                existing_tables.append(table_name)
                
                # Check if table has data
                row_count = self.get_table_row_count(database, table_name)
                if row_count == 0:
                    empty_tables.append(table_name)
            else:
                missing_tables.append(table_name)
        
        # Assess pipeline execution completeness
        completion_rate = len(existing_tables) / len(expected_tables) * 100
        
        if missing_tables:
            status = 'FAIL'
            risk_level = 'CRITICAL' if len(missing_tables) > 5 else 'HIGH'
            message = f"Pipeline incomplete: {len(missing_tables)} tables missing ({completion_rate:.1f}% complete)"
        elif empty_tables:
            status = 'WARNING'
            risk_level = 'HIGH' if len(empty_tables) > 3 else 'MEDIUM'
            message = f"Pipeline executed but {len(empty_tables)} tables are empty ({completion_rate:.1f}% complete)"
        else:
            status = 'PASS'
            risk_level = 'LOW'
            message = f"Pipeline execution complete: all {len(expected_tables)} tables present with data"
        
        self.create_result(
            check_name=f"pipeline_execution_{database}",
            status=status,
            message=message,
            risk_level=risk_level,
            metrics={
                'completion_percentage': round(completion_rate, 1),
                'existing_tables': len(existing_tables),
                'missing_tables': len(missing_tables),
                'empty_tables': len(empty_tables)
            },
            details={
                'database': database,
                'missing_tables': missing_tables,
                'empty_tables': empty_tables,
                'expected_total': len(expected_tables)
            }
        )
    
    def _validate_processing_sequence(self, database: str):
        """Validate that pipeline stages were executed in the correct sequence."""
        self.logger.debug(f"Validating processing sequence for {database}")
        
        # Check that gcp_dmv_core.py ran last by comparing timestamps
        core_tables = self.pipeline_outputs['gcp_dmv_core.py']
        other_fe_tables = []
        
        for script, tables in self.pipeline_outputs.items():
            if script != 'gcp_dmv_core.py':
                other_fe_tables.extend(tables)
        
        try:
            sequence_violations = []
            core_timestamp = None
            
            # Get timestamp from core tables (should be the latest)
            for table in core_tables:
                if self.check_table_exists(database, table):
                    with self.db_manager.get_connection(database) as conn:
                        from sqlalchemy import text
                        
                        timestamp_query = text(f"""
                            SELECT MAX(timestamp) as latest_timestamp
                            FROM public."{table}"
                        """)
                        result = conn.execute(timestamp_query).fetchone()
                        if result and result[0]:
                            core_timestamp = result[0]
                            break
            
            if not core_timestamp:
                self.create_result(
                    check_name=f"processing_sequence_{database}",
                    status='FAIL',
                    message="Cannot validate sequence: gcp_dmv_core.py output tables missing or empty",
                    risk_level='CRITICAL',
                    details={'database': database}
                )
                return
            
            # Check other FE tables have timestamps before or equal to core timestamp
            for table in other_fe_tables:
                if self.check_table_exists(database, table):
                    try:
                        with self.db_manager.get_connection(database) as conn:
                            timestamp_query = text(f"""
                                SELECT MAX(timestamp) as latest_timestamp
                                FROM public."{table}"
                            """)
                            result = conn.execute(timestamp_query).fetchone()
                            
                            if result and result[0]:
                                table_timestamp = result[0]
                                
                                # For FE tables, timestamps should match exactly (same processing run)
                                if table_timestamp > core_timestamp:
                                    sequence_violations.append({
                                        'table': table,
                                        'table_timestamp': str(table_timestamp),
                                        'core_timestamp': str(core_timestamp),
                                        'issue': 'Table timestamp is newer than core output'
                                    })
                    except Exception as e:
                        self.logger.warning(f"Could not check timestamp for {table}: {e}")
            
            # Report sequence validation results
            if sequence_violations:
                self.create_result(
                    check_name=f"processing_sequence_{database}",
                    status='FAIL',
                    message=f"Processing sequence violations detected in {len(sequence_violations)} tables",
                    risk_level='HIGH',
                    details={
                        'database': database,
                        'violations': sequence_violations,
                        'core_timestamp': str(core_timestamp)
                    }
                )
            else:
                self.create_result(
                    check_name=f"processing_sequence_{database}",
                    status='PASS',
                    message="Processing sequence validation passed: gcp_dmv_core.py executed last",
                    risk_level='LOW',
                    details={
                        'database': database,
                        'core_timestamp': str(core_timestamp),
                        'tables_validated': len(other_fe_tables)
                    }
                )
        
        except Exception as e:
            self.create_result(
                check_name=f"processing_sequence_{database}",
                status='ERROR',
                message=f"Processing sequence validation failed: {str(e)}",
                risk_level='MEDIUM',
                details={'database': database}
            )
    
    def _validate_data_flow_integrity(self, database: str):
        """Validate data flow integrity between pipeline stages."""
        self.logger.debug(f"Validating data flow integrity for {database}")
        
        # Check that input data (1K_coins_ohlcv, crypto_listings) flows to output tables
        input_tables = ['1K_coins_ohlcv', 'crypto_listings_latest_1000']
        output_tables = ['FE_DMV_ALL', 'FE_DMV_SCORES']
        
        flow_issues = []
        
        try:
            # Check cryptocurrency coverage consistency
            for input_table in input_tables:
                if not self.check_table_exists(database, input_table):
                    continue
                
                with self.db_manager.get_connection(database) as conn:
                    from sqlalchemy import text
                    
                    # Get unique slugs from input table
                    input_slugs_query = text(f"""
                        SELECT COUNT(DISTINCT slug) as unique_slugs
                        FROM public."{input_table}"
                        WHERE slug IS NOT NULL
                    """)
                    input_result = conn.execute(input_slugs_query).fetchone()
                    input_slug_count = input_result[0] if input_result else 0
                    
                    # Check coverage in output tables
                    for output_table in output_tables:
                        if not self.check_table_exists(database, output_table):
                            continue
                        
                        output_slugs_query = text(f"""
                            SELECT COUNT(DISTINCT slug) as unique_slugs
                            FROM public."{output_table}"
                            WHERE slug IS NOT NULL
                        """)
                        output_result = conn.execute(output_slugs_query).fetchone()
                        output_slug_count = output_result[0] if output_result else 0
                        
                        # Calculate coverage ratio
                        if input_slug_count > 0:
                            coverage_ratio = output_slug_count / input_slug_count
                            
                            if coverage_ratio < 0.8:  # Less than 80% coverage
                                flow_issues.append({
                                    'input_table': input_table,
                                    'output_table': output_table,
                                    'input_slugs': input_slug_count,
                                    'output_slugs': output_slug_count,
                                    'coverage_ratio': round(coverage_ratio, 3),
                                    'issue': 'Low cryptocurrency coverage in output'
                                })
            
            # Report data flow integrity
            if flow_issues:
                avg_coverage = sum(issue['coverage_ratio'] for issue in flow_issues) / len(flow_issues)
                
                self.create_result(
                    check_name=f"data_flow_integrity_{database}",
                    status='WARNING',
                    message=f"Data flow issues detected: {avg_coverage:.1%} average coverage",
                    risk_level='HIGH' if avg_coverage < 0.5 else 'MEDIUM',
                    details={
                        'database': database,
                        'flow_issues': flow_issues,
                        'average_coverage': round(avg_coverage, 3)
                    }
                )
            else:
                self.create_result(
                    check_name=f"data_flow_integrity_{database}",
                    status='PASS',
                    message="Data flow integrity validated: good coverage from input to output tables",
                    risk_level='LOW',
                    details={'database': database}
                )
        
        except Exception as e:
            self.create_result(
                check_name=f"data_flow_integrity_{database}",
                status='ERROR',
                message=f"Data flow integrity validation failed: {str(e)}",
                risk_level='MEDIUM',
                details={'database': database}
            )
    
    def _detect_data_loss(self, database: str):
        """Detect potential data loss during pipeline processing."""
        self.logger.debug(f"Detecting data loss for {database}")
        
        try:
            data_loss_indicators = []
            
            # Compare row counts between related tables
            table_comparisons = [
                {'base': '1K_coins_ohlcv', 'derived': 'FE_DMV_ALL', 'relationship': 'aggregated'},
                {'base': 'crypto_listings_latest_1000', 'derived': 'FE_DMV_SCORES', 'relationship': 'processed'}
            ]
            
            for comparison in table_comparisons:
                base_table = comparison['base']
                derived_table = comparison['derived']
                
                if not (self.check_table_exists(database, base_table) and 
                       self.check_table_exists(database, derived_table)):
                    continue
                
                with self.db_manager.get_connection(database) as conn:
                    from sqlalchemy import text
                    
                    # Get unique slug counts
                    base_count_query = text(f"""
                        SELECT COUNT(DISTINCT slug) as unique_slugs
                        FROM public."{base_table}"
                        WHERE slug IS NOT NULL
                    """)
                    base_count = conn.execute(base_count_query).scalar()
                    
                    derived_count_query = text(f"""
                        SELECT COUNT(DISTINCT slug) as unique_slugs
                        FROM public."{derived_table}"
                        WHERE slug IS NOT NULL
                    """)
                    derived_count = conn.execute(derived_count_query).scalar()
                    
                    if base_count and derived_count:
                        retention_rate = derived_count / base_count
                        
                        # Expected retention rates vary by table relationship
                        if comparison['relationship'] == 'aggregated':
                            min_retention = 0.95  # Should retain 95%+ for aggregated data
                        else:
                            min_retention = 0.90  # Should retain 90%+ for processed data
                        
                        if retention_rate < min_retention:
                            data_loss_indicators.append({
                                'base_table': base_table,
                                'derived_table': derived_table,
                                'base_count': base_count,
                                'derived_count': derived_count,
                                'retention_rate': round(retention_rate, 3),
                                'data_lost': base_count - derived_count,
                                'relationship': comparison['relationship']
                            })
            
            # Report data loss findings
            if data_loss_indicators:
                total_lost = sum(indicator['data_lost'] for indicator in data_loss_indicators)
                avg_retention = sum(indicator['retention_rate'] for indicator in data_loss_indicators) / len(data_loss_indicators)
                
                self.create_result(
                    check_name=f"data_loss_detection_{database}",
                    status='WARNING',
                    message=f"Potential data loss detected: {total_lost} cryptocurrencies lost, {avg_retention:.1%} average retention",
                    risk_level='HIGH' if avg_retention < 0.8 else 'MEDIUM',
                    details={
                        'database': database,
                        'data_loss_indicators': data_loss_indicators,
                        'total_data_lost': total_lost,
                        'average_retention': round(avg_retention, 3)
                    }
                )
            else:
                self.create_result(
                    check_name=f"data_loss_detection_{database}",
                    status='PASS',
                    message="No significant data loss detected during pipeline processing",
                    risk_level='LOW',
                    details={'database': database}
                )
        
        except Exception as e:
            self.create_result(
                check_name=f"data_loss_detection_{database}",
                status='ERROR',
                message=f"Data loss detection failed: {str(e)}",
                risk_level='MEDIUM',
                details={'database': database}
            )
    
    def _validate_signal_generation(self, database: str):
        """Validate that signal generation is producing reasonable results."""
        self.logger.debug(f"Validating signal generation for {database}")
        
        signal_tables = ['FE_MOMENTUM_SIGNALS', 'FE_OSCILLATORS_SIGNALS', 'FE_RATIOS_SIGNALS', 
                        'FE_TVV_SIGNALS', 'FE_DMV_ALL']
        
        signal_validation_results = []
        
        for table in signal_tables:
            if not self.check_table_exists(database, table):
                continue
            
            try:
                with self.db_manager.get_connection(database) as conn:
                    from sqlalchemy import text
                    
                    if table == 'FE_DMV_ALL':
                        # Validate aggregated signals
                        signal_query = text(f"""
                            SELECT 
                                COUNT(*) as total_records,
                                AVG(bullish) as avg_bullish,
                                AVG(bearish) as avg_bearish,
                                AVG(neutral) as avg_neutral,
                                COUNT(CASE WHEN bullish > bearish THEN 1 END) as bullish_dominant,
                                COUNT(CASE WHEN bearish > bullish THEN 1 END) as bearish_dominant
                            FROM public."{table}"
                            WHERE timestamp = (SELECT MAX(timestamp) FROM public."{table}")
                        """)
                    else:
                        # Count signal distributions for individual signal tables
                        # This is a simplified check - could be enhanced based on specific column patterns
                        signal_query = text(f"""
                            SELECT 
                                COUNT(*) as total_records,
                                COUNT(DISTINCT slug) as unique_slugs
                            FROM public."{table}"
                            WHERE timestamp = (SELECT MAX(timestamp) FROM public."{table}")
                        """)
                    
                    result = conn.execute(signal_query).fetchone()
                    
                    if result:
                        if table == 'FE_DMV_ALL':
                            total_records, avg_bullish, avg_bearish, avg_neutral, bullish_dominant, bearish_dominant = result
                            
                            # Validate signal distributions
                            issues = []
                            
                            # Check for reasonable signal balance
                            if bullish_dominant + bearish_dominant < total_records * 0.1:  # Less than 10% have directional signals
                                issues.append("Very few directional signals generated")
                            
                            # Check average signal values are reasonable
                            if avg_bullish is not None and avg_bullish < 1:
                                issues.append(f"Low average bullish signals: {avg_bullish:.2f}")
                            
                            if avg_bearish is not None and avg_bearish < 1:
                                issues.append(f"Low average bearish signals: {avg_bearish:.2f}")
                            
                            signal_validation_results.append({
                                'table': table,
                                'total_records': total_records,
                                'avg_bullish': round(float(avg_bullish), 2) if avg_bullish else None,
                                'avg_bearish': round(float(avg_bearish), 2) if avg_bearish else None,
                                'bullish_dominant': bullish_dominant,
                                'bearish_dominant': bearish_dominant,
                                'issues': issues
                            })
                        else:
                            total_records, unique_slugs = result
                            
                            issues = []
                            if total_records == 0:
                                issues.append("No signal records found")
                            elif unique_slugs < 100:  # Expect signals for at least 100 cryptocurrencies
                                issues.append(f"Signals for only {unique_slugs} cryptocurrencies")
                            
                            signal_validation_results.append({
                                'table': table,
                                'total_records': total_records,
                                'unique_slugs': unique_slugs,
                                'issues': issues
                            })
            
            except Exception as e:
                signal_validation_results.append({
                    'table': table,
                    'error': str(e),
                    'issues': ['Failed to validate signals']
                })
        
        # Assess overall signal generation quality
        all_issues = []
        for result in signal_validation_results:
            all_issues.extend(result.get('issues', []))
        
        if all_issues:
            self.create_result(
                check_name=f"signal_generation_{database}",
                status='WARNING',
                message=f"Signal generation issues detected: {len(all_issues)} problems found",
                risk_level='MEDIUM' if len(all_issues) < 5 else 'HIGH',
                details={
                    'database': database,
                    'validation_results': signal_validation_results,
                    'total_issues': len(all_issues),
                    'issue_summary': all_issues
                }
            )
        else:
            self.create_result(
                check_name=f"signal_generation_{database}",
                status='PASS',
                message=f"Signal generation validated: {len(signal_validation_results)} tables producing reasonable signals",
                risk_level='LOW',
                details={
                    'database': database,
                    'validation_results': signal_validation_results
                }
            )
    
    def _validate_cross_stage_timestamps(self, database: str):
        """Validate timestamp consistency across pipeline stages."""
        self.logger.debug(f"Validating cross-stage timestamps for {database}")
        
        fe_tables = [t for t in self.config.fe_tables if self.check_table_exists(database, t)]
        
        if len(fe_tables) < 2:
            return
        
        timestamp_consistency = []
        reference_timestamp = None
        
        try:
            # Get reference timestamp from first available table
            for table in fe_tables:
                with self.db_manager.get_connection(database) as conn:
                    from sqlalchemy import text
                    
                    ref_query = text(f"""
                        SELECT MAX(timestamp) as latest_timestamp
                        FROM public."{table}"
                    """)
                    result = conn.execute(ref_query).fetchone()
                    
                    if result and result[0]:
                        reference_timestamp = result[0]
                        break
            
            if not reference_timestamp:
                self.create_result(
                    check_name=f"cross_stage_timestamps_{database}",
                    status='FAIL',
                    message="Cannot validate cross-stage timestamps: no valid timestamps found",
                    risk_level='HIGH',
                    details={'database': database}
                )
                return
            
            # Check all FE tables against reference timestamp
            inconsistent_tables = []
            
            for table in fe_tables:
                with self.db_manager.get_connection(database) as conn:
                    table_query = text(f"""
                        SELECT MAX(timestamp) as latest_timestamp
                        FROM public."{table}"
                    """)
                    result = conn.execute(table_query).fetchone()
                    
                    if result and result[0]:
                        table_timestamp = result[0]
                        
                        # Calculate time difference
                        time_diff = abs((table_timestamp - reference_timestamp).total_seconds())
                        
                        timestamp_consistency.append({
                            'table': table,
                            'timestamp': str(table_timestamp),
                            'time_diff_seconds': time_diff,
                            'consistent': time_diff <= 60  # Within 1 minute tolerance
                        })
                        
                        if time_diff > 60:  # More than 1 minute difference
                            inconsistent_tables.append({
                                'table': table,
                                'timestamp': str(table_timestamp),
                                'time_diff_minutes': time_diff / 60
                            })
            
            # Report timestamp consistency results
            if inconsistent_tables:
                self.create_result(
                    check_name=f"cross_stage_timestamps_{database}",
                    status='WARNING',
                    message=f"Timestamp inconsistencies: {len(inconsistent_tables)} tables have mismatched timestamps",
                    risk_level='MEDIUM',
                    details={
                        'database': database,
                        'reference_timestamp': str(reference_timestamp),
                        'inconsistent_tables': inconsistent_tables,
                        'all_timestamps': timestamp_consistency
                    }
                )
            else:
                self.create_result(
                    check_name=f"cross_stage_timestamps_{database}",
                    status='PASS',
                    message=f"Cross-stage timestamp consistency validated: all {len(fe_tables)} tables synchronized",
                    risk_level='LOW',
                    details={
                        'database': database,
                        'reference_timestamp': str(reference_timestamp),
                        'synchronized_tables': len(fe_tables)
                    }
                )
        
        except Exception as e:
            self.create_result(
                check_name=f"cross_stage_timestamps_{database}",
                status='ERROR',
                message=f"Cross-stage timestamp validation failed: {str(e)}",
                risk_level='MEDIUM',
                details={'database': database}
            )
    
    def _compare_cross_database_pipelines(self, databases: List[str]):
        """Compare pipeline execution status across databases."""
        self.logger.debug("Comparing cross-database pipeline execution")
        
        # Extract pipeline completion metrics from results
        db_completions = {}
        
        for result in self.results:
            if result.check_name.startswith('pipeline_execution_'):
                db_name = result.check_name.split('_')[-1]
                if db_name in databases:
                    db_completions[db_name] = {
                        'completion_percentage': result.metrics.get('completion_percentage', 0),
                        'status': result.status,
                        'risk_level': result.risk_level
                    }
        
        if len(db_completions) >= 2:
            # Find databases with different completion rates
            completion_rates = {db: data['completion_percentage'] for db, data in db_completions.items()}
            min_completion = min(completion_rates.values())
            max_completion = max(completion_rates.values())
            
            completion_variance = max_completion - min_completion
            
            if completion_variance > 20:  # More than 20% difference
                incomplete_dbs = [db for db, rate in completion_rates.items() if rate < max_completion]
                
                self.create_result(
                    check_name="cross_database_pipeline_consistency",
                    status='WARNING',
                    message=f"Pipeline completion variance: {completion_variance:.1f}% across databases",
                    risk_level='HIGH' if completion_variance > 50 else 'MEDIUM',
                    details={
                        'databases_compared': databases,
                        'completion_rates': completion_rates,
                        'incomplete_databases': incomplete_dbs,
                        'recommendation': 'Investigate pipeline failures in incomplete databases'
                    }
                )
            else:
                self.create_result(
                    check_name="cross_database_pipeline_consistency",
                    status='PASS',
                    message=f"Pipeline completion consistent: {completion_variance:.1f}% variance across databases",
                    risk_level='LOW',
                    details={
                        'databases_compared': databases,
                        'completion_rates': completion_rates
                    }
                )