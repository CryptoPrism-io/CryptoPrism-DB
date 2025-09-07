"""
Base QA module providing common functionality for all QA checks.
Contains shared methods, logging, and reporting infrastructure.
"""

import logging
from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod
from datetime import datetime
import time

from .config import QAConfig
from .database import DatabaseManager

logger = logging.getLogger(__name__)

class QAResult:
    """Represents the result of a QA check."""
    
    def __init__(self, 
                 check_name: str,
                 status: str,  # 'PASS', 'FAIL', 'WARNING', 'ERROR'
                 risk_level: str,  # 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
                 message: str,
                 details: Optional[Dict[str, Any]] = None,
                 metrics: Optional[Dict[str, float]] = None):
        self.check_name = check_name
        self.status = status
        self.risk_level = risk_level
        self.message = message
        self.details = details or {}
        self.metrics = metrics or {}
        self.timestamp = datetime.utcnow().isoformat()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert QA result to dictionary for JSON serialization."""
        return {
            'check_name': self.check_name,
            'status': self.status,
            'risk_level': self.risk_level,
            'message': self.message,
            'details': self.details,
            'metrics': self.metrics,
            'timestamp': self.timestamp
        }
    
    def should_alert(self, config: QAConfig) -> bool:
        """Determine if this result should trigger an alert."""
        return config.should_alert(self.risk_level)

class BaseQAModule(ABC):
    """
    Abstract base class for all QA modules.
    Provides common functionality and enforces consistent interface.
    """
    
    def __init__(self, config: QAConfig, db_manager: DatabaseManager, module_name: str):
        """
        Initialize base QA module.
        
        Args:
            config: QA configuration instance
            db_manager: Database manager instance
            module_name: Name of the QA module
        """
        self.config = config
        self.db_manager = db_manager
        self.module_name = module_name
        self.logger = logging.getLogger(f"qa.{module_name}")
        self.results: List[QAResult] = []
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
    
    @abstractmethod
    def run_checks(self, databases: Optional[List[str]] = None) -> List[QAResult]:
        """
        Run all QA checks for this module.
        
        Args:
            databases: List of databases to check, or None for all configured databases
            
        Returns:
            List of QA results
        """
        pass
    
    def start_module(self):
        """Initialize module execution."""
        self.start_time = time.time()
        self.results.clear()
        self.logger.info(f"🔍 Starting {self.module_name} QA checks")
    
    def end_module(self):
        """Finalize module execution."""
        self.end_time = time.time()
        execution_time = self.end_time - self.start_time if self.start_time else 0
        
        # Summary statistics
        total_checks = len(self.results)
        failed_checks = len([r for r in self.results if r.status == 'FAIL'])
        critical_issues = len([r for r in self.results if r.risk_level == 'CRITICAL'])
        high_issues = len([r for r in self.results if r.risk_level == 'HIGH'])
        
        self.logger.info(
            f"✅ Completed {self.module_name} QA checks in {execution_time:.2f}s - "
            f"Total: {total_checks}, Failed: {failed_checks}, Critical: {critical_issues}, High: {high_issues}"
        )
    
    def add_result(self, result: QAResult):
        """Add a QA result to the module results."""
        self.results.append(result)
        
        # Log based on risk level
        log_message = f"{result.check_name}: {result.message}"
        if result.risk_level in ['HIGH', 'CRITICAL']:
            self.logger.warning(f"🔴 {log_message}")
        elif result.risk_level == 'MEDIUM':
            self.logger.info(f"🟡 {log_message}")
        else:
            self.logger.debug(f"🟢 {log_message}")
    
    def create_result(self, 
                     check_name: str,
                     status: str,
                     message: str,
                     risk_level: str = 'LOW',
                     details: Optional[Dict[str, Any]] = None,
                     metrics: Optional[Dict[str, float]] = None) -> QAResult:
        """
        Create and add a QA result.
        
        Args:
            check_name: Name of the check
            status: Status ('PASS', 'FAIL', 'WARNING', 'ERROR')
            message: Result message
            risk_level: Risk level ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')
            details: Additional details
            metrics: Performance metrics
            
        Returns:
            Created QA result
        """
        result = QAResult(
            check_name=f"{self.module_name}.{check_name}",
            status=status,
            risk_level=risk_level,
            message=message,
            details=details,
            metrics=metrics
        )
        self.add_result(result)
        return result
    
    def check_table_exists(self, database: str, table_name: str) -> bool:
        """
        Check if a table exists in the specified database.
        
        Args:
            database: Database name
            table_name: Table name to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            with self.db_manager.get_connection(database) as conn:
                from sqlalchemy import text
                result = conn.execute(text(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = :table_name)"
                ), {'table_name': table_name}).scalar()
                return bool(result)
        except Exception as e:
            self.logger.error(f"Error checking table existence {table_name} in {database}: {e}")
            return False
    
    def get_table_row_count(self, database: str, table_name: str) -> int:
        """
        Get row count for a specific table.
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            Row count, or 0 if error
        """
        try:
            with self.db_manager.get_connection(database) as conn:
                from sqlalchemy import text
                result = conn.execute(text(f'SELECT COUNT(*) FROM public."{table_name}"')).scalar()
                return int(result)
        except Exception as e:
            self.logger.error(f"Error getting row count for {table_name} in {database}: {e}")
            return 0
    
    def get_column_names(self, database: str, table_name: str) -> List[str]:
        """
        Get column names for a specific table.
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            List of column names
        """
        try:
            with self.db_manager.get_connection(database) as conn:
                from sqlalchemy import text
                result = conn.execute(text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = :table_name AND table_schema = 'public'
                    ORDER BY ordinal_position
                """), {'table_name': table_name})
                return [row[0] for row in result]
        except Exception as e:
            self.logger.error(f"Error getting columns for {table_name} in {database}: {e}")
            return []
    
    def check_required_columns(self, database: str, table_name: str, required_columns: List[str]) -> Dict[str, bool]:
        """
        Check if required columns exist in a table.
        
        Args:
            database: Database name
            table_name: Table name
            required_columns: List of required column names
            
        Returns:
            Dictionary mapping column names to existence status
        """
        existing_columns = set(self.get_column_names(database, table_name))
        return {col: col in existing_columns for col in required_columns}
    
    def measure_query_performance(self, database: str, query: str, description: str = "") -> Dict[str, Any]:
        """
        Measure query execution performance.
        
        Args:
            database: Database name
            query: SQL query to execute
            description: Query description
            
        Returns:
            Performance metrics dictionary
        """
        return self.db_manager.execute_performance_query(database, query, description)
    
    def validate_timestamp_range(self, database: str, table_name: str) -> Dict[str, Any]:
        """
        Validate timestamp ranges for a table.
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            Timestamp validation results
        """
        try:
            with self.db_manager.get_connection(database) as conn:
                from sqlalchemy import text
                
                # Find timestamp columns
                timestamp_cols = conn.execute(text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = :table_name 
                      AND table_schema = 'public'
                      AND data_type LIKE '%timestamp%'
                    ORDER BY ordinal_position
                    LIMIT 1
                """), {'table_name': table_name}).fetchone()
                
                if not timestamp_cols:
                    return {
                        'status': 'NO_TIMESTAMP_COLUMN',
                        'message': 'No timestamp column found',
                        'risk_level': 'MEDIUM'
                    }
                
                timestamp_col = timestamp_cols[0]
                
                # Get timestamp range
                result = conn.execute(text(f"""
                    SELECT 
                        MIN("{timestamp_col}") as first_timestamp,
                        MAX("{timestamp_col}") as last_timestamp,
                        COUNT(CASE WHEN "{timestamp_col}" IS NULL THEN 1 END) as null_count,
                        COUNT(*) as total_rows
                    FROM public."{table_name}"
                """)).fetchone()
                
                if not result or not result[0] or not result[1]:
                    return {
                        'status': 'NULL_TIMESTAMPS',
                        'message': 'Timestamp column contains NULL values',
                        'risk_level': 'HIGH',
                        'null_count': result[2] if result else 0
                    }
                
                first_ts, last_ts, null_count, total_rows = result
                
                # Validate based on table type
                if table_name.startswith('FE_'):
                    # FE tables should have matching first and last timestamps
                    if first_ts != last_ts:
                        return {
                            'status': 'TIMESTAMP_MISMATCH',
                            'message': f'FE table timestamps must match: {first_ts} != {last_ts}',
                            'risk_level': 'CRITICAL',
                            'first_timestamp': str(first_ts),
                            'last_timestamp': str(last_ts)
                        }
                else:
                    # Non-FE tables should have at least 4-day range
                    days_diff = (last_ts - first_ts).days
                    if days_diff < self.config.thresholds['min_timestamp_range_days']:
                        return {
                            'status': 'INSUFFICIENT_RANGE',
                            'message': f'Timestamp range too small: {days_diff} days',
                            'risk_level': 'HIGH',
                            'days_range': days_diff,
                            'required_days': self.config.thresholds['min_timestamp_range_days']
                        }
                
                return {
                    'status': 'VALID',
                    'message': 'Timestamp validation passed',
                    'risk_level': 'LOW',
                    'first_timestamp': str(first_ts),
                    'last_timestamp': str(last_ts),
                    'days_range': (last_ts - first_ts).days,
                    'null_count': null_count,
                    'total_rows': total_rows
                }
                
        except Exception as e:
            return {
                'status': 'ERROR',
                'message': f'Timestamp validation error: {str(e)}',
                'risk_level': 'CRITICAL'
            }
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """Get summary statistics for this module's results."""
        if not self.results:
            return {}
        
        status_counts = {}
        risk_counts = {}
        
        for result in self.results:
            status_counts[result.status] = status_counts.get(result.status, 0) + 1
            risk_counts[result.risk_level] = risk_counts.get(result.risk_level, 0) + 1
        
        execution_time = (self.end_time - self.start_time) if self.start_time and self.end_time else 0
        
        return {
            'module_name': self.module_name,
            'total_checks': len(self.results),
            'execution_time_seconds': round(execution_time, 2),
            'status_distribution': status_counts,
            'risk_distribution': risk_counts,
            'alerts_triggered': len([r for r in self.results if r.should_alert(self.config)])
        }