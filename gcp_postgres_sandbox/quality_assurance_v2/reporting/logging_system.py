"""
Enhanced Logging System for CryptoPrism-DB QA system v2.
Provides structured logging with file rotation, filtering, and historical tracking.
"""

import logging
import logging.handlers
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
import json
import os

class QALoggingSystem:
    """
    Enhanced logging system for QA operations with multiple handlers and filtering.
    Provides structured logging with rotation, filtering, and historical tracking.
    """
    
    def __init__(self, 
                 log_level: str = 'INFO',
                 log_dir: Optional[str] = None,
                 max_file_size: int = 10 * 1024 * 1024,  # 10MB
                 backup_count: int = 5):
        """
        Initialize QA logging system.
        
        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_dir: Directory for log files (defaults to quality_assurance_v2/logs)
            max_file_size: Maximum size of log files before rotation
            backup_count: Number of backup log files to keep
        """
        self.log_level = getattr(logging, log_level.upper())
        
        # Set up log directory
        if log_dir:
            self.log_dir = Path(log_dir)
        else:
            # Default to logs directory relative to this module
            module_dir = Path(__file__).parent.parent
            self.log_dir = module_dir / "logs"
        
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        
        # Initialize loggers
        self._setup_loggers()
        
        # Track logging session
        self.session_start = datetime.utcnow()
        self.session_id = self.session_start.strftime("%Y%m%d_%H%M%S")
        
        # Log counters for statistics
        self.log_counters = {
            'DEBUG': 0, 'INFO': 0, 'WARNING': 0, 'ERROR': 0, 'CRITICAL': 0
        }
    
    def _setup_loggers(self):
        """Set up multiple loggers for different purposes."""
        
        # Main QA logger with rotation
        self.qa_logger = self._create_rotating_logger(
            name='qa_system',
            filename='qa_system.log',
            level=self.log_level
        )
        
        # Performance logger for timing and metrics
        self.performance_logger = self._create_rotating_logger(
            name='qa_performance',
            filename='qa_performance.log',
            level=logging.DEBUG
        )
        
        # Error logger for critical issues
        self.error_logger = self._create_rotating_logger(
            name='qa_errors',
            filename='qa_errors.log',
            level=logging.WARNING
        )
        
        # Alert logger for notifications
        self.alert_logger = self._create_rotating_logger(
            name='qa_alerts', 
            filename='qa_alerts.log',
            level=logging.INFO
        )
    
    def _create_rotating_logger(self, 
                               name: str, 
                               filename: str, 
                               level: int) -> logging.Logger:
        """Create a logger with rotating file handler."""
        
        logger = logging.getLogger(name)
        logger.setLevel(level)
        
        # Clear any existing handlers
        logger.handlers.clear()
        
        # File handler with rotation
        log_path = self.log_dir / filename
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=self.max_file_size,
            backupCount=self.backup_count
        )
        
        # Console handler for immediate feedback
        console_handler = logging.StreamHandler()
        
        # Formatter with structured information
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Set different levels for different handlers if needed
        if name == 'qa_errors':
            console_handler.setLevel(logging.ERROR)
        else:
            console_handler.setLevel(level)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def log_qa_start(self, databases: List[str], modules: List[str]):
        """Log the start of QA execution."""
        self.qa_logger.info(f"🚀 Starting QA execution - Session ID: {self.session_id}")
        self.qa_logger.info(f"Databases: {', '.join(databases)}")
        self.qa_logger.info(f"Modules: {', '.join(modules)}")
        self.qa_logger.info(f"Log Level: {logging.getLevelName(self.log_level)}")
        
        # Log to structured performance log
        self.performance_logger.info(json.dumps({
            'event': 'qa_session_start',
            'session_id': self.session_id,
            'timestamp': self.session_start.isoformat(),
            'databases': databases,
            'modules': modules,
            'log_level': logging.getLevelName(self.log_level)
        }))
    
    def log_qa_end(self, 
                   total_checks: int, 
                   execution_time: float,
                   results_summary: Dict[str, int]):
        """Log the completion of QA execution."""
        self.qa_logger.info(f"✅ QA execution completed in {execution_time:.2f}s")
        self.qa_logger.info(f"Total checks: {total_checks}")
        
        # Log summary statistics
        for status, count in results_summary.items():
            self.qa_logger.info(f"{status}: {count}")
        
        # Log to structured performance log
        self.performance_logger.info(json.dumps({
            'event': 'qa_session_end',
            'session_id': self.session_id,
            'timestamp': datetime.utcnow().isoformat(),
            'execution_time_seconds': execution_time,
            'total_checks': total_checks,
            'results_summary': results_summary,
            'log_counters': self.log_counters
        }))
    
    def log_module_start(self, module_name: str, database: str):
        """Log the start of a QA module."""
        self.qa_logger.info(f"🔍 Starting {module_name} for database: {database}")
    
    def log_module_end(self, 
                      module_name: str, 
                      database: str, 
                      checks_run: int,
                      execution_time: float,
                      issues_found: int):
        """Log the completion of a QA module."""
        self.qa_logger.info(
            f"✅ Completed {module_name} for {database}: "
            f"{checks_run} checks, {issues_found} issues, {execution_time:.2f}s"
        )
        
        # Log to performance log
        self.performance_logger.info(json.dumps({
            'event': 'module_completion',
            'session_id': self.session_id,
            'timestamp': datetime.utcnow().isoformat(),
            'module': module_name,
            'database': database,
            'checks_run': checks_run,
            'execution_time_seconds': execution_time,
            'issues_found': issues_found
        }))
    
    def log_check_result(self, 
                        check_name: str,
                        status: str,
                        risk_level: str,
                        message: str,
                        database: str = '',
                        execution_time: Optional[float] = None,
                        details: Optional[Dict[str, Any]] = None):
        """Log individual check results with appropriate level."""
        
        # Update counters
        log_level = self._get_log_level_from_status(status, risk_level)
        level_name = logging.getLevelName(log_level)
        if level_name in self.log_counters:
            self.log_counters[level_name] += 1
        
        # Format message
        db_prefix = f"[{database}] " if database else ""
        log_message = f"{db_prefix}{check_name}: {message}"
        
        # Log to appropriate logger
        self.qa_logger.log(log_level, log_message)
        
        # Log alerts for high-risk issues
        if risk_level in ['HIGH', 'CRITICAL'] or status in ['FAIL', 'ERROR']:
            self.alert_logger.warning(f"🚨 {log_message}")
            
            # Also log to error logger for CRITICAL/ERROR
            if risk_level == 'CRITICAL' or status == 'ERROR':
                error_details = details or {}
                self.error_logger.error(json.dumps({
                    'check_name': check_name,
                    'status': status,
                    'risk_level': risk_level,
                    'message': message,
                    'database': database,
                    'timestamp': datetime.utcnow().isoformat(),
                    'session_id': self.session_id,
                    'details': error_details
                }))
        
        # Log performance data if execution time provided
        if execution_time is not None:
            self.performance_logger.debug(json.dumps({
                'event': 'check_execution',
                'session_id': self.session_id,
                'timestamp': datetime.utcnow().isoformat(),
                'check_name': check_name,
                'database': database,
                'execution_time_seconds': execution_time,
                'status': status,
                'risk_level': risk_level
            }))
    
    def log_database_connection(self, database: str, success: bool, response_time: float):
        """Log database connection attempts."""
        if success:
            self.qa_logger.info(f"✅ Connected to {database} in {response_time:.3f}s")
        else:
            self.qa_logger.error(f"❌ Failed to connect to {database}")
            self.error_logger.error(json.dumps({
                'event': 'database_connection_failed',
                'database': database,
                'timestamp': datetime.utcnow().isoformat(),
                'session_id': self.session_id
            }))
        
        # Always log performance data
        self.performance_logger.debug(json.dumps({
            'event': 'database_connection',
            'session_id': self.session_id,
            'timestamp': datetime.utcnow().isoformat(),
            'database': database,
            'success': success,
            'response_time_seconds': response_time
        }))
    
    def log_notification_sent(self, 
                             notification_type: str,
                             destination: str,
                             success: bool,
                             message_length: int):
        """Log notification attempts."""
        if success:
            self.qa_logger.info(f"📤 Sent {notification_type} notification to {destination}")
        else:
            self.qa_logger.error(f"❌ Failed to send {notification_type} notification")
        
        # Log to alert logger
        self.alert_logger.info(json.dumps({
            'event': 'notification_sent',
            'session_id': self.session_id,
            'timestamp': datetime.utcnow().isoformat(),
            'notification_type': notification_type,
            'destination': destination,
            'success': success,
            'message_length': message_length
        }))
    
    def _get_log_level_from_status(self, status: str, risk_level: str) -> int:
        """Convert status and risk level to logging level."""
        if status == 'ERROR' or risk_level == 'CRITICAL':
            return logging.ERROR
        elif status == 'FAIL' or risk_level == 'HIGH':
            return logging.WARNING
        elif status == 'WARNING' or risk_level == 'MEDIUM':
            return logging.INFO
        else:
            return logging.DEBUG
    
    def get_session_logs(self, 
                        log_type: str = 'qa_system',
                        max_lines: int = 1000) -> List[str]:
        """
        Retrieve recent log entries for the current session.
        
        Args:
            log_type: Type of log ('qa_system', 'performance', 'errors', 'alerts')
            max_lines: Maximum number of lines to retrieve
            
        Returns:
            List of log lines
        """
        log_filename = f"{log_type}.log"
        log_path = self.log_dir / log_filename
        
        if not log_path.exists():
            return []
        
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
                # Filter lines for current session
                session_lines = []
                for line in lines:
                    if self.session_id in line:
                        session_lines.append(line.strip())
                
                # Return most recent lines
                return session_lines[-max_lines:]
                
        except Exception as e:
            self.qa_logger.error(f"Failed to retrieve session logs: {e}")
            return []
    
    def get_log_statistics(self) -> Dict[str, Any]:
        """Get logging statistics for the current session."""
        return {
            'session_id': self.session_id,
            'session_start': self.session_start.isoformat(),
            'log_counters': self.log_counters.copy(),
            'total_logs': sum(self.log_counters.values()),
            'log_level': logging.getLevelName(self.log_level),
            'log_directory': str(self.log_dir)
        }
    
    def cleanup_old_logs(self, retention_days: int = 30):
        """Clean up old log files to manage disk space."""
        from datetime import timedelta
        
        self.qa_logger.info(f"Cleaning up logs older than {retention_days} days")
        
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        deleted_count = 0
        
        for log_file in self.log_dir.glob("*.log*"):
            try:
                file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                
                if file_mtime < cutoff_date:
                    log_file.unlink()
                    deleted_count += 1
                    
            except Exception as e:
                self.qa_logger.warning(f"Failed to delete {log_file}: {e}")
        
        self.qa_logger.info(f"Cleaned up {deleted_count} old log files")
    
    def export_session_summary(self) -> Dict[str, Any]:
        """Export a summary of the current logging session."""
        return {
            'session_metadata': {
                'session_id': self.session_id,
                'start_time': self.session_start.isoformat(),
                'end_time': datetime.utcnow().isoformat(),
                'log_level': logging.getLevelName(self.log_level)
            },
            'log_statistics': self.get_log_statistics(),
            'recent_errors': self.get_session_logs('errors', 10),
            'recent_alerts': self.get_session_logs('alerts', 20)
        }