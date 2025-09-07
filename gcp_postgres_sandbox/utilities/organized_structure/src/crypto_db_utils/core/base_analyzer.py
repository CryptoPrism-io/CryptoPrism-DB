#!/usr/bin/env python3
"""
Base analyzer class providing common functionality for CryptoPrism Database Utilities.

This module provides a foundation class that other analysis tools can inherit from,
providing common database operations, logging setup, and output management.

Features:
- Common database connection management
- Standardized logging configuration
- Output directory management
- Error handling patterns
- Progress reporting utilities
- JSON serialization helpers

Requirements:
- sqlalchemy>=2.0.0
- python-dotenv
"""

import os
import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from decimal import Decimal

from .db_connection import DatabaseConnection


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Decimal types and datetime objects."""
    
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class BaseAnalyzer(ABC):
    """Base class for database analysis tools with common functionality."""
    
    def __init__(self, 
                 output_dir: str = "output", 
                 log_level: int = logging.INFO,
                 databases: Optional[List[str]] = None):
        """
        Initialize base analyzer.
        
        Args:
            output_dir: Directory for output files (relative to organized_structure)
            log_level: Logging level
            databases: List of databases to analyze (defaults to ['main'])
        """
        self.output_dir = Path(output_dir)
        self.databases = databases or ['main']
        self.start_time = datetime.now()
        
        # Setup logging
        self.setup_logging(log_level)
        
        # Initialize database connection
        self.db_conn = DatabaseConnection()
        
        # Create output directory
        self.ensure_output_directory()
        
        # Analysis results storage
        self.results: Dict[str, Any] = {}
        
        logger.info(f"Initialized {self.__class__.__name__}")
    
    def setup_logging(self, log_level: int) -> None:
        """Configure logging for the analyzer."""
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Create logger for this analyzer
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(log_level)
    
    def ensure_output_directory(self) -> None:
        """Create output directory if it doesn't exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger.debug(f"Output directory ensured: {self.output_dir}")
    
    def get_timestamp(self) -> str:
        """Get current timestamp in standard format."""
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def get_output_filename(self, 
                          base_name: str, 
                          database: str = None, 
                          extension: str = "json") -> Path:
        """
        Generate standardized output filename.
        
        Args:
            base_name: Base name for the file
            database: Database name (if applicable)
            extension: File extension
            
        Returns:
            Path object for output file
        """
        timestamp = self.get_timestamp()
        
        if database:
            filename = f"{base_name}_{database}_{timestamp}.{extension}"
        else:
            filename = f"{base_name}_{timestamp}.{extension}"
            
        return self.output_dir / filename
    
    def save_json_results(self, 
                         data: Dict[str, Any], 
                         filename: Union[str, Path]) -> Path:
        """
        Save results to JSON file with proper formatting.
        
        Args:
            data: Data to save
            filename: Output filename
            
        Returns:
            Path to saved file
        """
        if isinstance(filename, str):
            filepath = self.output_dir / filename
        else:
            filepath = filename
            
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, cls=DecimalEncoder, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Results saved to: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Failed to save results to {filepath}: {str(e)}")
            raise
    
    def save_text_results(self, 
                         content: str, 
                         filename: Union[str, Path]) -> Path:
        """
        Save text results to file.
        
        Args:
            content: Text content to save
            filename: Output filename
            
        Returns:
            Path to saved file
        """
        if isinstance(filename, str):
            filepath = self.output_dir / filename
        else:
            filepath = filename
            
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            self.logger.info(f"Text results saved to: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Failed to save text to {filepath}: {str(e)}")
            raise
    
    def test_database_connections(self) -> Dict[str, bool]:
        """
        Test connections to all configured databases.
        
        Returns:
            Dictionary mapping database names to connection status
        """
        connection_status = {}
        
        for database in self.databases:
            self.logger.info(f"Testing connection to database: {database}")
            try:
                status = self.db_conn.test_connection(database)
                connection_status[database] = status
                
                if status:
                    self.logger.info(f"✓ Connection successful: {database}")
                else:
                    self.logger.error(f"✗ Connection failed: {database}")
                    
            except Exception as e:
                self.logger.error(f"✗ Connection error for {database}: {str(e)}")
                connection_status[database] = False
        
        return connection_status
    
    def get_analysis_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the analysis run.
        
        Returns:
            Dictionary with analysis metadata
        """
        return {
            'analyzer': self.__class__.__name__,
            'start_time': self.start_time.isoformat(),
            'current_time': datetime.now().isoformat(),
            'databases': self.databases,
            'output_directory': str(self.output_dir),
            'database_config': self.db_conn.list_available_databases()
        }
    
    def progress_callback(self, 
                         current: int, 
                         total: int, 
                         item_name: str = "items") -> None:
        """
        Standard progress reporting callback.
        
        Args:
            current: Current progress count
            total: Total items to process
            item_name: Description of items being processed
        """
        if total > 0:
            percentage = (current / total) * 100
            self.logger.info(f"Progress: {current}/{total} {item_name} ({percentage:.1f}%)")
        else:
            self.logger.info(f"Progress: {current} {item_name} processed")
    
    def measure_execution_time(self, func_name: str = None) -> float:
        """
        Measure elapsed time since initialization.
        
        Args:
            func_name: Optional function name for logging
            
        Returns:
            Elapsed time in seconds
        """
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        if func_name:
            self.logger.info(f"{func_name} execution time: {elapsed:.2f} seconds")
        else:
            self.logger.info(f"Total execution time: {elapsed:.2f} seconds")
            
        return elapsed
    
    def cleanup(self) -> None:
        """Clean up resources (close database connections, etc.)."""
        try:
            self.db_conn.close_all_connections()
            self.logger.info("Cleanup completed successfully")
        except Exception as e:
            self.logger.warning(f"Error during cleanup: {str(e)}")
    
    @abstractmethod
    def run_analysis(self) -> Dict[str, Any]:
        """
        Run the main analysis. Must be implemented by subclasses.
        
        Returns:
            Dictionary with analysis results
        """
        pass
    
    def execute(self) -> Dict[str, Any]:
        """
        Execute the full analysis workflow with error handling.
        
        Returns:
            Dictionary with analysis results and metadata
        """
        self.logger.info(f"Starting {self.__class__.__name__} analysis")
        
        try:
            # Test database connections first
            connection_status = self.test_database_connections()
            failed_connections = [db for db, status in connection_status.items() if not status]
            
            if failed_connections:
                raise ConnectionError(f"Failed to connect to databases: {failed_connections}")
            
            # Run the main analysis
            analysis_results = self.run_analysis()
            
            # Add metadata
            results_with_metadata = {
                'metadata': self.get_analysis_metadata(),
                'connection_status': connection_status,
                'results': analysis_results
            }
            
            # Record final execution time
            execution_time = self.measure_execution_time("Total analysis")
            results_with_metadata['metadata']['execution_time_seconds'] = execution_time
            
            self.logger.info(f"Analysis completed successfully in {execution_time:.2f} seconds")
            return results_with_metadata
            
        except Exception as e:
            self.logger.error(f"Analysis failed: {str(e)}")
            self.logger.exception("Full error details:")
            raise
            
        finally:
            # Always cleanup
            self.cleanup()


# Utility functions for common operations
def format_table_info(table_info: Dict[str, Any]) -> str:
    """Format table information for display."""
    return (
        f"Table: {table_info.get('name', 'Unknown')}\n"
        f"  Columns: {table_info.get('column_count', 0)}\n"
        f"  Rows: {table_info.get('row_count', 'Unknown')}\n"
        f"  Primary Key: {table_info.get('has_primary_key', False)}\n"
    )


def format_analysis_summary(results: Dict[str, Any]) -> str:
    """Format analysis results summary for display."""
    metadata = results.get('metadata', {})
    
    summary = f"""
Analysis Summary
================
Analyzer: {metadata.get('analyzer', 'Unknown')}
Start Time: {metadata.get('start_time', 'Unknown')}
Execution Time: {metadata.get('execution_time_seconds', 0):.2f} seconds
Databases: {', '.join(metadata.get('databases', []))}
Output Directory: {metadata.get('output_directory', 'Unknown')}

Connection Status:
"""
    
    for db, status in results.get('connection_status', {}).items():
        status_symbol = "✓" if status else "✗"
        summary += f"  {status_symbol} {db}\n"
    
    return summary