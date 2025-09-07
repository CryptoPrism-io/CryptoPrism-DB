#!/usr/bin/env python3
"""
Shared database connection utilities for CryptoPrism Database Utilities.

This module provides a centralized way to manage database connections across all utilities,
supporting both main production database and backtest databases with environment-based configuration.

Features:
- Environment variable-based configuration
- Connection validation and testing
- Multiple database support (dbcp, cp_ai, cp_backtest)
- SQLAlchemy engine management
- Error handling and logging

Requirements:
- sqlalchemy>=2.0.0
- psycopg2-binary>=2.9.0  
- python-dotenv
"""

import os
import logging
from typing import Dict, Optional
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Set up logging
logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Centralized database connection manager for CryptoPrism utilities."""
    
    def __init__(self):
        """Initialize with environment variables and validate configuration."""
        load_dotenv()
        
        # Database connection parameters
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'), 
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # Database names mapping
        self.databases = {
            'main': os.getenv('DB_NAME', 'dbcp'),
            'ai': os.getenv('DB_NAME_AI', 'cp_ai'),
            'backtest': os.getenv('DB_NAME_BT', 'cp_backtest'),
            'backtest_h': os.getenv('DB_NAME_BTH', 'cp_backtest_h')
        }
        
        # Validate required environment variables
        self._validate_environment()
        
        # Connection cache
        self._engines: Dict[str, Engine] = {}
    
    def _validate_environment(self) -> None:
        """Validate that required environment variables are present."""
        required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise EnvironmentError(
                f"Missing required environment variables: {missing_vars}. "
                f"Please ensure your .env file contains all required database credentials."
            )
        
        logger.info("Database environment configuration validated successfully")
    
    def create_connection_string(self, database_name: str) -> str:
        """
        Create PostgreSQL connection string for specified database.
        
        Args:
            database_name: Database name or alias ('main', 'ai', 'backtest', 'backtest_h')
            
        Returns:
            PostgreSQL connection string
            
        Raises:
            ValueError: If database_name is not recognized
        """
        # Resolve database alias to actual name
        if database_name in self.databases:
            db_name = self.databases[database_name]
        else:
            db_name = database_name
        
        connection_string = (
            f"postgresql+psycopg2://{self.db_config['user']}:"
            f"{self.db_config['password']}@{self.db_config['host']}:"
            f"{self.db_config['port']}/{db_name}"
        )
        
        logger.debug(f"Created connection string for database: {db_name}")
        return connection_string
    
    def get_engine(self, database: str = 'main', echo: bool = False) -> Engine:
        """
        Get SQLAlchemy engine for specified database with connection caching.
        
        Args:
            database: Database alias or name
            echo: Enable SQL logging
            
        Returns:
            SQLAlchemy Engine instance
        """
        cache_key = f"{database}_{echo}"
        
        if cache_key not in self._engines:
            connection_string = self.create_connection_string(database)
            
            # Engine configuration optimized for analytics workloads
            self._engines[cache_key] = create_engine(
                connection_string,
                echo=echo,
                pool_pre_ping=True,          # Verify connections before use
                pool_recycle=3600,           # Recycle connections every hour
                connect_args={
                    "connect_timeout": 30,    # Connection timeout
                    "application_name": "CryptoPrism-DB-Utilities"
                }
            )
            
            logger.info(f"Created new database engine for: {database}")
        
        return self._engines[cache_key]
    
    def test_connection(self, database: str = 'main') -> bool:
        """
        Test database connection and return success status.
        
        Args:
            database: Database alias or name to test
            
        Returns:
            True if connection successful, False otherwise
        """
        try:
            engine = self.get_engine(database)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test"))
                test_value = result.fetchone()[0]
                
                if test_value == 1:
                    logger.info(f"Database connection test successful for: {database}")
                    return True
                else:
                    logger.error(f"Unexpected test result for database: {database}")
                    return False
                    
        except SQLAlchemyError as e:
            logger.error(f"Database connection failed for {database}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error testing database {database}: {str(e)}")
            return False
    
    def get_database_info(self, database: str = 'main') -> Dict[str, str]:
        """
        Get basic database information.
        
        Args:
            database: Database alias or name
            
        Returns:
            Dictionary with database info
        """
        try:
            engine = self.get_engine(database)
            with engine.connect() as conn:
                # Get database version and basic info
                version_result = conn.execute(text("SELECT version()"))
                version = version_result.fetchone()[0]
                
                db_name_result = conn.execute(text("SELECT current_database()"))
                current_db = db_name_result.fetchone()[0]
                
                user_result = conn.execute(text("SELECT current_user"))
                current_user = user_result.fetchone()[0]
                
                return {
                    'database': current_db,
                    'user': current_user,
                    'version': version,
                    'host': self.db_config['host'],
                    'port': self.db_config['port']
                }
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get database info for {database}: {str(e)}")
            return {}
    
    def close_all_connections(self) -> None:
        """Close all cached database connections."""
        for engine_name, engine in self._engines.items():
            try:
                engine.dispose()
                logger.info(f"Closed database connection: {engine_name}")
            except Exception as e:
                logger.warning(f"Error closing connection {engine_name}: {str(e)}")
        
        self._engines.clear()
        logger.info("All database connections closed")
    
    def list_available_databases(self) -> Dict[str, str]:
        """Return mapping of available database aliases to names."""
        return self.databases.copy()


# Convenience function for simple usage
def get_database_connection(database: str = 'main') -> DatabaseConnection:
    """
    Convenience function to get a DatabaseConnection instance.
    
    Args:
        database: Database to connect to
        
    Returns:
        DatabaseConnection instance
    """
    return DatabaseConnection()