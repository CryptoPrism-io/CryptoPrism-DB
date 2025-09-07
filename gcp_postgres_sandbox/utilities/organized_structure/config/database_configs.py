#!/usr/bin/env python3
"""
Database configuration management for CryptoPrism Database Utilities.

This module provides configuration classes and utilities for managing database
connections across different environments (development, testing, production).
"""

import os
from typing import Dict, List, Optional
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    
    host: str
    port: str = "5432"
    user: str = ""
    password: str = ""
    database: str = ""
    
    def to_connection_string(self, driver: str = "psycopg2") -> str:
        """Generate SQLAlchemy connection string."""
        return (
            f"postgresql+{driver}://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.host:
            raise ValueError("Database host is required")
        if not self.user:
            raise ValueError("Database user is required")
        if not self.database:
            raise ValueError("Database name is required")


class DatabaseConfigManager:
    """Manage multiple database configurations."""
    
    def __init__(self):
        """Initialize with environment variables."""
        load_dotenv()
        self._configs: Dict[str, DatabaseConfig] = {}
        self._load_default_configs()
    
    def _load_default_configs(self) -> None:
        """Load default database configurations from environment variables."""
        # Main production database
        if os.getenv('DB_HOST'):
            self._configs['main'] = DatabaseConfig(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT', '5432'),
                user=os.getenv('DB_USER', ''),
                password=os.getenv('DB_PASSWORD', ''),
                database=os.getenv('DB_NAME', 'dbcp')
            )
        
        # AI analysis database
        if os.getenv('DB_HOST') and os.getenv('DB_NAME_AI'):
            self._configs['ai'] = DatabaseConfig(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT', '5432'),
                user=os.getenv('DB_USER', ''),
                password=os.getenv('DB_PASSWORD', ''),
                database=os.getenv('DB_NAME_AI', 'cp_ai')
            )
        
        # Backtest database
        if os.getenv('DB_HOST') and os.getenv('DB_NAME_BT'):
            self._configs['backtest'] = DatabaseConfig(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT', '5432'),
                user=os.getenv('DB_USER', ''),
                password=os.getenv('DB_PASSWORD', ''),
                database=os.getenv('DB_NAME_BT', 'cp_backtest')
            )
        
        # Historical backtest database
        if os.getenv('DB_HOST') and os.getenv('DB_NAME_BTH'):
            self._configs['backtest_h'] = DatabaseConfig(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT', '5432'),
                user=os.getenv('DB_USER', ''),
                password=os.getenv('DB_PASSWORD', ''),
                database=os.getenv('DB_NAME_BTH', 'cp_backtest_h')
            )
    
    def add_config(self, name: str, config: DatabaseConfig) -> None:
        """Add a database configuration."""
        self._configs[name] = config
    
    def get_config(self, name: str) -> DatabaseConfig:
        """Get database configuration by name."""
        if name not in self._configs:
            raise KeyError(f"Database configuration '{name}' not found")
        return self._configs[name]
    
    def list_configs(self) -> List[str]:
        """List available database configuration names."""
        return list(self._configs.keys())
    
    def get_connection_string(self, name: str, driver: str = "psycopg2") -> str:
        """Get connection string for named database."""
        config = self.get_config(name)
        return config.to_connection_string(driver)
    
    def validate_all_configs(self) -> Dict[str, bool]:
        """Validate all database configurations."""
        validation_results = {}
        
        for name, config in self._configs.items():
            try:
                # Basic validation - check required fields
                if config.host and config.user and config.database:
                    validation_results[name] = True
                else:
                    validation_results[name] = False
            except Exception:
                validation_results[name] = False
        
        return validation_results
    
    def get_environment_info(self) -> Dict[str, str]:
        """Get information about the current environment configuration."""
        return {
            'DB_HOST': os.getenv('DB_HOST', 'Not set'),
            'DB_PORT': os.getenv('DB_PORT', 'Not set (default: 5432)'),
            'DB_USER': os.getenv('DB_USER', 'Not set'),
            'DB_NAME': os.getenv('DB_NAME', 'Not set (default: dbcp)'),
            'DB_NAME_AI': os.getenv('DB_NAME_AI', 'Not set'),
            'DB_NAME_BT': os.getenv('DB_NAME_BT', 'Not set'),
            'DB_NAME_BTH': os.getenv('DB_NAME_BTH', 'Not set'),
            'Total Configs': str(len(self._configs))
        }


# Global instance for easy access
config_manager = DatabaseConfigManager()


def get_database_config(name: str = 'main') -> DatabaseConfig:
    """
    Convenience function to get database configuration.
    
    Args:
        name: Database configuration name
        
    Returns:
        DatabaseConfig instance
    """
    return config_manager.get_config(name)


def get_connection_string(name: str = 'main', driver: str = "psycopg2") -> str:
    """
    Convenience function to get database connection string.
    
    Args:
        name: Database configuration name
        driver: SQLAlchemy driver name
        
    Returns:
        SQLAlchemy connection string
    """
    return config_manager.get_connection_string(name, driver)