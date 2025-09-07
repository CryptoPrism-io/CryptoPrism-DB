"""
CryptoPrism Database Utilities

A comprehensive suite of PostgreSQL database analysis, benchmarking, and optimization tools
specifically designed for cryptocurrency trading systems.

Features:
- Schema analysis and visualization
- Performance benchmarking and testing
- Database optimization and indexing
- Query performance analysis
- Automated optimization recommendations

Author: CryptoPrism.io
License: Proprietary
"""

__version__ = "1.0.0"
__author__ = "CryptoPrism.io"

from .core.db_connection import DatabaseConnection
from .core.base_analyzer import BaseAnalyzer

__all__ = [
    'DatabaseConnection',
    'BaseAnalyzer',
    '__version__',
    '__author__'
]