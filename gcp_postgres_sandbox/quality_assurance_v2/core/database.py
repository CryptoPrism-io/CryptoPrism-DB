"""
Database connection management for CryptoPrism-DB QA system v2.
Handles connections to all three databases with connection pooling and health checks.
"""

import logging
from typing import Dict, Optional, Any, List
from contextlib import contextmanager
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool
import time

from .config import QAConfig

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Manages database connections for the QA system.
    Supports connection pooling, health checks, and multi-database operations.
    """
    
    def __init__(self, config: QAConfig):
        """
        Initialize database manager with QA configuration.
        
        Args:
            config: QA configuration instance
        """
        self.config = config
        self.engines: Dict[str, Engine] = {}
        self._initialize_engines()
    
    def _initialize_engines(self):
        """Initialize database engines for all configured databases."""
        for db_name in self.config.database_configs.keys():
            try:
                connection_string = self.config.get_connection_string(db_name)
                
                # Create engine with connection pooling
                engine = create_engine(
                    connection_string,
                    poolclass=QueuePool,
                    pool_size=5,
                    max_overflow=10,
                    pool_timeout=30,
                    pool_recycle=3600,  # Recycle connections every hour
                    isolation_level="AUTOCOMMIT"
                )
                
                # Test connection
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                
                self.engines[db_name] = engine
                logger.info(f"✅ Database engine initialized: {db_name}")
                
            except Exception as e:
                logger.error(f"❌ Failed to initialize database engine {db_name}: {e}")
                raise
    
    def get_engine(self, database: str) -> Engine:
        """
        Get SQLAlchemy engine for specified database.
        
        Args:
            database: Database name ('dbcp', 'cp_ai', or 'cp_backtest')
            
        Returns:
            SQLAlchemy engine instance
            
        Raises:
            ValueError: If database is not configured
        """
        if database not in self.engines:
            raise ValueError(f"Database not configured: {database}. Available: {list(self.engines.keys())}")
        
        return self.engines[database]
    
    @contextmanager
    def get_connection(self, database: str):
        """
        Context manager for database connections with automatic cleanup.
        
        Args:
            database: Database name
            
        Yields:
            Database connection
        """
        engine = self.get_engine(database)
        connection = None
        
        try:
            connection = engine.connect()
            yield connection
        except Exception as e:
            logger.error(f"Database connection error for {database}: {e}")
            raise
        finally:
            if connection:
                connection.close()
    
    def health_check(self, database: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        Perform health check on database connections.
        
        Args:
            database: Specific database to check, or None for all databases
            
        Returns:
            Health check results for each database
        """
        results = {}
        databases_to_check = [database] if database else list(self.engines.keys())
        
        for db_name in databases_to_check:
            start_time = time.time()
            
            try:
                with self.get_connection(db_name) as conn:
                    # Test basic connectivity
                    conn.execute(text("SELECT 1"))
                    
                    # Test table access
                    table_count = conn.execute(text(
                        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'"
                    )).scalar()
                    
                    # Test key table existence
                    key_tables_exist = []
                    for table in self.config.key_tables:
                        table_exists = conn.execute(text(
                            f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table}')"
                        )).scalar()
                        key_tables_exist.append({
                            'table': table,
                            'exists': table_exists
                        })
                    
                    response_time = time.time() - start_time
                    
                    results[db_name] = {
                        'status': 'healthy',
                        'response_time_ms': round(response_time * 1000, 2),
                        'table_count': table_count,
                        'key_tables': key_tables_exist,
                        'connection_pool_size': self.engines[db_name].pool.size(),
                        'checked_out_connections': self.engines[db_name].pool.checkedout()
                    }
                    
            except Exception as e:
                results[db_name] = {
                    'status': 'unhealthy',
                    'error': str(e),
                    'response_time_ms': round((time.time() - start_time) * 1000, 2)
                }
                logger.error(f"Health check failed for {db_name}: {e}")
        
        return results
    
    def get_table_info(self, database: str, table_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get table information from specified database.
        
        Args:
            database: Database name
            table_name: Specific table name, or None for all tables
            
        Returns:
            List of table information dictionaries
        """
        with self.get_connection(database) as conn:
            if table_name:
                query = text("""
                    SELECT 
                        t.table_name,
                        COALESCE(p.row_count, 0) as row_count,
                        array_agg(c.column_name) as columns,
                        array_agg(c.data_type) as column_types
                    FROM information_schema.tables t
                    LEFT JOIN (
                        SELECT schemaname, tablename, n_tup_ins - n_tup_del as row_count
                        FROM pg_stat_user_tables
                    ) p ON t.table_name = p.tablename
                    LEFT JOIN information_schema.columns c ON t.table_name = c.table_name
                    WHERE t.table_schema = 'public' AND t.table_name = :table_name
                    GROUP BY t.table_name, p.row_count
                """)
                result = conn.execute(query, {'table_name': table_name})
            else:
                query = text("""
                    SELECT 
                        t.table_name,
                        COALESCE(p.row_count, 0) as row_count,
                        array_agg(c.column_name) as columns,
                        array_agg(c.data_type) as column_types
                    FROM information_schema.tables t
                    LEFT JOIN (
                        SELECT schemaname, tablename, n_tup_ins - n_tup_del as row_count
                        FROM pg_stat_user_tables  
                    ) p ON t.table_name = p.tablename
                    LEFT JOIN information_schema.columns c ON t.table_name = c.table_name
                    WHERE t.table_schema = 'public'
                    GROUP BY t.table_name, p.row_count
                    ORDER BY t.table_name
                """)
                result = conn.execute(query)
            
            tables_info = []
            for row in result:
                tables_info.append({
                    'table_name': row[0],
                    'row_count': row[1],
                    'columns': row[2] if row[2] and row[2][0] else [],
                    'column_types': row[3] if row[3] and row[3][0] else []
                })
            
            return tables_info
    
    def get_table_statistics(self, database: str, table_name: str) -> Dict[str, Any]:
        """
        Get detailed statistics for a specific table.
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            Dictionary containing table statistics
        """
        with self.get_connection(database) as conn:
            # Basic table stats
            basic_stats = conn.execute(text(f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT slug) as unique_slugs,
                    MIN(timestamp) as first_timestamp,
                    MAX(timestamp) as last_timestamp
                FROM public."{table_name}"
                WHERE EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = '{table_name}' AND column_name IN ('slug', 'timestamp')
                )
            """)).fetchone()
            
            # Column statistics
            columns_query = text(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{table_name}' AND table_schema = 'public'
                ORDER BY ordinal_position
            """)
            columns = conn.execute(columns_query).fetchall()
            
            # Null statistics for each column
            null_stats = {}
            for column in columns:
                column_name = column[0]
                null_count = conn.execute(text(f"""
                    SELECT COUNT(*) FROM public."{table_name}" WHERE "{column_name}" IS NULL
                """)).scalar()
                
                total_rows = basic_stats[0] if basic_stats else 0
                null_ratio = null_count / total_rows if total_rows > 0 else 0
                
                null_stats[column_name] = {
                    'data_type': column[1],
                    'is_nullable': column[2] == 'YES',
                    'null_count': null_count,
                    'null_ratio': round(null_ratio, 4),
                    'risk_level': self.config.classify_risk_level('null_ratio', null_ratio)
                }
            
            return {
                'table_name': table_name,
                'total_rows': basic_stats[0] if basic_stats else 0,
                'unique_slugs': basic_stats[1] if basic_stats else 0,
                'first_timestamp': str(basic_stats[2]) if basic_stats and basic_stats[2] else None,
                'last_timestamp': str(basic_stats[3]) if basic_stats and basic_stats[3] else None,
                'column_statistics': null_stats
            }
    
    def execute_performance_query(self, database: str, query: str, description: str = "") -> Dict[str, Any]:
        """
        Execute a query and measure its performance.
        
        Args:
            database: Database name
            query: SQL query to execute
            description: Query description
            
        Returns:
            Dictionary containing query results and performance metrics
        """
        start_time = time.time()
        
        try:
            with self.get_connection(database) as conn:
                result = conn.execute(text(query))
                rows = result.fetchall()
                execution_time = time.time() - start_time
                
                return {
                    'database': database,
                    'description': description,
                    'execution_time_seconds': round(execution_time, 4),
                    'execution_time_ms': round(execution_time * 1000, 2),
                    'row_count': len(rows),
                    'status': 'success',
                    'risk_level': self.config.classify_risk_level('query_time', execution_time),
                    'rows': [dict(row._mapping) for row in rows[:10]]  # First 10 rows only
                }
                
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                'database': database,
                'description': description,
                'execution_time_seconds': round(execution_time, 4),
                'execution_time_ms': round(execution_time * 1000, 2),
                'status': 'error',
                'error': str(e),
                'risk_level': 'CRITICAL'
            }
    
    def get_index_usage_stats(self, database: str) -> List[Dict[str, Any]]:
        """
        Get index usage statistics for database optimization.
        
        Args:
            database: Database name
            
        Returns:
            List of index usage statistics
        """
        with self.get_connection(database) as conn:
            query = text("""
                SELECT 
                    schemaname,
                    tablename,
                    indexname,
                    idx_tup_read,
                    idx_tup_fetch,
                    idx_scan,
                    CASE 
                        WHEN idx_scan = 0 THEN 'UNUSED'
                        WHEN idx_scan < 10 THEN 'LOW_USAGE'
                        WHEN idx_scan < 100 THEN 'MEDIUM_USAGE'
                        ELSE 'HIGH_USAGE'
                    END as usage_category
                FROM pg_stat_user_indexes
                WHERE schemaname = 'public'
                ORDER BY idx_scan DESC, tablename, indexname
            """)
            
            results = []
            for row in conn.execute(query):
                results.append({
                    'schema': row[0],
                    'table': row[1],
                    'index': row[2],
                    'tuples_read': row[3],
                    'tuples_fetched': row[4],
                    'scan_count': row[5],
                    'usage_category': row[6],
                    'risk_level': 'HIGH' if row[6] == 'UNUSED' else 'LOW'
                })
            
            return results
    
    def test_connection(self, database: str) -> bool:
        """
        Test database connection health.
        
        Args:
            database: Database name to test
            
        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            with self.get_connection(database) as conn:
                # Simple test query
                result = conn.execute(text("SELECT 1"))
                test_value = result.fetchone()[0]
                return test_value == 1
        except Exception as e:
            logger.error(f"Connection test failed for {database}: {e}")
            return False
    
    def get_table_names(self, database: str, case_sensitive: bool = False) -> List[str]:
        """
        Get all table names from the database.
        
        Args:
            database: Database name
            case_sensitive: If False, returns lowercase names for comparison
            
        Returns:
            List of table names
        """
        try:
            with self.get_connection(database) as conn:
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    ORDER BY table_name
                """))
                
                tables = [row[0] for row in result.fetchall()]
                
                if not case_sensitive:
                    return [table.lower() for table in tables]
                return tables
                
        except Exception as e:
            logger.error(f"Failed to get table names for {database}: {e}")
            return []
    
    def table_exists(self, database: str, table_name: str) -> Dict[str, Any]:
        """
        Check if a table exists, handling case sensitivity.
        
        Args:
            database: Database name
            table_name: Table name to check
            
        Returns:
            Dict with existence info and actual table name
        """
        try:
            with self.get_connection(database) as conn:
                # First try exact match
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = :table_name
                """), {"table_name": table_name})
                
                exact_match = result.fetchone()
                if exact_match:
                    return {
                        'exists': True,
                        'actual_name': exact_match[0],
                        'match_type': 'exact'
                    }
                
                # Try case-insensitive match
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND LOWER(table_name) = LOWER(:table_name)
                """), {"table_name": table_name})
                
                case_match = result.fetchone()
                if case_match:
                    return {
                        'exists': True,
                        'actual_name': case_match[0],
                        'match_type': 'case_insensitive'
                    }
                
                return {
                    'exists': False,
                    'actual_name': None,
                    'match_type': 'none'
                }
                
        except Exception as e:
            logger.error(f"Failed to check table existence for {table_name}: {e}")
            return {
                'exists': False,
                'actual_name': None,
                'match_type': 'error',
                'error': str(e)
            }
    
    def execute_performance_query_safe(self, database: str, table_name: str, query_template: str, description: str = "") -> Dict[str, Any]:
        """
        Execute a performance query with proper table name handling.
        
        Args:
            database: Database name
            table_name: Table name (will be verified for case sensitivity)
            query_template: Query template with {table} placeholder
            description: Query description
            
        Returns:
            Query execution results with table name resolution info
        """
        # Check if table exists and get actual name
        table_info = self.table_exists(database, table_name)
        
        if not table_info['exists']:
            return {
                'status': 'table_not_found',
                'table_requested': table_name,
                'table_info': table_info,
                'execution_time_seconds': 0,
                'rows': []
            }
        
        actual_table_name = table_info['actual_name']
        
        # Execute query with actual table name - use quoted identifier for safety
        query = query_template.format(table=f'"{actual_table_name}"')
        result = self.execute_performance_query(database, query, description)
        
        # Add table resolution info
        result['table_requested'] = table_name
        result['table_actual'] = actual_table_name
        result['table_match_type'] = table_info['match_type']
        
        return result

    def close_all_connections(self):
        """Close all database connections and clean up resources."""
        for db_name, engine in self.engines.items():
            try:
                engine.dispose()
                logger.info(f"Closed connections for {db_name}")
            except Exception as e:
                logger.error(f"Error closing connections for {db_name}: {e}")
        
        self.engines.clear()