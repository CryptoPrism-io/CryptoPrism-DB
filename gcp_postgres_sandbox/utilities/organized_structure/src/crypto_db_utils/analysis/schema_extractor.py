#!/usr/bin/env python3
"""
Database Schema Extractor for CryptoPrism-DB Performance Optimization

This script extracts complete database schema information to structured JSON files
for analysis and optimization planning. It focuses on identifying missing primary keys,
indexes, and performance optimization opportunities.

Features:
- Extracts schema from all 3 databases (dbcp, cp_ai, cp_backtest)
- Identifies tables without primary keys
- Analyzes column types and constraints
- Calculates table statistics (row counts, sizes)
- Detects optimization opportunities
- Outputs structured JSON for further analysis

Requirements:
- sqlalchemy>=2.0.0
- psycopg2-binary>=2.9.0
- python-dotenv (for environment variables)
"""

import os
import sys
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from decimal import Decimal
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, inspect, text
from collections import defaultdict

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Decimal types."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class DatabaseSchemaExtractor:
    """Extract comprehensive schema information from PostgreSQL databases."""
    
    def __init__(self):
        """Initialize with environment variables."""
        load_dotenv()
        
        # Database connection parameters
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'), 
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # Database names - Focus on dbcp only for testing
        self.databases = {
            'main': os.getenv('DB_NAME', 'dbcp')
        }
        
        # Validate environment variables
        required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise EnvironmentError(f"Missing required environment variables: {missing_vars}")
    
    def create_connection_string(self, database_name: str) -> str:
        """Create PostgreSQL connection string."""
        return (f"postgresql+psycopg2://{self.db_config['user']}:"
                f"{self.db_config['password']}@{self.db_config['host']}:"
                f"{self.db_config['port']}/{database_name}")
    
    def get_table_statistics(self, engine, table_name: str) -> Dict[str, Any]:
        """Get table row count and size statistics."""
        stats = {
            'row_count': 0,
            'size_mb': 0.0,
            'last_analyzed': None
        }
        
        try:
            # Get fast statistics from system catalogs
            with engine.connect() as conn:
                # Use pg_stat_user_tables for approximate row count (much faster than COUNT(*))
                stats_query = text("""
                    SELECT 
                        COALESCE(n_tup_ins - n_tup_del, 0) as approx_row_count,
                        pg_total_relation_size('"' || schemaname || '"."' || relname || '"') / 1024.0 / 1024.0 as size_mb,
                        last_analyze,
                        last_autoanalyze
                    FROM pg_stat_user_tables 
                    WHERE relname = :table_name AND schemaname = 'public'
                """)
                result = conn.execute(stats_query, {'table_name': table_name})
                row = result.fetchone()
                
                if row:
                    stats['row_count'] = max(int(row[0] or 0), 0)
                    stats['size_mb'] = round(row[1] or 0.0, 2)
                    last_analyze = row[2] or row[3]
                    stats['last_analyzed'] = str(last_analyze) if last_analyze else None
                else:
                    # Fallback to basic size calculation if no stats available
                    size_query = text("""
                        SELECT pg_total_relation_size('"' || :table_name || '"') / 1024.0 / 1024.0 as size_mb
                    """)
                    result = conn.execute(size_query, {'table_name': table_name})
                    stats['size_mb'] = round(result.scalar() or 0.0, 2)
                    
        except Exception as e:
            logger.warning(f"Could not get statistics for table {table_name}: {e}")
        
        return stats
    
    def analyze_optimization_opportunities(self, table_info: Dict[str, Any]) -> List[str]:
        """Identify optimization opportunities for a table."""
        opportunities = []
        
        # Check for missing primary key
        if not table_info['primary_key']:
            opportunities.append("MISSING_PRIMARY_KEY")
        
        # Check for large tables without indexes
        if table_info['statistics']['row_count'] > 10000 and not table_info['indexes']:
            opportunities.append("LARGE_TABLE_NO_INDEXES")
        
        # Check for time-series pattern (has slug and timestamp columns)
        column_names = [col['name'].lower() for col in table_info['columns']]
        if 'slug' in column_names and any('timestamp' in col or 'date' in col for col in column_names):
            if not table_info['primary_key']:
                opportunities.append("TIME_SERIES_NEEDS_COMPOSITE_PK")
        
        # Check for potential index candidates (frequently queried columns)
        potential_index_columns = []
        for col in table_info['columns']:
            col_name = col['name'].lower()
            if any(keyword in col_name for keyword in ['slug', 'timestamp', 'date', 'id']):
                potential_index_columns.append(col['name'])
        
        if potential_index_columns and len(table_info['indexes']) < 2:
            opportunities.append(f"NEEDS_INDEXES_ON: {', '.join(potential_index_columns)}")
        
        # Check for oversized text columns
        for col in table_info['columns']:
            if 'VARCHAR' in str(col['type']).upper() and '255' in str(col['type']):
                opportunities.append(f"OVERSIZED_VARCHAR: {col['name']}")
        
        return opportunities
    
    def extract_table_schema(self, engine, table_name: str) -> Dict[str, Any]:
        """Extract comprehensive schema information for a single table."""
        inspector = inspect(engine)
        
        # Get column information
        columns = []
        for col in inspector.get_columns(table_name):
            columns.append({
                'name': col['name'],
                'type': str(col['type']),
                'nullable': col['nullable'],
                'default': str(col['default']) if col['default'] is not None else None,
                'autoincrement': col.get('autoincrement', False),
                'comment': col.get('comment', None)
            })
        
        # Get primary key information
        primary_key = inspector.get_pk_constraint(table_name)
        pk_columns = primary_key.get('constrained_columns', []) if primary_key else []
        
        # Get foreign key information
        foreign_keys = []
        for fk in inspector.get_foreign_keys(table_name):
            foreign_keys.append({
                'name': fk['name'],
                'constrained_columns': fk['constrained_columns'],
                'referred_table': fk['referred_table'],
                'referred_columns': fk['referred_columns']
            })
        
        # Get unique constraints
        unique_constraints = []
        for uc in inspector.get_unique_constraints(table_name):
            unique_constraints.append({
                'name': uc['name'],
                'column_names': uc['column_names']
            })
        
        # Get check constraints
        check_constraints = []
        try:
            for cc in inspector.get_check_constraints(table_name):
                check_constraints.append({
                    'name': cc['name'],
                    'sqltext': cc['sqltext']
                })
        except Exception:
            # Some PostgreSQL versions might not support this
            pass
        
        # Get index information
        indexes = []
        for idx in inspector.get_indexes(table_name):
            indexes.append({
                'name': idx['name'],
                'column_names': idx['column_names'],
                'unique': idx['unique'],
                'type': idx.get('type', 'btree')
            })
        
        # Get table statistics
        statistics = self.get_table_statistics(engine, table_name)
        
        # Build table information structure
        table_info = {
            'name': table_name,
            'columns': columns,
            'primary_key': pk_columns,
            'foreign_keys': foreign_keys,
            'unique_constraints': unique_constraints,
            'check_constraints': check_constraints,
            'indexes': indexes,
            'statistics': statistics,
            'optimization_opportunities': []
        }
        
        # Analyze optimization opportunities
        table_info['optimization_opportunities'] = self.analyze_optimization_opportunities(table_info)
        
        return table_info
    
    def extract_database_schema(self, database_key: str) -> Dict[str, Any]:
        """Extract schema for a single database."""
        database_name = self.databases[database_key]
        logger.info(f"Extracting schema for {database_name} ({database_key})")
        
        try:
            # Create database connection
            conn_string = self.create_connection_string(database_name)
            engine = create_engine(conn_string)
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            inspector = inspect(engine)
            
            # Get all table names
            table_names = inspector.get_table_names(schema='public')
            logger.info(f"Found {len(table_names)} tables in {database_name}")
            
            # Extract schema for each table
            tables = {}
            for table_name in table_names:
                try:
                    logger.info(f"Processing table: {table_name}")
                    tables[table_name] = self.extract_table_schema(engine, table_name)
                except Exception as e:
                    logger.error(f"Error processing table {table_name}: {e}")
                    continue
            
            # Create database summary
            total_tables = len(tables)
            total_rows = sum(table['statistics']['row_count'] for table in tables.values())
            total_size_mb = sum(table['statistics']['size_mb'] for table in tables.values())
            tables_without_pk = sum(1 for table in tables.values() if not table['primary_key'])
            tables_without_indexes = sum(1 for table in tables.values() if not table['indexes'])
            
            # Group tables by prefix for analysis
            table_groups = defaultdict(list)
            for table_name in tables.keys():
                prefix = table_name.split('_')[0] if '_' in table_name else 'other'
                table_groups[prefix].append(table_name)
            
            # Compile database schema information
            schema_info = {
                'database_name': database_name,
                'database_key': database_key,
                'extraction_timestamp': datetime.now().isoformat(),
                'summary': {
                    'total_tables': total_tables,
                    'total_rows': total_rows,
                    'total_size_mb': round(total_size_mb, 2),
                    'tables_without_primary_keys': tables_without_pk,
                    'tables_without_indexes': tables_without_indexes,
                    'table_groups': dict(table_groups)
                },
                'tables': tables
            }
            
            engine.dispose()
            return schema_info
            
        except Exception as e:
            logger.error(f"Error extracting schema for {database_name}: {e}")
            return {
                'database_name': database_name,
                'database_key': database_key,
                'extraction_timestamp': datetime.now().isoformat(),
                'error': str(e),
                'tables': {}
            }
    
    def extract_all_schemas(self) -> Dict[str, Any]:
        """Extract schemas from all configured databases."""
        logger.info("Starting schema extraction for all databases")
        
        schemas = {}
        for db_key in self.databases.keys():
            schemas[db_key] = self.extract_database_schema(db_key)
        
        # Create comprehensive analysis
        analysis = {
            'extraction_timestamp': datetime.now().isoformat(),
            'extractor_version': '1.0.0',
            'databases': schemas,
            'cross_database_analysis': self.analyze_cross_database_patterns(schemas)
        }
        
        return analysis
    
    def analyze_cross_database_patterns(self, schemas: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze patterns across all databases."""
        analysis = {
            'common_table_prefixes': set(),
            'tables_missing_pk_across_dbs': [],
            'largest_tables': [],
            'optimization_priorities': []
        }
        
        all_tables = []
        for db_key, schema in schemas.items():
            if 'tables' in schema:
                for table_name, table_info in schema['tables'].items():
                    # Collect all tables with metadata
                    all_tables.append({
                        'database': db_key,
                        'name': table_name,
                        'row_count': table_info['statistics']['row_count'],
                        'size_mb': table_info['statistics']['size_mb'],
                        'has_primary_key': bool(table_info['primary_key']),
                        'opportunities': table_info['optimization_opportunities']
                    })
                    
                    # Track common prefixes
                    if '_' in table_name:
                        prefix = table_name.split('_')[0]
                        analysis['common_table_prefixes'].add(prefix)
        
        # Convert set to list for JSON serialization
        analysis['common_table_prefixes'] = list(analysis['common_table_prefixes'])
        
        # Find largest tables for optimization priority
        analysis['largest_tables'] = sorted(
            all_tables, 
            key=lambda x: x['row_count'], 
            reverse=True
        )[:10]
        
        # Find tables missing primary keys
        analysis['tables_missing_pk_across_dbs'] = [
            {'database': t['database'], 'table': t['name'], 'rows': t['row_count']}
            for t in all_tables if not t['has_primary_key']
        ]
        
        # Priority optimization recommendations
        high_priority = []
        for table in all_tables:
            if table['row_count'] > 10000 and not table['has_primary_key']:
                high_priority.append(f"{table['database']}.{table['name']} - Large table without PK ({table['row_count']:,} rows)")
        
        analysis['optimization_priorities'] = high_priority
        
        return analysis
    
    def save_to_json(self, data: Dict[str, Any], output_dir: str = "database_analysis") -> str:
        """Save schema analysis to JSON file."""
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"database_schema_analysis_{timestamp}.json"
        filepath = output_path / filename
        
        # Save to JSON file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, cls=DecimalEncoder)
        
        logger.info(f"Schema analysis saved to: {filepath}")
        return str(filepath)
    
    def generate_summary_report(self, analysis: Dict[str, Any]) -> str:
        """Generate a human-readable summary report."""
        report = []
        report.append("=" * 80)
        report.append("DATABASE SCHEMA ANALYSIS SUMMARY")
        report.append("=" * 80)
        report.append(f"Generated: {analysis['extraction_timestamp']}")
        report.append("")
        
        # Database summaries
        for db_key, db_info in analysis['databases'].items():
            if 'error' in db_info:
                report.append(f"❌ {db_info['database_name'].upper()} ({db_key}): ERROR - {db_info['error']}")
                continue
            
            summary = db_info['summary']
            report.append(f"📊 {db_info['database_name'].upper()} ({db_key}):")
            report.append(f"   Tables: {summary['total_tables']}")
            report.append(f"   Total Rows: {summary['total_rows']:,}")
            report.append(f"   Total Size: {summary['total_size_mb']} MB")
            report.append(f"   Tables without Primary Keys: {summary['tables_without_primary_keys']}")
            report.append(f"   Tables without Indexes: {summary['tables_without_indexes']}")
            report.append("")
        
        # Cross-database analysis
        cross_analysis = analysis['cross_database_analysis']
        report.append("🔍 OPTIMIZATION PRIORITIES:")
        if cross_analysis['optimization_priorities']:
            for priority in cross_analysis['optimization_priorities']:
                report.append(f"   ⚠️  {priority}")
        else:
            report.append("   ✅ No high-priority optimization issues found")
        report.append("")
        
        report.append("📋 LARGEST TABLES:")
        for table in cross_analysis['largest_tables'][:5]:
            status = "❌ No PK" if not table['has_primary_key'] else "✅"
            report.append(f"   {status} {table['database']}.{table['name']}: {table['row_count']:,} rows ({table['size_mb']} MB)")
        
        return "\n".join(report)


def main():
    """Main execution function."""
    try:
        # Initialize extractor
        extractor = DatabaseSchemaExtractor()
        
        # Extract schemas from all databases
        analysis = extractor.extract_all_schemas()
        
        # Save to JSON file
        json_filepath = extractor.save_to_json(analysis)
        
        # Generate and display summary report
        summary = extractor.generate_summary_report(analysis)
        print(summary)
        
        # Save summary report as well
        summary_path = Path(json_filepath).parent / f"schema_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(summary)
        
        logger.info(f"Summary report saved to: {summary_path}")
        logger.info("Schema extraction completed successfully!")
        
        return json_filepath
        
    except Exception as e:
        logger.error(f"Schema extraction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()