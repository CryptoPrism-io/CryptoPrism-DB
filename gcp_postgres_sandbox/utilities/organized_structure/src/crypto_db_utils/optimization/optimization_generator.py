#!/usr/bin/env python3
"""
Database Optimization Script Generator for CryptoPrism-DB

This script analyzes database schema data and generates targeted SQL optimization scripts
including primary keys, strategic indexes, and performance improvements. It focuses on
time-series cryptocurrency data patterns and query optimization.

Features:
- Reads schema analysis JSON files
- Generates primary key scripts for time-series tables
- Creates strategic index scripts for performance
- Provides rollback scripts for safe implementation
- Focuses on FE_* signal tables optimization
- Generates validation queries for monitoring

Requirements:
- sqlalchemy>=2.0.0
- psycopg2-binary>=2.9.0  
- python-dotenv
"""

import os
import sys
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple
from collections import defaultdict

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class OptimizationGenerator:
    """Generate targeted database optimization scripts based on schema analysis."""
    
    def __init__(self):
        """Initialize optimization generator."""
        # High-priority tables for optimization (based on CryptoPrism-DB usage)
        self.priority_tables = [
            'FE_DMV_ALL',           # Primary aggregated signals
            'FE_MOMENTUM_SIGNALS',  # Momentum indicators  
            'FE_OSCILLATORS_SIGNALS', # Technical oscillators
            'FE_RATIOS_SIGNALS',    # Financial ratios
            'FE_METRICS_SIGNAL',    # Fundamental metrics
            'FE_TVV_SIGNALS',       # Volume/value signals
            'FE_DMV_SCORES',        # Durability/Momentum/Valuation scores
            '1K_coins_ohlcv',       # OHLCV price data
            'crypto_listings_latest_1000'  # Cryptocurrency listings
        ]
        
        # Common time-series patterns
        self.time_series_indicators = [
            'timestamp', 'date', 'created_at', 'updated_at', 'last_updated'
        ]
        
        self.slug_indicators = [
            'slug', 'symbol', 'coin_id', 'cryptocurrency'
        ]
    
    def load_schema_analysis(self, filepath: str) -> Dict[str, Any]:
        """Load schema analysis JSON file."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Schema analysis file not found: {filepath}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in schema analysis file: {e}")
    
    def identify_time_series_pattern(self, table_info: Dict[str, Any]) -> Tuple[str, str]:
        """Identify time-series pattern columns (slug, timestamp) in a table."""
        columns = table_info.get('columns', [])
        column_names = [col['name'].lower() for col in columns]
        
        # Find slug column
        slug_column = None
        for col in columns:
            if col['name'].lower() in self.slug_indicators:
                slug_column = col['name']
                break
        
        # Find timestamp column
        timestamp_column = None
        for col in columns:
            col_name_lower = col['name'].lower()
            if any(indicator in col_name_lower for indicator in self.time_series_indicators):
                timestamp_column = col['name']
                break
        
        return slug_column, timestamp_column
    
    def generate_primary_key_script(self, database_name: str, tables: Dict[str, Any]) -> str:
        """Generate primary key creation script for database."""
        script_lines = []
        script_lines.append("-- ============================================================================")
        script_lines.append(f"-- PRIMARY KEY OPTIMIZATION SCRIPT FOR {database_name.upper()}")
        script_lines.append("-- ============================================================================")
        script_lines.append(f"-- Generated: {datetime.now().isoformat()}")
        script_lines.append("-- ")
        script_lines.append("-- This script adds primary keys to tables that are missing them.")
        script_lines.append("-- Focus on time-series tables with (slug, timestamp) composite keys.")
        script_lines.append("-- ")
        script_lines.append("-- IMPORTANT: Test on a backup database first!")
        script_lines.append("-- ============================================================================")
        script_lines.append("")
        
        # Process priority tables first
        processed_tables = set()
        
        for priority_table in self.priority_tables:
            if priority_table in tables and priority_table not in processed_tables:
                table_info = tables[priority_table]
                script_lines.extend(self._generate_table_primary_key(priority_table, table_info))
                processed_tables.add(priority_table)
        
        # Process remaining tables without primary keys
        for table_name, table_info in tables.items():
            if (table_name not in processed_tables and 
                not table_info.get('primary_key') and 
                table_info.get('statistics', {}).get('row_count', 0) > 0):
                script_lines.extend(self._generate_table_primary_key(table_name, table_info))
                processed_tables.add(table_name)
        
        script_lines.append("")
        script_lines.append("-- ============================================================================")
        script_lines.append("-- VALIDATION QUERIES")
        script_lines.append("-- ============================================================================")
        script_lines.append("")
        script_lines.append("-- Check which tables now have primary keys")
        script_lines.append("""
SELECT 
    tc.table_name,
    STRING_AGG(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as primary_key_columns
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
    AND tc.table_schema = 'public'
    AND tc.table_name LIKE 'FE_%'
GROUP BY tc.table_name
ORDER BY tc.table_name;
""")
        
        return "\n".join(script_lines)
    
    def _generate_table_primary_key(self, table_name: str, table_info: Dict[str, Any]) -> List[str]:
        """Generate primary key script for a specific table."""
        lines = []
        
        # Skip if table already has primary key
        if table_info.get('primary_key'):
            return lines
        
        lines.append(f"-- {'-' * 60}")
        lines.append(f"-- Table: {table_name}")
        lines.append(f"-- Rows: {table_info.get('statistics', {}).get('row_count', 0):,}")
        lines.append(f"-- Size: {table_info.get('statistics', {}).get('size_mb', 0)} MB")
        lines.append(f"-- {'-' * 60}")
        
        # Identify optimal primary key strategy
        slug_col, timestamp_col = self.identify_time_series_pattern(table_info)
        
        if slug_col and timestamp_col:
            # Time-series pattern: composite primary key (slug, timestamp)
            lines.append(f"-- Time-series table: Adding composite primary key ({slug_col}, {timestamp_col})")
            lines.append(f'ALTER TABLE "{table_name}" ADD CONSTRAINT "pk_{table_name.lower()}" ')
            lines.append(f'    PRIMARY KEY ("{slug_col}", "{timestamp_col}");')
            lines.append("")
            
        elif slug_col:
            # Reference table: slug as primary key
            lines.append(f"-- Reference table: Adding primary key ({slug_col})")
            lines.append(f'ALTER TABLE "{table_name}" ADD CONSTRAINT "pk_{table_name.lower()}" ')
            lines.append(f'    PRIMARY KEY ("{slug_col}");')
            lines.append("")
            
        else:
            # Look for id column or create synthetic key
            columns = table_info.get('columns', [])
            id_columns = [col for col in columns if col['name'].lower() in ['id', 'row_id']]
            
            if id_columns:
                id_col = id_columns[0]['name']
                lines.append(f"-- Using existing ID column: {id_col}")
                lines.append(f'ALTER TABLE "{table_name}" ADD CONSTRAINT "pk_{table_name.lower()}" ')
                lines.append(f'    PRIMARY KEY ("{id_col}");')
                lines.append("")
            else:
                # Suggest adding a synthetic primary key
                lines.append(f"-- WARNING: No suitable primary key candidates found")
                lines.append(f"-- Consider adding a synthetic primary key:")
                lines.append(f'-- ALTER TABLE "{table_name}" ADD COLUMN id SERIAL PRIMARY KEY;')
                lines.append("")
        
        return lines
    
    def generate_index_script(self, database_name: str, tables: Dict[str, Any]) -> str:
        """Generate strategic index creation script."""
        script_lines = []
        script_lines.append("-- ============================================================================")
        script_lines.append(f"-- STRATEGIC INDEX OPTIMIZATION SCRIPT FOR {database_name.upper()}")
        script_lines.append("-- ============================================================================")
        script_lines.append(f"-- Generated: {datetime.now().isoformat()}")
        script_lines.append("-- ")
        script_lines.append("-- This script creates strategic indexes for query performance optimization.")
        script_lines.append("-- Based on common query patterns in CryptoPrism-DB system.")
        script_lines.append("-- ")
        script_lines.append("-- IMPORTANT: Test on a backup database first!")
        script_lines.append("-- ============================================================================")
        script_lines.append("")
        
        # Process priority tables for indexing
        for priority_table in self.priority_tables:
            if priority_table in tables:
                table_info = tables[priority_table]
                script_lines.extend(self._generate_table_indexes(priority_table, table_info))
        
        script_lines.append("")
        script_lines.append("-- ============================================================================")
        script_lines.append("-- INDEX USAGE VALIDATION QUERIES")
        script_lines.append("-- ============================================================================")
        script_lines.append("")
        script_lines.append("-- Check index usage statistics")
        script_lines.append("""
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_tup_read,
    idx_tup_fetch,
    idx_scan
FROM pg_stat_user_indexes 
WHERE schemaname = 'public'
    AND tablename LIKE 'FE_%'
ORDER BY idx_scan DESC, tablename;
""")
        
        return "\n".join(script_lines)
    
    def _generate_table_indexes(self, table_name: str, table_info: Dict[str, Any]) -> List[str]:
        """Generate index script for a specific table."""
        lines = []
        
        # Check if table is large enough to benefit from indexes
        row_count = table_info.get('statistics', {}).get('row_count', 0)
        if row_count < 1000:  # Skip small tables
            return lines
        
        lines.append(f"-- {'-' * 60}")
        lines.append(f"-- Indexes for: {table_name}")
        lines.append(f"-- Rows: {row_count:,}")
        lines.append(f"-- {'-' * 60}")
        
        # Get existing indexes to avoid duplicates
        existing_indexes = set()
        for idx in table_info.get('indexes', []):
            existing_indexes.add(tuple(sorted(idx['column_names'])))
        
        # Identify key columns for indexing
        slug_col, timestamp_col = self.identify_time_series_pattern(table_info)
        columns = table_info.get('columns', [])
        
        # Create indexes based on table type and query patterns
        if table_name == 'FE_DMV_ALL':
            # Primary signal table - most queried
            indexes_to_create = [
                ([timestamp_col], "timestamp_idx", "Date range queries"),
                ([slug_col], "slug_idx", "Cryptocurrency filtering"),
                (['bullish'], "bullish_idx", "Bullish signal filtering"),
                (['momentum_score'], "momentum_score_idx", "Momentum analysis"),
                (['durability_score'], "durability_score_idx", "Durability analysis")
            ]
            
        elif 'MOMENTUM' in table_name:
            # Momentum signals table
            indexes_to_create = [
                ([timestamp_col], "timestamp_idx", "Date range queries"),
                ([slug_col], "slug_idx", "Cryptocurrency filtering"),
                (['rsi_14'], "rsi_14_idx", "RSI analysis"),
                (['roc_21'], "roc_21_idx", "ROC analysis")
            ]
            
        elif 'OSCILLATORS' in table_name:
            # Oscillators table
            indexes_to_create = [
                ([timestamp_col], "timestamp_idx", "Date range queries"),
                ([slug_col], "slug_idx", "Cryptocurrency filtering"),
                (['macd_signal'], "macd_signal_idx", "MACD analysis"),
                (['adx_14'], "adx_14_idx", "ADX trend analysis")
            ]
            
        elif table_name == '1K_coins_ohlcv':
            # OHLCV price data
            indexes_to_create = [
                ([timestamp_col], "timestamp_idx", "Date range queries"),
                ([slug_col], "slug_idx", "Cryptocurrency filtering"),
                (['volume'], "volume_idx", "Volume filtering"),
                (['close'], "close_idx", "Price analysis")
            ]
            
        else:
            # Generic time-series table
            indexes_to_create = []
            if slug_col:
                indexes_to_create.append(([slug_col], "slug_idx", "Cryptocurrency filtering"))
            if timestamp_col:
                indexes_to_create.append(([timestamp_col], "timestamp_idx", "Date range queries"))
        
        # Generate index creation statements
        for idx_columns, idx_suffix, description in indexes_to_create:
            # Check if columns exist
            column_names = [col['name'] for col in columns]
            valid_columns = [col for col in idx_columns if col in column_names]
            
            if not valid_columns:
                continue
            
            # Check if index already exists
            if tuple(sorted(valid_columns)) in existing_indexes:
                continue
            
            idx_name = f"idx_{table_name.lower()}_{idx_suffix}"
            column_list = ', '.join([f'"{col}"' for col in valid_columns])
            
            lines.append(f"-- {description}")
            lines.append(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table_name}" ({column_list});')
            lines.append("")
        
        return lines
    
    def generate_rollback_script(self, database_name: str, tables: Dict[str, Any]) -> str:
        """Generate rollback script to remove optimizations if needed."""
        script_lines = []
        script_lines.append("-- ============================================================================")
        script_lines.append(f"-- ROLLBACK SCRIPT FOR {database_name.upper()} OPTIMIZATIONS")
        script_lines.append("-- ============================================================================")
        script_lines.append(f"-- Generated: {datetime.now().isoformat()}")
        script_lines.append("-- ")
        script_lines.append("-- WARNING: This script removes all optimizations!")
        script_lines.append("-- Only run this if you need to rollback the optimization changes.")
        script_lines.append("-- ")
        script_lines.append("-- ============================================================================")
        script_lines.append("")
        
        script_lines.append("-- Drop all optimization indexes")
        for table_name in self.priority_tables:
            if table_name in tables:
                script_lines.append(f'DROP INDEX IF EXISTS "idx_{table_name.lower()}_timestamp_idx";')
                script_lines.append(f'DROP INDEX IF EXISTS "idx_{table_name.lower()}_slug_idx";')
                script_lines.append(f'DROP INDEX IF EXISTS "idx_{table_name.lower()}_bullish_idx";')
                script_lines.append(f'DROP INDEX IF EXISTS "idx_{table_name.lower()}_momentum_score_idx";')
                script_lines.append(f'DROP INDEX IF EXISTS "idx_{table_name.lower()}_durability_score_idx";')
                script_lines.append(f'DROP INDEX IF EXISTS "idx_{table_name.lower()}_rsi_14_idx";')
                script_lines.append(f'DROP INDEX IF EXISTS "idx_{table_name.lower()}_roc_21_idx";')
                script_lines.append(f'DROP INDEX IF EXISTS "idx_{table_name.lower()}_macd_signal_idx";')
                script_lines.append(f'DROP INDEX IF EXISTS "idx_{table_name.lower()}_adx_14_idx";')
                script_lines.append(f'DROP INDEX IF EXISTS "idx_{table_name.lower()}_volume_idx";')
                script_lines.append(f'DROP INDEX IF EXISTS "idx_{table_name.lower()}_close_idx";')
        
        script_lines.append("")
        script_lines.append("-- Drop primary key constraints (WARNING: This may affect data integrity)")
        script_lines.append("-- Uncomment only if absolutely necessary:")
        
        for table_name in tables.keys():
            if not tables[table_name].get('primary_key'):  # Only for tables that didn't have PKs
                script_lines.append(f'-- ALTER TABLE "{table_name}" DROP CONSTRAINT IF EXISTS "pk_{table_name.lower()}";')
        
        return "\n".join(script_lines)
    
    def generate_monitoring_script(self, database_name: str) -> str:
        """Generate performance monitoring and validation script."""
        script_lines = []
        script_lines.append("-- ============================================================================")
        script_lines.append(f"-- PERFORMANCE MONITORING SCRIPT FOR {database_name.upper()}")
        script_lines.append("-- ============================================================================")
        script_lines.append(f"-- Generated: {datetime.now().isoformat()}")
        script_lines.append("-- ")
        script_lines.append("-- Use these queries to monitor optimization effectiveness")
        script_lines.append("-- ============================================================================")
        script_lines.append("")
        
        monitoring_queries = [
            ("Table Sizes and Row Counts", """
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes,
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_name = pt.tablename AND table_schema = pt.schemaname) as column_count
FROM pg_tables pt 
WHERE schemaname = 'public' 
    AND tablename LIKE 'FE_%'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;"""),
            
            ("Primary Key Status", """
SELECT 
    t.table_name,
    CASE WHEN tc.constraint_name IS NOT NULL THEN 'YES' ELSE 'NO' END as has_primary_key,
    STRING_AGG(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as pk_columns
FROM information_schema.tables t
LEFT JOIN information_schema.table_constraints tc 
    ON t.table_name = tc.table_name 
    AND tc.constraint_type = 'PRIMARY KEY'
LEFT JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
WHERE t.table_schema = 'public' 
    AND t.table_name LIKE 'FE_%'
GROUP BY t.table_name, tc.constraint_name
ORDER BY t.table_name;"""),
            
            ("Index Usage Statistics", """
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as times_used,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes 
WHERE schemaname = 'public'
    AND tablename LIKE 'FE_%'
ORDER BY idx_scan DESC, tablename;"""),
            
            ("Query Performance Monitoring", """
-- Enable query statistics collection (run once)
-- SELECT pg_stat_statements_reset();

-- View slowest queries (requires pg_stat_statements extension)
SELECT 
    query,
    calls,
    total_exec_time,
    mean_exec_time,
    stddev_exec_time,
    rows
FROM pg_stat_statements 
WHERE query ILIKE '%FE_%'
ORDER BY mean_exec_time DESC
LIMIT 10;""")
        ]
        
        for title, query in monitoring_queries:
            script_lines.append(f"-- {title}")
            script_lines.append("-" * (len(title) + 3))
            script_lines.append(query.strip())
            script_lines.append("")
        
        return "\n".join(script_lines)
    
    def generate_all_optimization_scripts(self, schema_filepath: str, output_dir: str = "sql_optimizations") -> Dict[str, str]:
        """Generate all optimization scripts from schema analysis."""
        logger.info(f"Loading schema analysis from: {schema_filepath}")
        schema_analysis = self.load_schema_analysis(schema_filepath)
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        generated_files = {}
        
        # Process each database
        for db_key, db_info in schema_analysis.get('databases', {}).items():
            if 'error' in db_info or 'tables' not in db_info:
                logger.warning(f"Skipping {db_key} database due to errors")
                continue
            
            database_name = db_info['database_name']
            tables = db_info['tables']
            
            logger.info(f"Generating optimization scripts for {database_name}")
            
            # Generate timestamp for file naming
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Generate primary key script
            pk_script = self.generate_primary_key_script(database_name, tables)
            pk_filename = f"01_primary_keys_{database_name}_{timestamp}.sql"
            pk_filepath = output_path / pk_filename
            
            with open(pk_filepath, 'w', encoding='utf-8') as f:
                f.write(pk_script)
            
            generated_files[f"{database_name}_primary_keys"] = str(pk_filepath)
            logger.info(f"Primary key script saved: {pk_filepath}")
            
            # Generate index script
            idx_script = self.generate_index_script(database_name, tables)
            idx_filename = f"02_strategic_indexes_{database_name}_{timestamp}.sql"
            idx_filepath = output_path / idx_filename
            
            with open(idx_filepath, 'w', encoding='utf-8') as f:
                f.write(idx_script)
            
            generated_files[f"{database_name}_indexes"] = str(idx_filepath)
            logger.info(f"Index script saved: {idx_filepath}")
            
            # Generate rollback script
            rollback_script = self.generate_rollback_script(database_name, tables)
            rollback_filename = f"03_rollback_{database_name}_{timestamp}.sql"
            rollback_filepath = output_path / rollback_filename
            
            with open(rollback_filepath, 'w', encoding='utf-8') as f:
                f.write(rollback_script)
            
            generated_files[f"{database_name}_rollback"] = str(rollback_filepath)
            logger.info(f"Rollback script saved: {rollback_filepath}")
            
            # Generate monitoring script
            monitoring_script = self.generate_monitoring_script(database_name)
            monitoring_filename = f"04_monitoring_{database_name}_{timestamp}.sql"
            monitoring_filepath = output_path / monitoring_filename
            
            with open(monitoring_filepath, 'w', encoding='utf-8') as f:
                f.write(monitoring_script)
            
            generated_files[f"{database_name}_monitoring"] = str(monitoring_filepath)
            logger.info(f"Monitoring script saved: {monitoring_filepath}")
        
        return generated_files


def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate database optimization scripts')
    parser.add_argument('schema_file', help='Path to schema analysis JSON file')
    parser.add_argument('--output', '-o', default='sql_optimizations', 
                       help='Output directory for optimization scripts')
    
    args = parser.parse_args()
    
    try:
        generator = OptimizationGenerator()
        generated_files = generator.generate_all_optimization_scripts(args.schema_file, args.output)
        
        print("\n" + "=" * 80)
        print("DATABASE OPTIMIZATION SCRIPTS GENERATED")
        print("=" * 80)
        
        for script_type, filepath in generated_files.items():
            print(f"✅ {script_type}: {filepath}")
        
        print("\n📋 NEXT STEPS:")
        print("1. Review the generated SQL scripts")
        print("2. Test on a backup database first")
        print("3. Run benchmark tests before optimization")
        print("4. Execute primary key script first")
        print("5. Execute index script second")
        print("6. Run benchmark tests after optimization")
        print("7. Use monitoring script to track performance")
        print("8. Keep rollback script ready for emergencies")
        
        logger.info("Optimization script generation completed successfully!")
        
    except Exception as e:
        logger.error(f"Optimization script generation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()