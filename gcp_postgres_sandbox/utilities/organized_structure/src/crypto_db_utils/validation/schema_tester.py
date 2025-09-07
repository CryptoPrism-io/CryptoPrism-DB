#!/usr/bin/env python3
"""
Quick Schema Test - Simplified schema extraction for testing
"""

import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect
from datetime import datetime

load_dotenv()

def quick_schema_test():
    """Quick test of schema extraction focusing on primary keys and indexes only."""
    
    # Database connection
    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'), 
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'dbcp')
    }
    
    conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(conn_string)
    inspector = inspect(engine)
    
    print(f"Connected to database: {db_config['database']}")
    
    # Get table names
    table_names = inspector.get_table_names(schema='public')
    print(f"Found {len(table_names)} tables")
    
    tables_analysis = {}
    
    for table_name in table_names:
        print(f"Processing {table_name}...")
        
        # Get basic info only - no statistics
        columns = inspector.get_columns(table_name)
        primary_key = inspector.get_pk_constraint(table_name)
        indexes = inspector.get_indexes(table_name)
        
        # Quick analysis
        has_pk = bool(primary_key.get('constrained_columns', []))
        column_names = [col['name'].lower() for col in columns]
        
        # Check for common patterns
        has_slug = 'slug' in column_names
        has_timestamp = any('timestamp' in col or 'date' in col for col in column_names)
        
        tables_analysis[table_name] = {
            'columns': len(columns),
            'has_primary_key': has_pk,
            'primary_key_columns': primary_key.get('constrained_columns', []),
            'index_count': len(indexes),
            'has_slug': has_slug,
            'has_timestamp': has_timestamp,
            'needs_optimization': not has_pk and (has_slug or has_timestamp)
        }
    
    # Summary
    print("\n" + "="*60)
    print("QUICK SCHEMA ANALYSIS SUMMARY")
    print("="*60)
    
    tables_without_pk = [name for name, info in tables_analysis.items() if not info['has_primary_key']]
    optimization_candidates = [name for name, info in tables_analysis.items() if info['needs_optimization']]
    
    print(f"Total tables: {len(tables_analysis)}")
    print(f"Tables without primary key: {len(tables_without_pk)}")
    print(f"Optimization candidates (no PK + has slug/timestamp): {len(optimization_candidates)}")
    
    if tables_without_pk:
        print(f"\nTables without primary keys:")
        for table in tables_without_pk:
            info = tables_analysis[table]
            patterns = []
            if info['has_slug']: patterns.append('slug')
            if info['has_timestamp']: patterns.append('timestamp')
            pattern_str = f" ({', '.join(patterns)})" if patterns else ""
            print(f"  - {table}{pattern_str}")
    
    if optimization_candidates:
        print(f"\nHigh-priority optimization candidates:")
        for table in optimization_candidates:
            info = tables_analysis[table]
            print(f"  - {table}: {info['columns']} columns, {info['index_count']} indexes")
    
    engine.dispose()
    return tables_analysis

if __name__ == "__main__":
    quick_schema_test()