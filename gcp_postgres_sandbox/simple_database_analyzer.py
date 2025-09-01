#!/usr/bin/env python3
"""
Simple Database Schema Analyzer for CryptoPrism-DB

This script analyzes PostgreSQL database schemas and generates comprehensive
text-based reports about tables, relationships, and structure. It provides
an alternative to visual ERD generation that doesn't require external dependencies.

Features:
- Connects to PostgreSQL databases using environment variables
- Generates detailed schema analysis reports
- Shows table relationships and foreign keys
- Analyzes column types and constraints
- Compatible with existing environment configuration
- No external dependencies beyond SQLAlchemy and psycopg2

Requirements:
- sqlalchemy>=2.0.0 (already in requirements.txt)
- psycopg2-binary>=2.9.0 (already in requirements.txt)
"""

import os
import sys
import argparse
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, inspect
from collections import defaultdict


def print_status(message, status="info"):
    """Print colored status messages with timestamps."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status_icons = {
        "success": "[SUCCESS]",
        "error": "[ERROR]", 
        "warning": "[WARNING]",
        "info": "[INFO]"
    }
    icon = status_icons.get(status, "[LOG]")
    print(f"[{timestamp}] {icon} {message}")


def load_environment():
    """Load environment variables from .env file if not in GitHub Actions."""
    if not os.getenv("GITHUB_ACTIONS") == "true":
        # Find project root and load .env
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        env_path = project_root / ".env"
        
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
            print_status(f"Loaded .env from: {env_path}", "success")
        else:
            print_status(f".env file not found at: {env_path}", "warning")
    else:
        print_status("Running in GitHub Actions: Using GitHub Secrets", "info")


def get_database_config():
    """Get database configuration from environment variables."""
    config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '5432'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'main_db': os.getenv('DB_NAME'),
        'backtest_db': os.getenv('DB_NAME_BT', 'cp_backtest')
    }
    
    # Validate required variables
    required_vars = ['host', 'user', 'password', 'main_db']
    missing_vars = [var for var in required_vars if not config[var]]
    
    if missing_vars:
        print_status(f"Missing required environment variables: {', '.join(missing_vars)}", "error")
        sys.exit(1)
    
    print_status(f"Database config loaded: {config['host']}:{config['port']}", "success")
    return config


def create_connection_string(config, database_name):
    """Create PostgreSQL connection string for SQLAlchemy."""
    return f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{database_name}"


def analyze_database_schema(connection_string, exclude_tables=None, schema=None):
    """Analyze database schema and return structured information."""
    try:
        print_status("Connecting to database...", "info")
        
        # Create engine and inspector
        engine = create_engine(connection_string)
        inspector = inspect(engine)
        metadata = MetaData()
        
        # Reflect database schema
        if schema:
            print_status(f"Using schema: {schema}", "info")
            metadata.reflect(bind=engine, schema=schema)
        else:
            metadata.reflect(bind=engine)
        
        print_status(f"Found {len(metadata.tables)} total tables", "info")
        
        # Filter tables if needed
        tables_to_analyze = metadata.tables
        if exclude_tables:
            print_status(f"Excluding tables matching: {', '.join(exclude_tables)}", "info")
            tables_to_analyze = {
                name: table for name, table in metadata.tables.items() 
                if not any(excluded in name.lower() for excluded in [ex.lower() for ex in exclude_tables])
            }
        
        print_status(f"Analyzing {len(tables_to_analyze)} tables", "info")
        
        # Analyze schema
        schema_info = {
            'database_name': engine.url.database,
            'total_tables': len(metadata.tables),
            'analyzed_tables': len(tables_to_analyze),
            'tables': {},
            'relationships': [],
            'table_groups': defaultdict(list)
        }
        
        # Analyze each table
        for table_name, table in tables_to_analyze.items():
            table_info = {
                'name': table_name,
                'columns': [],
                'primary_keys': [],
                'foreign_keys': [],
                'indexes': [],
                'constraints': []
            }
            
            # Analyze columns
            for column in table.columns:
                column_info = {
                    'name': column.name,
                    'type': str(column.type),
                    'nullable': column.nullable,
                    'default': str(column.default) if column.default else None,
                    'primary_key': column.primary_key,
                    'foreign_key': bool(column.foreign_keys)
                }
                table_info['columns'].append(column_info)
                
                if column.primary_key:
                    table_info['primary_keys'].append(column.name)
            
            # Analyze foreign keys
            for fk in table.foreign_keys:
                fk_info = {
                    'column': fk.parent.name,
                    'references_table': fk.column.table.name,
                    'references_column': fk.column.name
                }
                table_info['foreign_keys'].append(fk_info)
                
                # Add to relationships
                schema_info['relationships'].append({
                    'from_table': table_name,
                    'from_column': fk.parent.name,
                    'to_table': fk.column.table.name,
                    'to_column': fk.column.name
                })
            
            # Group tables by prefix for organization
            table_prefix = table_name.split('_')[0] if '_' in table_name else 'misc'
            schema_info['table_groups'][table_prefix].append(table_name)
            
            schema_info['tables'][table_name] = table_info
        
        # Close engine connection
        engine.dispose()
        
        print_status("Schema analysis completed successfully", "success")
        return schema_info
        
    except Exception as e:
        print_status(f"Error analyzing database schema: {str(e)}", "error")
        return None


def generate_schema_report(schema_info, output_file):
    """Generate a comprehensive text-based schema report."""
    try:
        print_status(f"Generating schema report: {output_file}", "info")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            # Header
            f.write("=" * 80 + "\n")
            f.write(f"DATABASE SCHEMA ANALYSIS REPORT\n")
            f.write(f"Database: {schema_info['database_name']}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            # Summary
            f.write("SUMMARY\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total Tables: {schema_info['total_tables']}\n")
            f.write(f"Analyzed Tables: {schema_info['analyzed_tables']}\n")
            f.write(f"Total Relationships: {len(schema_info['relationships'])}\n")
            f.write(f"Table Groups: {len(schema_info['table_groups'])}\n\n")
            
            # Table Groups
            f.write("TABLE GROUPS (by prefix)\n")
            f.write("-" * 40 + "\n")
            for group, tables in schema_info['table_groups'].items():
                f.write(f"{group.upper()}: {len(tables)} tables\n")
                for table in sorted(tables):
                    f.write(f"  - {table}\n")
                f.write("\n")
            
            # Relationships
            f.write("FOREIGN KEY RELATIONSHIPS\n")
            f.write("-" * 40 + "\n")
            for rel in schema_info['relationships']:
                f.write(f"{rel['from_table']}.{rel['from_column']} -> {rel['to_table']}.{rel['to_column']}\n")
            f.write("\n")
            
            # Detailed Table Information
            f.write("DETAILED TABLE ANALYSIS\n")
            f.write("-" * 40 + "\n")
            for table_name in sorted(schema_info['tables'].keys()):
                table_info = schema_info['tables'][table_name]
                
                f.write(f"\nTable: {table_name}\n")
                f.write("-" * len(f"Table: {table_name}") + "\n")
                
                # Primary keys
                if table_info['primary_keys']:
                    f.write(f"Primary Keys: {', '.join(table_info['primary_keys'])}\n")
                
                # Foreign keys
                if table_info['foreign_keys']:
                    f.write("Foreign Keys:\n")
                    for fk in table_info['foreign_keys']:
                        f.write(f"  {fk['column']} -> {fk['references_table']}.{fk['references_column']}\n")
                
                # Columns
                f.write("Columns:\n")
                for col in table_info['columns']:
                    nullable = "NULL" if col['nullable'] else "NOT NULL"
                    pk_marker = " [PK]" if col['primary_key'] else ""
                    fk_marker = " [FK]" if col['foreign_key'] else ""
                    default = f" DEFAULT {col['default']}" if col['default'] else ""
                    
                    f.write(f"  {col['name']} | {col['type']} | {nullable}{pk_marker}{fk_marker}{default}\n")
                
                f.write("\n")
        
        print_status(f"Schema report generated successfully: {output_file}", "success")
        return True
        
    except Exception as e:
        print_status(f"Error generating schema report: {str(e)}", "error")
        return False


def create_output_directory(output_dir="database_analysis"):
    """Create output directory for analysis reports."""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    return output_path


def analyze_databases(config, output_dir="database_analysis", exclude_tables=None):
    """Analyze both main and backtest databases."""
    output_path = create_output_directory(output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    databases = {
        'main': config['main_db'],
        'backtest': config['backtest_db']
    }
    
    results = {}
    
    for db_type, db_name in databases.items():
        print_status(f"Analyzing {db_type} database: {db_name}", "info")
        connection_string = create_connection_string(config, db_name)
        
        # Analyze schema
        schema_info = analyze_database_schema(
            connection_string=connection_string,
            exclude_tables=exclude_tables
        )
        
        if schema_info:
            # Generate report
            output_file = output_path / f"cryptoprism_{db_type}_schema_{timestamp}.txt"
            success = generate_schema_report(schema_info, output_file)
            results[db_type] = {"success": success, "path": output_file if success else None}
        else:
            results[db_type] = {"success": False, "path": None}
    
    return results


def print_summary(results):
    """Print summary of generated reports."""
    print_status("=" * 60, "info")
    print_status("DATABASE SCHEMA ANALYSIS SUMMARY", "info")
    print_status("=" * 60, "info")
    
    for db_type, result in results.items():
        print_status(f"\n{db_type.upper()} Database:", "info")
        if result["success"]:
            print_status(f"  Report: {result['path']}", "success")
        else:
            print_status(f"  Report: Failed to generate", "error")


def main():
    """Main function with command-line interface."""
    parser = argparse.ArgumentParser(
        description="Analyze CryptoPrism-DB PostgreSQL database schemas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python simple_database_analyzer.py                           # Analyze both databases
  python simple_database_analyzer.py --exclude temp audit     # Exclude system tables
  python simple_database_analyzer.py --output ./my_analysis   # Custom output directory
        """
    )
    
    parser.add_argument(
        "--output", 
        default="database_analysis",
        help="Output directory (default: database_analysis)"
    )
    
    parser.add_argument(
        "--exclude", 
        nargs="*",
        default=["temp", "audit", "pg_stat", "information_schema"],
        help="Tables to exclude from analysis (default: temp audit pg_stat information_schema)"
    )
    
    args = parser.parse_args()
    
    print_status("Starting CryptoPrism Database Schema Analysis", "info")
    
    # Load environment and configuration
    load_environment()
    config = get_database_config()
    
    # Analyze databases
    results = analyze_databases(
        config=config,
        output_dir=args.output,
        exclude_tables=args.exclude
    )
    
    # Print summary
    print_summary(results)
    
    # Check if any reports were generated successfully
    success_count = sum(1 for result in results.values() if result["success"])
    
    if success_count > 0:
        print_status(f"Successfully generated {success_count} schema reports!", "success")
        print_status("Check the output directory for your analysis files", "info")
    else:
        print_status("No schema reports were generated successfully", "error")
        sys.exit(1)


if __name__ == "__main__":
    main()