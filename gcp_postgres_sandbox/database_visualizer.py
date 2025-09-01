#!/usr/bin/env python3
"""
Database Schema Visualizer for CryptoPrism-DB

This script generates Entity Relationship Diagrams (ERDs) for PostgreSQL databases
using SQLAlchemy Schema Display. It supports both main and backtest databases with 
comprehensive visualization options.

Features:
- Connects to PostgreSQL databases using environment variables
- Generates ERD diagrams in multiple formats (PNG, PDF, SVG)
- Supports table filtering and schema selection
- Compatible with existing environment configuration
- Provides both command-line and programmatic interfaces

Requirements:
- sqlalchemy-schemadisplay>=1.3
- graphviz>=0.20.0
- psycopg2-binary>=2.9.0 (already in requirements.txt)
"""

import os
import sys
import argparse
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData
from sqlalchemy_schemadisplay import create_schema_graph
import platform


def setup_graphviz_path():
    """Ensure Graphviz is available in PATH, especially on Windows."""
    if platform.system() == "Windows":
        # Common Graphviz installation paths on Windows
        possible_paths = [
            r"C:\Program Files\Graphviz\bin",
            r"C:\Program Files (x86)\Graphviz\bin",
            r"C:\Graphviz\bin"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                if path not in os.environ.get("PATH", ""):
                    os.environ["PATH"] = path + ";" + os.environ.get("PATH", "")
                    print_status(f"Added Graphviz to PATH: {path}", "info")
                return True
        
        print_status("Graphviz not found in common Windows locations", "warning")
        return False
    return True


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


def generate_erd(connection_string, output_path, exclude_tables=None, schema=None, format_type="png"):
    """Generate ERD diagram using SQLAlchemy Schema Display."""
    try:
        print_status(f"Generating ERD diagram: {output_path}", "info")
        
        # Create engine and reflect metadata
        engine = create_engine(connection_string)
        metadata = MetaData()
        
        # Reflect database schema
        if schema:
            print_status(f"Using schema: {schema}", "info")
            metadata.reflect(bind=engine, schema=schema)
        else:
            metadata.reflect(bind=engine)
        
        # Filter tables if needed
        if exclude_tables:
            print_status(f"Excluding tables: {', '.join(exclude_tables)}", "info")
            filtered_tables = {name: table for name, table in metadata.tables.items() 
                             if not any(excluded in name for excluded in exclude_tables)}
            # Create new metadata with filtered tables
            filtered_metadata = MetaData()
            for table in filtered_tables.values():
                table.to_metadata(filtered_metadata)
            metadata = filtered_metadata
        
        print_status(f"Found {len(metadata.tables)} tables to visualize", "info")
        
        # Generate the schema graph
        graph = create_schema_graph(
            metadata=metadata,
            engine=engine,
            show_datatypes=True,
            show_indexes=True,
            rankdir='TB',  # Top to bottom layout
            concentrate=False
        )
        
        # Render the graph
        graph.write(output_path, format=format_type)
        print_status(f"ERD generated successfully: {output_path}", "success")
        
        # Close engine connection
        engine.dispose()
        return True
        
    except Exception as e:
        print_status(f"Error generating ERD: {str(e)}", "error")
        return False


def create_output_directory(output_dir="database_diagrams"):
    """Create output directory for diagrams."""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    return output_path


def visualize_databases(config, output_dir="database_diagrams", formats=None, exclude_tables=None):
    """Generate ERD diagrams for both main and backtest databases."""
    if formats is None:
        formats = ["png", "pdf", "svg"]
    
    output_path = create_output_directory(output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    databases = {
        'main': config['main_db'],
        'backtest': config['backtest_db']
    }
    
    results = {}
    
    for db_type, db_name in databases.items():
        print_status(f"Processing {db_type} database: {db_name}", "info")
        connection_string = create_connection_string(config, db_name)
        
        db_results = {}
        for fmt in formats:
            output_file = output_path / f"cryptoprism_{db_type}_erd_{timestamp}.{fmt}"
            success = generate_erd(
                connection_string=connection_string,
                output_path=str(output_file),
                exclude_tables=exclude_tables,
                format_type=fmt
            )
            db_results[fmt] = {"success": success, "path": output_file if success else None}
        
        results[db_type] = db_results
    
    return results


def print_summary(results):
    """Print summary of generated diagrams."""
    print_status("=" * 60, "info")
    print_status("DATABASE VISUALIZATION SUMMARY", "info")
    print_status("=" * 60, "info")
    
    for db_type, db_results in results.items():
        print_status(f"\n{db_type.upper()} Database:", "info")
        for fmt, result in db_results.items():
            if result["success"]:
                print_status(f"  {fmt.upper()}: {result['path']}", "success")
            else:
                print_status(f"  {fmt.upper()}: Failed to generate", "error")


def main():
    """Main function with command-line interface."""
    parser = argparse.ArgumentParser(
        description="Generate ERD diagrams for CryptoPrism-DB PostgreSQL databases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python database_visualizer.py                           # Generate all formats
  python database_visualizer.py --formats png            # PNG only
  python database_visualizer.py --formats png pdf        # PNG and PDF
  python database_visualizer.py --exclude temp audit     # Exclude tables
  python database_visualizer.py --output ./my_diagrams   # Custom output directory
        """
    )
    
    parser.add_argument(
        "--formats", 
        nargs="+", 
        choices=["png", "pdf", "svg"], 
        default=["png", "pdf", "svg"],
        help="Output formats (default: png pdf svg)"
    )
    
    parser.add_argument(
        "--output", 
        default="database_diagrams",
        help="Output directory (default: database_diagrams)"
    )
    
    parser.add_argument(
        "--exclude", 
        nargs="*",
        help="Tables to exclude from diagrams"
    )
    
    parser.add_argument(
        "--schema",
        help="Specific schema to visualize (default: all accessible schemas)"
    )
    
    args = parser.parse_args()
    
    print_status("Starting CryptoPrism Database Visualization", "info")
    
    # Setup Graphviz PATH
    setup_graphviz_path()
    
    # Load environment and configuration
    load_environment()
    config = get_database_config()
    
    # Generate diagrams
    results = visualize_databases(
        config=config,
        output_dir=args.output,
        formats=args.formats,
        exclude_tables=args.exclude
    )
    
    # Print summary
    print_summary(results)
    
    # Check if any diagrams were generated successfully
    success_count = sum(
        1 for db_results in results.values() 
        for result in db_results.values() 
        if result["success"]
    )
    
    if success_count > 0:
        print_status(f"Successfully generated {success_count} diagrams!", "success")
        print_status("Check the output directory for your ERD files", "info")
    else:
        print_status("No diagrams were generated successfully", "error")
        sys.exit(1)


if __name__ == "__main__":
    main()