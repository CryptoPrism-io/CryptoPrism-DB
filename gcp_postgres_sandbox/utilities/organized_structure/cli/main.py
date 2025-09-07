#!/usr/bin/env python3
"""
CryptoPrism Database Utilities - Unified Command Line Interface

This module provides a unified CLI for accessing all database analysis, benchmarking,
and optimization tools in the CryptoPrism Database Utilities suite.

Usage:
    cryptoprism-db <command> [options]
    cpdb <command> [options]  # Short alias

Available Commands:
    analyze     - Schema analysis and reporting
    benchmark   - Performance benchmarking and testing
    optimize    - Database optimization and indexing
    validate    - Data validation and quality checks
    visualize   - Generate database schema diagrams
    
Example:
    cryptoprism-db analyze --database main --output ./reports
    cpdb benchmark --query-suite standard --iterations 5
    cpdb optimize --strategy indexes --dry-run
"""

import argparse
import sys
import logging
from pathlib import Path
from typing import List, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from crypto_db_utils.core.db_connection import DatabaseConnection
from crypto_db_utils.core.base_analyzer import BaseAnalyzer


def setup_logging(log_level: str) -> None:
    """Setup logging configuration."""
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def create_main_parser() -> argparse.ArgumentParser:
    """Create the main argument parser."""
    parser = argparse.ArgumentParser(
        prog='cryptoprism-db',
        description='CryptoPrism Database Utilities - Comprehensive PostgreSQL analysis toolkit',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  cryptoprism-db analyze schema --database main
  cryptoprism-db benchmark queries --iterations 10
  cryptoprism-db optimize indexes --dry-run
  cryptoprism-db validate data --check-integrity
  cryptoprism-db visualize erd --format png

For more information on each command, use:
  cryptoprism-db <command> --help
        """
    )
    
    # Global options
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='./output',
        help='Output directory for reports and files (default: ./output)'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='CryptoPrism Database Utilities v1.0.0'
    )
    
    # Add subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Analysis commands
    add_analyze_parser(subparsers)
    
    # Benchmarking commands
    add_benchmark_parser(subparsers)
    
    # Optimization commands
    add_optimize_parser(subparsers)
    
    # Validation commands
    add_validate_parser(subparsers)
    
    # Visualization commands
    add_visualize_parser(subparsers)
    
    # Utility commands
    add_utility_parser(subparsers)
    
    return parser


def add_analyze_parser(subparsers) -> None:
    """Add analysis command parser."""
    analyze_parser = subparsers.add_parser(
        'analyze',
        help='Database schema analysis and reporting',
        description='Perform comprehensive database schema analysis'
    )
    
    analyze_subparsers = analyze_parser.add_subparsers(dest='analyze_command')
    
    # Schema analysis
    schema_parser = analyze_subparsers.add_parser('schema', help='Analyze database schema')
    schema_parser.add_argument('--database', default='main', help='Database to analyze')
    schema_parser.add_argument('--exclude', nargs='*', help='Table patterns to exclude')
    schema_parser.add_argument('--format', choices=['text', 'json'], default='text')
    
    # Quick analysis
    quick_parser = analyze_subparsers.add_parser('quick', help='Quick database overview')
    quick_parser.add_argument('--database', default='main', help='Database to analyze')
    
    # Column analysis
    columns_parser = analyze_subparsers.add_parser('columns', help='Detailed column analysis')
    columns_parser.add_argument('--database', default='main', help='Database to analyze')
    columns_parser.add_argument('--table', help='Specific table to analyze')


def add_benchmark_parser(subparsers) -> None:
    """Add benchmarking command parser."""
    benchmark_parser = subparsers.add_parser(
        'benchmark',
        help='Performance benchmarking and testing',
        description='Run performance benchmarks on database queries'
    )
    
    benchmark_subparsers = benchmark_parser.add_subparsers(dest='benchmark_command')
    
    # Query benchmarking
    queries_parser = benchmark_subparsers.add_parser('queries', help='Benchmark database queries')
    queries_parser.add_argument('--database', default='main', help='Database to benchmark')
    queries_parser.add_argument('--iterations', type=int, default=5, help='Number of test iterations')
    queries_parser.add_argument('--suite', choices=['standard', 'joins', 'aggregations'], default='standard')
    
    # Table benchmarking
    table_parser = benchmark_subparsers.add_parser('table', help='Benchmark specific table')
    table_parser.add_argument('--database', default='main', help='Database to benchmark')
    table_parser.add_argument('--table', required=True, help='Table name to benchmark')
    
    # Full database speed test
    full_parser = benchmark_subparsers.add_parser('full', help='Comprehensive database speed test')
    full_parser.add_argument('--database', default='main', help='Database to test')


def add_optimize_parser(subparsers) -> None:
    """Add optimization command parser."""
    optimize_parser = subparsers.add_parser(
        'optimize',
        help='Database optimization and indexing',
        description='Optimize database performance through indexing and structure improvements'
    )
    
    optimize_subparsers = optimize_parser.add_subparsers(dest='optimize_command')
    
    # Index optimization
    indexes_parser = optimize_subparsers.add_parser('indexes', help='Optimize database indexes')
    indexes_parser.add_argument('--database', default='main', help='Database to optimize')
    indexes_parser.add_argument('--strategy', choices=['strategic', 'comprehensive'], default='strategic')
    indexes_parser.add_argument('--dry-run', action='store_true', help='Show what would be done without executing')
    
    # Primary key optimization
    pk_parser = optimize_subparsers.add_parser('primary-keys', help='Add missing primary keys')
    pk_parser.add_argument('--database', default='main', help='Database to optimize')
    pk_parser.add_argument('--dry-run', action='store_true', help='Show what would be done without executing')
    
    # Complete optimization
    complete_parser = optimize_subparsers.add_parser('complete', help='Complete database optimization')
    complete_parser.add_argument('--database', default='main', help='Database to optimize')
    complete_parser.add_argument('--dry-run', action='store_true', help='Show what would be done without executing')


def add_validate_parser(subparsers) -> None:
    """Add validation command parser."""
    validate_parser = subparsers.add_parser(
        'validate',
        help='Data validation and quality checks',
        description='Validate database structure and data integrity'
    )
    
    validate_subparsers = validate_parser.add_subparsers(dest='validate_command')
    
    # Data integrity
    integrity_parser = validate_subparsers.add_parser('integrity', help='Check data integrity')
    integrity_parser.add_argument('--database', default='main', help='Database to validate')
    
    # Schema validation
    schema_parser = validate_subparsers.add_parser('schema', help='Validate schema structure')
    schema_parser.add_argument('--database', default='main', help='Database to validate')
    
    # Performance comparison
    perf_parser = validate_subparsers.add_parser('performance', help='Compare performance metrics')
    perf_parser.add_argument('--before', required=True, help='Before optimization benchmark file')
    perf_parser.add_argument('--after', required=True, help='After optimization benchmark file')


def add_visualize_parser(subparsers) -> None:
    """Add visualization command parser."""
    visualize_parser = subparsers.add_parser(
        'visualize',
        help='Generate database schema diagrams',
        description='Create visual representations of database structure'
    )
    
    visualize_subparsers = visualize_parser.add_subparsers(dest='visualize_command')
    
    # ERD generation
    erd_parser = visualize_subparsers.add_parser('erd', help='Generate Entity Relationship Diagram')
    erd_parser.add_argument('--database', default='main', help='Database to visualize')
    erd_parser.add_argument('--format', choices=['png', 'pdf', 'svg'], default='png')
    erd_parser.add_argument('--exclude', nargs='*', help='Table patterns to exclude')


def add_utility_parser(subparsers) -> None:
    """Add utility command parser."""
    utility_parser = subparsers.add_parser(
        'utils',
        help='Utility commands',
        description='Utility functions for database management'
    )
    
    utility_subparsers = utility_parser.add_subparsers(dest='utility_command')
    
    # Test connections
    test_parser = utility_subparsers.add_parser('test', help='Test database connections')
    test_parser.add_argument('--database', help='Specific database to test (default: all)')
    
    # List databases
    list_parser = utility_subparsers.add_parser('list', help='List available databases')
    
    # Environment info
    env_parser = utility_subparsers.add_parser('env', help='Show environment configuration')


def execute_analyze_command(args) -> int:
    """Execute analysis commands."""
    if args.analyze_command == 'schema':
        print(f"Analyzing schema for database: {args.database}")
        print(f"Output format: {args.format}")
        if args.exclude:
            print(f"Excluding patterns: {', '.join(args.exclude)}")
        
        # TODO: Import and execute schema analyzer
        from crypto_db_utils.analysis.schema_analyzer import SimpleAnalyzer
        analyzer = SimpleAnalyzer(output_dir=args.output_dir)
        results = analyzer.execute()
        return 0
        
    elif args.analyze_command == 'quick':
        print(f"Quick analysis of database: {args.database}")
        # TODO: Import and execute quick analyzer
        return 0
        
    elif args.analyze_command == 'columns':
        print(f"Column analysis for database: {args.database}")
        if args.table:
            print(f"Focusing on table: {args.table}")
        # TODO: Import and execute column inspector
        return 0
    
    print("Please specify an analysis command. Use --help for options.")
    return 1


def execute_benchmark_command(args) -> int:
    """Execute benchmarking commands."""
    if args.benchmark_command == 'queries':
        print(f"Benchmarking queries on database: {args.database}")
        print(f"Test suite: {args.suite}, Iterations: {args.iterations}")
        # TODO: Import and execute query benchmarker
        return 0
        
    elif args.benchmark_command == 'table':
        print(f"Benchmarking table '{args.table}' on database: {args.database}")
        # TODO: Import and execute table benchmarker
        return 0
        
    elif args.benchmark_command == 'full':
        print(f"Full database speed test for: {args.database}")
        # TODO: Import and execute full database speed test
        return 0
    
    print("Please specify a benchmark command. Use --help for options.")
    return 1


def execute_optimize_command(args) -> int:
    """Execute optimization commands."""
    if args.optimize_command == 'indexes':
        action = "Would optimize" if args.dry_run else "Optimizing"
        print(f"{action} indexes for database: {args.database}")
        print(f"Strategy: {args.strategy}")
        # TODO: Import and execute index optimizer
        return 0
        
    elif args.optimize_command == 'primary-keys':
        action = "Would add" if args.dry_run else "Adding"
        print(f"{action} missing primary keys for database: {args.database}")
        # TODO: Import and execute primary key optimizer
        return 0
        
    elif args.optimize_command == 'complete':
        action = "Would perform" if args.dry_run else "Performing"
        print(f"{action} complete optimization for database: {args.database}")
        # TODO: Import and execute complete optimizer
        return 0
    
    print("Please specify an optimization command. Use --help for options.")
    return 1


def execute_validate_command(args) -> int:
    """Execute validation commands."""
    if args.validate_command == 'integrity':
        print(f"Checking data integrity for database: {args.database}")
        # TODO: Import and execute integrity validator
        return 0
        
    elif args.validate_command == 'schema':
        print(f"Validating schema structure for database: {args.database}")
        # TODO: Import and execute schema validator
        return 0
        
    elif args.validate_command == 'performance':
        print(f"Comparing performance: {args.before} vs {args.after}")
        # TODO: Import and execute performance comparator
        return 0
    
    print("Please specify a validation command. Use --help for options.")
    return 1


def execute_visualize_command(args) -> int:
    """Execute visualization commands."""
    if args.visualize_command == 'erd':
        print(f"Generating ERD for database: {args.database}")
        print(f"Format: {args.format}")
        if args.exclude:
            print(f"Excluding patterns: {', '.join(args.exclude)}")
        # TODO: Import and execute database visualizer
        return 0
    
    print("Please specify a visualization command. Use --help for options.")
    return 1


def execute_utility_command(args) -> int:
    """Execute utility commands."""
    if args.utility_command == 'test':
        print("Testing database connections...")
        db_conn = DatabaseConnection()
        
        if args.database:
            databases = [args.database]
        else:
            databases = list(db_conn.list_available_databases().keys())
        
        for database in databases:
            try:
                success = db_conn.test_connection(database)
                status = "✓ Connected" if success else "✗ Failed"
                print(f"  {database}: {status}")
            except Exception as e:
                print(f"  {database}: ✗ Error - {str(e)}")
        
        return 0
        
    elif args.utility_command == 'list':
        print("Available databases:")
        db_conn = DatabaseConnection()
        for alias, name in db_conn.list_available_databases().items():
            print(f"  {alias}: {name}")
        return 0
        
    elif args.utility_command == 'env':
        print("Environment Configuration:")
        db_conn = DatabaseConnection()
        from crypto_db_utils.config.database_configs import config_manager
        
        env_info = config_manager.get_environment_info()
        for key, value in env_info.items():
            print(f"  {key}: {value}")
        return 0
    
    print("Please specify a utility command. Use --help for options.")
    return 1


def main() -> int:
    """Main CLI entry point."""
    parser = create_main_parser()
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    # Handle no command
    if not args.command:
        parser.print_help()
        return 1
    
    # Create output directory
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    
    # Route to appropriate command handler
    if args.command == 'analyze':
        return execute_analyze_command(args)
    elif args.command == 'benchmark':
        return execute_benchmark_command(args)
    elif args.command == 'optimize':
        return execute_optimize_command(args)
    elif args.command == 'validate':
        return execute_validate_command(args)
    elif args.command == 'visualize':
        return execute_visualize_command(args)
    elif args.command == 'utils':
        return execute_utility_command(args)
    else:
        print(f"Unknown command: {args.command}")
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())