# CryptoPrism Database Utilities - Usage Guide

This guide provides practical examples and workflows for using the CryptoPrism Database Utilities.

## Getting Started

### 1. Environment Setup

First, ensure your environment is properly configured:

```bash
# Copy environment template
cp .env.example .env

# Edit with your database credentials
nano .env
```

Example `.env` configuration:
```bash
DB_HOST=your-postgres-host.com
DB_PORT=5432
DB_USER=cryptoprism_user
DB_PASSWORD=secure_password
DB_NAME=dbcp
DB_NAME_BT=cp_backtest
```

### 2. Test Connection

Before running any analysis, test your database connections:

```bash
# Test all configured databases
cryptoprism-db utils test

# Test specific database
cryptoprism-db utils test --database main

# List available databases
cryptoprism-db utils list
```

## Common Workflows

### Workflow 1: Initial Database Analysis

When working with a new database or after schema changes:

```bash
# Step 1: Get quick overview
cryptoprism-db analyze quick --database main

# Step 2: Detailed schema analysis
cryptoprism-db analyze schema --database main --format text

# Step 3: Generate visual ERD (requires Graphviz)
cryptoprism-db visualize erd --database main --format png
```

This provides comprehensive understanding of your database structure.

### Workflow 2: Performance Baseline and Optimization

For optimizing database performance:

```bash
# Step 1: Establish performance baseline
cryptoprism-db benchmark queries --database main --iterations 10

# Step 2: Analyze optimization opportunities
cryptoprism-db optimize indexes --database main --dry-run

# Step 3: Apply optimizations
cryptoprism-db optimize indexes --database main --strategy strategic

# Step 4: Measure improvement
cryptoprism-db benchmark queries --database main --iterations 10

# Step 5: Compare results
cryptoprism-db validate performance --before baseline.json --after optimized.json
```

### Workflow 3: Data Quality Validation

For ensuring data integrity and quality:

```bash
# Step 1: Schema validation
cryptoprism-db validate schema --database main

# Step 2: Data integrity checks
cryptoprism-db validate integrity --database main

# Step 3: Column analysis
cryptoprism-db analyze columns --database main
```

## Practical Examples

### Example 1: Cryptocurrency Database Optimization

For a CryptoPrism database with signal tables:

```bash
# Analyze signal table performance
cryptoprism-db benchmark table --table FE_DMV_ALL --database main

# Focus on momentum signals
cryptoprism-db analyze columns --database main --table FE_MOMENTUM_SIGNALS

# Optimize for analytical queries
cryptoprism-db optimize complete --database main --strategy comprehensive

# Verify optimization impact
cryptoprism-db benchmark full --database main
```

### Example 2: Multi-Database Analysis

For systems with multiple databases (main, AI, backtest):

```bash
# Test all database connections
cryptoprism-db utils test

# Analyze each database separately
cryptoprism-db analyze schema --database main --format json
cryptoprism-db analyze schema --database backtest --format json

# Compare schema differences (using external tools)
# This helps identify inconsistencies between environments
```

### Example 3: Production Health Check

Regular production monitoring workflow:

```bash
#!/bin/bash
# Daily database health check script

DATE=$(date +%Y%m%d)
OUTPUT_DIR="./health_checks/$DATE"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Test connections
cryptoprism-db utils test > "$OUTPUT_DIR/connection_test.log"

# Quick analysis
cryptoprism-db analyze quick --database main --output-dir "$OUTPUT_DIR"

# Performance benchmark
cryptoprism-db benchmark queries --database main --iterations 3 --output-dir "$OUTPUT_DIR"

# Data validation
cryptoprism-db validate integrity --database main --output-dir "$OUTPUT_DIR"

echo "Health check completed. Results in $OUTPUT_DIR"
```

## Advanced Usage

### Using Python API

For programmatic access and custom workflows:

```python
#!/usr/bin/env python3
"""
Custom analysis workflow combining multiple tools.
"""

from crypto_db_utils.core.db_connection import DatabaseConnection
from crypto_db_utils.analysis.schema_analyzer import SchemaAnalyzer
from crypto_db_utils.benchmarking.query_benchmarker import QueryBenchmarker
from crypto_db_utils.optimization.optimization_generator import OptimizationGenerator

def custom_optimization_workflow(database='main'):
    """Complete optimization workflow with custom logic."""
    
    # Test connection first
    db_conn = DatabaseConnection()
    if not db_conn.test_connection(database):
        print(f"Cannot connect to database: {database}")
        return False
    
    # Step 1: Analyze current schema
    print("Analyzing database schema...")
    schema_analyzer = SchemaAnalyzer(databases=[database])
    schema_results = schema_analyzer.execute()
    
    # Step 2: Benchmark current performance
    print("Benchmarking current performance...")
    benchmarker = QueryBenchmarker()
    before_results = benchmarker.run_comprehensive_benchmark(database)
    
    # Step 3: Generate optimization recommendations
    print("Generating optimization plan...")
    optimizer = OptimizationGenerator()
    optimization_plan = optimizer.generate_optimization_plan(database)
    
    # Step 4: Apply optimizations (with confirmation)
    print(f"Found {len(optimization_plan['recommendations'])} optimization opportunities")
    
    confirm = input("Apply optimizations? (y/N): ")
    if confirm.lower() == 'y':
        optimizer.execute_optimization_plan(database, optimization_plan)
        print("Optimizations applied successfully")
        
        # Step 5: Benchmark after optimization
        print("Benchmarking optimized performance...")
        after_results = benchmarker.run_comprehensive_benchmark(database)
        
        # Calculate improvement
        improvement = calculate_improvement(before_results, after_results)
        print(f"Performance improvement: {improvement:.1f}%")
    
    return True

def calculate_improvement(before, after):
    """Calculate percentage improvement in query performance."""
    before_avg = before['summary']['avg_execution_time_ms']
    after_avg = after['summary']['avg_execution_time_ms']
    
    if before_avg > 0:
        improvement = ((before_avg - after_avg) / before_avg) * 100
        return max(0, improvement)  # Don't report negative improvements
    return 0

if __name__ == '__main__':
    custom_optimization_workflow('main')
```

### Batch Processing Multiple Tables

For analyzing many tables efficiently:

```python
#!/usr/bin/env python3
"""
Batch analyze all signal tables in the database.
"""

from crypto_db_utils.core.db_connection import DatabaseConnection
from crypto_db_utils.benchmarking.single_table_test import SingleTableTester
import re

def batch_analyze_signal_tables(database='main'):
    """Analyze all FE_* signal tables."""
    
    db_conn = DatabaseConnection()
    engine = db_conn.get_engine(database)
    
    # Get all signal tables
    with engine.connect() as conn:
        result = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'FE_%'
            ORDER BY table_name
        """)
        
        signal_tables = [row[0] for row in result.fetchall()]
    
    print(f"Found {len(signal_tables)} signal tables to analyze")
    
    # Analyze each table
    tester = SingleTableTester()
    results = {}
    
    for i, table_name in enumerate(signal_tables, 1):
        print(f"Analyzing table {i}/{len(signal_tables)}: {table_name}")
        
        try:
            table_results = tester.analyze_table(database, table_name)
            results[table_name] = table_results
            print(f"  ✓ {table_results['row_count']:,} rows, "
                  f"{table_results['column_count']} columns")
        except Exception as e:
            print(f"  ✗ Error analyzing {table_name}: {str(e)}")
            results[table_name] = {'error': str(e)}
    
    # Save consolidated results
    tester.save_json_results(results, f'signal_tables_analysis_{database}.json')
    print(f"Analysis complete. Results saved for {len(results)} tables.")
    
    return results

if __name__ == '__main__':
    batch_analyze_signal_tables('main')
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Connection Errors

**Problem**: Cannot connect to database
```
Error: Missing required environment variables: ['DB_HOST', 'DB_USER']
```

**Solution**:
```bash
# Check environment variables
cryptoprism-db utils env

# Verify .env file exists and contains required variables
cat .env

# Test specific connection
cryptoprism-db utils test --database main
```

#### 2. Permission Errors

**Problem**: Database user lacks required permissions
```
Error: permission denied for table information_schema.tables
```

**Solution**:
```sql
-- Grant necessary permissions to analysis user
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cryptoprism_user;
GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO cryptoprism_user;
```

#### 3. Memory Issues with Large Databases

**Problem**: Out of memory during analysis
```
Error: MemoryError during schema extraction
```

**Solution**:
```python
# Use chunked processing for large databases
from crypto_db_utils.analysis.schema_analyzer import SchemaAnalyzer

analyzer = SchemaAnalyzer(
    chunk_size=1000,  # Process 1000 rows at a time
    exclude_patterns=['temp_*', 'staging_*']  # Skip temporary tables
)
```

#### 4. Visualization Dependencies

**Problem**: ERD generation fails
```
Error: Graphviz executable not found in PATH
```

**Solution**:
```bash
# Install Graphviz system dependency
# Ubuntu/Debian:
sudo apt-get install graphviz

# macOS:
brew install graphviz

# Windows: Download from https://graphviz.org/download/
```

### Performance Tips

#### 1. Optimize Connection Usage
```python
# Reuse connections for multiple operations
db_conn = DatabaseConnection()
engine = db_conn.get_engine('main')

# Use transactions for related operations
with engine.begin() as conn:
    # Multiple related queries
    pass
```

#### 2. Use Appropriate Analysis Scope
```bash
# For quick checks, use lightweight commands
cryptoprism-db analyze quick

# For detailed analysis, specify scope
cryptoprism-db analyze schema --exclude temp_* staging_* audit_*
```

#### 3. Parallel Processing
```python
# For multiple databases, use concurrent processing
import concurrent.futures
from crypto_db_utils.analysis.schema_analyzer import SchemaAnalyzer

databases = ['main', 'backtest', 'ai']

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = [
        executor.submit(SchemaAnalyzer(databases=[db]).execute)
        for db in databases
    ]
    
    results = [future.result() for future in concurrent.futures.as_completed(futures)]
```

## Integration Examples

### GitHub Actions Integration

Add database health checks to your CI/CD pipeline:

```yaml
name: Database Health Check
on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 6 AM UTC
  workflow_dispatch: {}

jobs:
  health-check:
    runs-on: ubuntu-latest
    environment: production
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
          
      - name: Install utilities
        run: |
          pip install cryptoprism-db-utils
          
      - name: Run health checks
        env:
          DB_HOST: ${{ secrets.DB_HOST }}
          DB_USER: ${{ secrets.DB_USER }}
          DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
          DB_NAME: ${{ secrets.DB_NAME }}
        run: |
          # Test connections
          cryptoprism-db utils test
          
          # Quick analysis
          cryptoprism-db analyze quick --database main
          
          # Performance benchmark
          cryptoprism-db benchmark queries --database main --iterations 3
```

### Docker Integration

Run utilities in containerized environments:

```dockerfile
FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    graphviz \
    && rm -rf /var/lib/apt/lists/*

# Install utilities
RUN pip install cryptoprism-db-utils[all]

# Copy configuration
COPY .env /app/.env
WORKDIR /app

# Default command
CMD ["cryptoprism-db", "utils", "test"]
```

This usage guide provides practical examples for getting the most out of the CryptoPrism Database Utilities. For more advanced scenarios and API details, see the API documentation.