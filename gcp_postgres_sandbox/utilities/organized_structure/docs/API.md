# CryptoPrism Database Utilities - API Documentation

This document provides detailed API documentation for the CryptoPrism Database Utilities package.

## Core Modules

### crypto_db_utils.core.db_connection

The `DatabaseConnection` class provides centralized database connection management.

#### DatabaseConnection

```python
from crypto_db_utils.core.db_connection import DatabaseConnection

db_conn = DatabaseConnection()
```

**Methods:**

- `get_engine(database='main', echo=False)`: Get SQLAlchemy engine
- `test_connection(database='main')`: Test database connectivity
- `get_database_info(database='main')`: Get database version and info
- `close_all_connections()`: Close all cached connections
- `list_available_databases()`: Get database alias mappings

**Example:**
```python
# Test connection
success = db_conn.test_connection('main')
print(f"Connection {'successful' if success else 'failed'}")

# Get database info
info = db_conn.get_database_info('main')
print(f"Connected to: {info['database']} on {info['host']}")

# Get engine for queries
engine = db_conn.get_engine('main')
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM information_schema.tables"))
```

### crypto_db_utils.core.base_analyzer

The `BaseAnalyzer` class provides common functionality for analysis tools.

#### BaseAnalyzer

```python
from crypto_db_utils.core.base_analyzer import BaseAnalyzer

class MyAnalyzer(BaseAnalyzer):
    def run_analysis(self):
        # Implement analysis logic
        return {"results": "data"}

analyzer = MyAnalyzer(output_dir="./output")
results = analyzer.execute()
```

**Methods:**

- `execute()`: Run complete analysis workflow
- `save_json_results(data, filename)`: Save results to JSON
- `save_text_results(content, filename)`: Save text output
- `test_database_connections()`: Test all configured databases
- `get_analysis_metadata()`: Get analysis metadata
- `progress_callback(current, total, item_name)`: Progress reporting
- `cleanup()`: Clean up resources

## Analysis Tools

### crypto_db_utils.analysis.schema_analyzer

Provides comprehensive schema analysis capabilities.

#### Usage Example:
```python
from crypto_db_utils.analysis.schema_analyzer import SchemaAnalyzer

analyzer = SchemaAnalyzer(
    output_dir="./analysis_reports",
    databases=['main', 'backtest']
)

results = analyzer.execute()
print(f"Analysis completed. Results saved to: {analyzer.output_dir}")
```

### crypto_db_utils.analysis.schema_extractor

Extracts database schema to structured JSON format.

#### Usage Example:
```python
from crypto_db_utils.analysis.schema_extractor import SchemaExtractor

extractor = SchemaExtractor()
schema_data = extractor.extract_complete_schema('main')

# Save to file
extractor.save_schema_json(schema_data, 'schema_main.json')
```

## Benchmarking Tools

### crypto_db_utils.benchmarking.query_benchmarker

Comprehensive query performance testing.

#### QueryBenchmarker

```python
from crypto_db_utils.benchmarking.query_benchmarker import QueryBenchmarker

benchmarker = QueryBenchmarker()
results = benchmarker.run_comprehensive_benchmark(
    database='main',
    iterations=5
)

print(f"Average query time: {results['summary']['avg_execution_time_ms']:.2f}ms")
```

**Methods:**

- `run_comprehensive_benchmark(database, iterations=5)`: Full benchmark suite
- `benchmark_single_query(sql, database, iterations=3)`: Single query benchmark
- `benchmark_join_queries(database, iterations=3)`: JOIN performance tests
- `benchmark_aggregation_queries(database, iterations=3)`: Aggregation tests

### crypto_db_utils.benchmarking.performance_analyzer

Compares performance before and after optimizations.

#### PerformanceAnalyzer

```python
from crypto_db_utils.benchmarking.performance_analyzer import PerformanceAnalyzer

analyzer = PerformanceAnalyzer()
comparison = analyzer.compare_benchmark_results(
    before_file='benchmark_before.json',
    after_file='benchmark_after.json'
)

print(f"Average improvement: {comparison['summary']['avg_improvement_percent']:.1f}%")
```

## Optimization Tools

### crypto_db_utils.optimization.complete_optimizer

Full database optimization suite.

#### CompleteOptimizer

```python
from crypto_db_utils.optimization.complete_optimizer import CompleteOptimizer

optimizer = CompleteOptimizer()
optimization_plan = optimizer.analyze_optimization_opportunities('main')

# Execute optimizations (use dry_run=True to preview)
results = optimizer.execute_optimizations(
    database='main',
    dry_run=False
)
```

**Methods:**

- `analyze_optimization_opportunities(database)`: Identify optimization targets
- `execute_optimizations(database, dry_run=False)`: Apply optimizations
- `generate_rollback_script(database)`: Create rollback SQL
- `validate_optimizations(database)`: Check optimization success

### crypto_db_utils.optimization.optimization_generator

Generate optimization recommendations and SQL scripts.

#### OptimizationGenerator

```python
from crypto_db_utils.optimization.optimization_generator import OptimizationGenerator

generator = OptimizationGenerator()
recommendations = generator.generate_optimization_plan('main')

for rec in recommendations['primary_keys']:
    print(f"Add PK to {rec['table']}: {rec['sql']}")
```

## Indexing Tools

### crypto_db_utils.indexing.index_builder

Strategic database index creation.

#### IndexBuilder

```python
from crypto_db_utils.indexing.index_builder import IndexBuilder

builder = IndexBuilder()
index_plan = builder.analyze_index_opportunities(
    database='main',
    strategy='strategic'
)

# Create indexes
results = builder.create_strategic_indexes(
    database='main',
    dry_run=False
)
```

**Methods:**

- `analyze_index_opportunities(database, strategy)`: Identify index opportunities
- `create_strategic_indexes(database, dry_run=False)`: Create recommended indexes
- `validate_indexes(database)`: Check index effectiveness
- `generate_index_rollback(database)`: Create rollback script

## Validation Tools

### crypto_db_utils.validation.column_validator

Column validation and quality checks.

#### ColumnValidator

```python
from crypto_db_utils.validation.column_validator import ColumnValidator

validator = ColumnValidator()
validation_results = validator.validate_database_columns('main')

print(f"Validation issues found: {len(validation_results['issues'])}")
for issue in validation_results['issues']:
    print(f"- {issue['severity']}: {issue['description']}")
```

### crypto_db_utils.validation.performance_comparator

Compare performance metrics between different states.

#### PerformanceComparator

```python
from crypto_db_utils.validation.performance_comparator import PerformanceComparator

comparator = PerformanceComparator()
comparison = comparator.compare_performance_files(
    before_file='benchmark_before.json',
    after_file='benchmark_after.json'
)

# Generate comparison report
report = comparator.generate_comparison_report(comparison)
print(report)
```

## Configuration Management

### crypto_db_utils.config.database_configs

Database configuration management.

#### DatabaseConfig and DatabaseConfigManager

```python
from crypto_db_utils.config.database_configs import DatabaseConfig, DatabaseConfigManager

# Create custom config
config = DatabaseConfig(
    host='localhost',
    port='5432',
    user='myuser',
    password='mypass',
    database='mydb'
)

# Use config manager
config_manager = DatabaseConfigManager()
available_dbs = config_manager.list_configs()
connection_string = config_manager.get_connection_string('main')
```

## Error Handling

All tools implement comprehensive error handling:

```python
from crypto_db_utils.analysis.schema_analyzer import SchemaAnalyzer
from sqlalchemy.exc import SQLAlchemyError

try:
    analyzer = SchemaAnalyzer()
    results = analyzer.execute()
except ConnectionError as e:
    print(f"Database connection failed: {e}")
except SQLAlchemyError as e:
    print(f"Database error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Custom Analyzers

Create custom analysis tools by inheriting from `BaseAnalyzer`:

```python
from crypto_db_utils.core.base_analyzer import BaseAnalyzer
from sqlalchemy import text

class CustomTableAnalyzer(BaseAnalyzer):
    def __init__(self, table_name, **kwargs):
        super().__init__(**kwargs)
        self.table_name = table_name
    
    def run_analysis(self):
        """Implement custom analysis logic."""
        results = {}
        
        for database in self.databases:
            engine = self.db_conn.get_engine(database)
            
            with engine.connect() as conn:
                # Get table statistics
                count_query = text(f"SELECT COUNT(*) FROM {self.table_name}")
                row_count = conn.execute(count_query).scalar()
                
                # Get column info
                columns_query = text("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = :table_name
                """)
                columns = conn.execute(columns_query, {"table_name": self.table_name}).fetchall()
                
                results[database] = {
                    'table': self.table_name,
                    'row_count': row_count,
                    'columns': [dict(col) for col in columns]
                }
        
        # Save results
        output_file = self.get_output_filename(f'custom_analysis_{self.table_name}', extension='json')
        self.save_json_results(results, output_file)
        
        return results

# Usage
analyzer = CustomTableAnalyzer(
    table_name='FE_DMV_ALL',
    output_dir='./custom_reports',
    databases=['main']
)
results = analyzer.execute()
```

## Performance Best Practices

### Connection Management
```python
# Use connection pooling for multiple operations
db_conn = DatabaseConnection()
engine = db_conn.get_engine('main')

# Always use connection contexts
with engine.connect() as conn:
    # Perform database operations
    result = conn.execute(text("SELECT * FROM my_table LIMIT 10"))

# Clean up connections when done
db_conn.close_all_connections()
```

### Large Dataset Handling
```python
# Use chunked processing for large datasets
def process_large_table(table_name, chunk_size=1000):
    engine = db_conn.get_engine('main')
    
    with engine.connect() as conn:
        offset = 0
        while True:
            query = text(f"""
                SELECT * FROM {table_name}
                ORDER BY id LIMIT :limit OFFSET :offset
            """)
            
            result = conn.execute(query, {
                "limit": chunk_size,
                "offset": offset
            })
            
            rows = result.fetchall()
            if not rows:
                break
                
            # Process chunk
            yield rows
            offset += chunk_size
```

### Memory Management
```python
# Use generators for memory efficiency
def analyze_tables_efficiently(database='main'):
    analyzer = SchemaAnalyzer(databases=[database])
    
    for table_info in analyzer.get_table_iterator():
        # Process one table at a time
        yield analyzer.analyze_single_table(table_info)
```

This API documentation provides comprehensive examples for using all major components of the CryptoPrism Database Utilities package. For additional examples and advanced usage patterns, see the `examples/` directory in the repository.