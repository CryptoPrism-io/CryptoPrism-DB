# CryptoPrism Database Utilities

A comprehensive suite of PostgreSQL database analysis, benchmarking, and optimization tools specifically designed for cryptocurrency trading systems.

## 🚀 Features

- **Schema Analysis**: Comprehensive database structure analysis and reporting
- **Performance Benchmarking**: Query and table performance testing with detailed metrics
- **Database Optimization**: Automated indexing and performance improvements
- **Data Validation**: Integrity checks and quality assurance tools
- **Visualization**: Generate ERD diagrams and performance charts
- **Unified CLI**: Easy-to-use command-line interface for all tools

## 📦 Installation

### Option 1: Install from PyPI (Recommended)
```bash
pip install cryptoprism-db-utils
```

### Option 2: Install from Source
```bash
git clone https://github.com/CryptoPrism-io/CryptoPrism-DB-Utils.git
cd CryptoPrism-DB-Utils
pip install -e .
```

### Option 3: Development Installation
```bash
git clone https://github.com/CryptoPrism-io/CryptoPrism-DB-Utils.git
cd CryptoPrism-DB-Utils
pip install -e ".[dev]"
```

## 🔧 Configuration

1. Copy the environment template:
   ```bash
   cp .env.example .env
   ```

2. Update `.env` with your database credentials:
   ```bash
   # Primary Database (Required)
   DB_HOST=your_postgresql_host
   DB_PORT=5432
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_NAME=dbcp
   
   # Optional: Additional Databases
   DB_NAME_AI=cp_ai
   DB_NAME_BT=cp_backtest
   DB_NAME_BTH=cp_backtest_h
   ```

## 🎯 Quick Start

### Using the CLI
```bash
# Test database connections
cryptoprism-db utils test

# Analyze database schema
cryptoprism-db analyze schema --database main --format text

# Run performance benchmarks
cryptoprism-db benchmark queries --database main --iterations 5

# Optimize database indexes
cryptoprism-db optimize indexes --database main --strategy strategic

# Generate database ERD
cryptoprism-db visualize erd --database main --format png

# Get help for any command
cryptoprism-db --help
cryptoprism-db analyze --help
```

### Using the Python API
```python
from crypto_db_utils import DatabaseConnection, BaseAnalyzer
from crypto_db_utils.analysis.schema_analyzer import SchemaAnalyzer

# Test database connection
db_conn = DatabaseConnection()
print(db_conn.test_connection('main'))

# Analyze database schema
analyzer = SchemaAnalyzer(output_dir='./reports')
results = analyzer.execute()
```

## 📚 Available Tools

### 🔍 Analysis Tools (`crypto_db_utils.analysis`)
- **SchemaAnalyzer**: Comprehensive text-based schema analysis
- **SchemaExtractor**: JSON schema extraction for optimization planning
- **ColumnInspector**: Detailed column analysis and validation
- **DatabaseVisualizer**: Visual ERD generation with Graphviz
- **QuickAnalyzer**: Fast database overview and statistics

### ⚡ Benchmarking Tools (`crypto_db_utils.benchmarking`)
- **QueryBenchmarker**: Comprehensive query performance testing
- **SimpleBenchmarker**: Basic benchmarking functionality
- **FullDatabaseSpeedTest**: Complete database performance testing
- **SingleQueryTest**: Individual query performance analysis
- **SingleTableTest**: Table-specific performance metrics
- **PerformanceAnalyzer**: Before/after optimization comparison

### 🎯 Optimization Tools (`crypto_db_utils.optimization`)
- **CompleteOptimizer**: Full database optimization suite
- **SimpleOptimizer**: Basic optimization execution
- **CoreOptimization**: Core optimization logic
- **StepByStepOptimizer**: Guided optimization process
- **OptimizationGenerator**: Comprehensive optimization planning
- **Orchestrator**: Manages complex optimization workflows
- **Executor**: Optimization execution engine

### 🏗️ Indexing Tools (`crypto_db_utils.indexing`)
- **IndexBuilder**: Strategic index creation
- **StrategicIndexes**: Index addition utilities
- **PrimaryKeyChecker**: Primary key validation and creation

### ✅ Validation Tools (`crypto_db_utils.validation`)
- **ColumnValidator**: Column validation utilities
- **TableValidator**: Table naming and structure validation
- **SchemaTester**: Schema validation testing
- **PerformanceComparator**: Performance comparison reports

## 🎨 CLI Commands Reference

### Analysis Commands
```bash
# Schema analysis
cryptoprism-db analyze schema [--database main] [--format text|json] [--exclude pattern1 pattern2]

# Quick overview
cryptoprism-db analyze quick [--database main]

# Column analysis
cryptoprism-db analyze columns [--database main] [--table specific_table]
```

### Benchmarking Commands
```bash
# Query benchmarks
cryptoprism-db benchmark queries [--database main] [--iterations 5] [--suite standard|joins|aggregations]

# Table benchmarks
cryptoprism-db benchmark table --table table_name [--database main]

# Full database speed test
cryptoprism-db benchmark full [--database main]
```

### Optimization Commands
```bash
# Index optimization
cryptoprism-db optimize indexes [--database main] [--strategy strategic|comprehensive] [--dry-run]

# Primary key optimization
cryptoprism-db optimize primary-keys [--database main] [--dry-run]

# Complete optimization
cryptoprism-db optimize complete [--database main] [--dry-run]
```

### Validation Commands
```bash
# Data integrity checks
cryptoprism-db validate integrity [--database main]

# Schema validation
cryptoprism-db validate schema [--database main]

# Performance comparison
cryptoprism-db validate performance --before before.json --after after.json
```

### Visualization Commands
```bash
# Generate ERD
cryptoprism-db visualize erd [--database main] [--format png|pdf|svg] [--exclude pattern1]
```

### Utility Commands
```bash
# Test connections
cryptoprism-db utils test [--database specific_db]

# List databases
cryptoprism-db utils list

# Environment info
cryptoprism-db utils env
```

## 📁 Output Structure

The tools generate organized output in the following structure:
```
output/
├── analysis_reports/     # Schema and analysis reports
├── benchmark_results/    # Performance benchmark data
├── sql_optimizations/    # Generated SQL optimization scripts
└── visualizations/       # ERD diagrams and charts
```

## 🔗 Integration with CryptoPrism-DB

This utilities package is designed to work seamlessly with the main CryptoPrism-DB system:

- **Database Compatibility**: Supports the three-database architecture (dbcp, cp_ai, cp_backtest)
- **Environment Consistency**: Uses the same environment variables as main system
- **Table Recognition**: Understands FE_* signal tables and crypto_* data tables
- **Performance Focus**: Optimized for analytical workloads common in crypto analysis

## 🛠️ Development

### Setting up Development Environment
```bash
git clone https://github.com/CryptoPrism-io/CryptoPrism-DB-Utils.git
cd CryptoPrism-DB-Utils
pip install -e ".[dev]"
pre-commit install
```

### Running Tests
```bash
pytest tests/ --cov=crypto_db_utils
```

### Code Formatting
```bash
black src/ tests/
flake8 src/ tests/
mypy src/
```

## 📋 Requirements

### Core Dependencies
- Python 3.8+
- SQLAlchemy >= 2.0.32
- psycopg2-binary >= 2.9.0
- pandas >= 2.2.2
- python-dotenv >= 1.0.1

### Optional Dependencies
- **Visualization**: matplotlib, seaborn, graphviz, sqlalchemy-schemadisplay
- **AI Features**: google-generativeai
- **Notifications**: python-telegram-bot
- **MySQL Support**: mysql-connector-python

## 🚨 Important Notes

### Security Considerations
- Never commit database credentials to version control
- Use environment variables or secure credential management
- Regularly rotate database passwords
- Limit database user permissions to minimum required

### Performance Considerations
- Run optimization tools during low-traffic periods
- Always test optimizations in development first
- Monitor database performance after applying optimizations
- Keep backups before making structural changes

### Production Usage
- Use `--dry-run` flag to preview changes before applying
- Schedule regular performance benchmarks
- Monitor optimization impact with before/after comparisons
- Implement rollback procedures for optimization changes

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes and add tests
4. Run the test suite: `pytest`
5. Format your code: `black . && flake8`
6. Commit your changes: `git commit -m 'Add amazing feature'`
7. Push to the branch: `git push origin feature/amazing-feature`
8. Open a Pull Request

## 📄 License

This project is proprietary software owned by CryptoPrism.io. All rights reserved.

## 🆘 Support

- **Documentation**: See `docs/` directory for detailed API documentation
- **Issues**: Report bugs and feature requests on GitHub
- **Contact**: dev@cryptoprism.io

## 🔄 Changelog

See [CHANGELOG.md](CHANGELOG.md) for detailed version history and updates.

---

**CryptoPrism Database Utilities** - Empowering cryptocurrency analysis through intelligent database optimization.