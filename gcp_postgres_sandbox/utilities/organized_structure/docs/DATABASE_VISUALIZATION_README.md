# Database Schema Visualization Tools for CryptoPrism-DB

This directory contains tools for analyzing and visualizing the PostgreSQL database schemas used in the CryptoPrism-DB project.

## Available Tools

### 1. Simple Database Analyzer (Recommended)
**File:** `simple_database_analyzer.py`

A robust, dependency-light tool that generates comprehensive text-based schema analysis reports.

#### Features
- ✅ No external dependencies beyond SQLAlchemy and psycopg2 (already in project)
- ✅ Works on all platforms (Windows, Linux, macOS)
- ✅ Detailed table relationship analysis
- ✅ Column type and constraint information
- ✅ Foreign key relationship mapping
- ✅ Table grouping by prefixes
- ✅ Compatible with existing environment configuration

#### Usage
```bash
# Basic analysis of both databases
python gcp_postgres_sandbox/simple_database_analyzer.py

# Exclude specific table patterns
python gcp_postgres_sandbox/simple_database_analyzer.py --exclude temp audit system

# Custom output directory
python gcp_postgres_sandbox/simple_database_analyzer.py --output ./my_analysis
```

#### Output
Generates comprehensive text reports in the `database_analysis/` directory:
- `cryptoprism_main_schema_YYYYMMDD_HHMMSS.txt` - Main database analysis
- `cryptoprism_backtest_schema_YYYYMMDD_HHMMSS.txt` - Backtest database analysis

### 2. Visual Database Visualizer (Advanced)
**File:** `database_visualizer.py`

A more advanced tool that can generate visual ERD diagrams, but requires additional system dependencies.

#### Features
- 🎨 Visual ERD diagram generation (PNG, PDF, SVG)
- 📊 Multiple output formats
- 🔧 Table filtering capabilities
- 🏗️ Schema selection options

#### Requirements
- Graphviz system binaries must be installed
- Windows users: Download from https://graphviz.org/download/
- Linux: `sudo apt-get install graphviz` or equivalent
- macOS: `brew install graphviz`

#### Usage
```bash
# Generate all formats
python gcp_postgres_sandbox/database_visualizer.py

# PNG only
python gcp_postgres_sandbox/database_visualizer.py --formats png

# Exclude system tables
python gcp_postgres_sandbox/database_visualizer.py --exclude temp audit pg_stat
```

## Current Database Analysis Results

Based on the latest analysis of your CryptoPrism-DB:

### Main Database (`dbcp`)
- **Total Tables:** 24
- **Table Groups:**
  - **FE_**: 15 tables (Feature Engineering data)
  - **crypto_**: 4 tables (Cryptocurrency listings and data)
  - **1K_**: 2 tables (OHLCV data for 1000 coins)
  - **NEWS_**: 2 tables (News and airdrop data)
  - **108_**: 1 table (Historical OHLCV data)

### Backtest Database (`cp_backtest`)
- **Total Tables:** 13
- **Similar structure** but smaller dataset for backtesting

### Key Findings
- **Well-organized schema** with clear table prefixes
- **FE_ tables** contain feature engineering and technical indicators
- **No foreign key relationships** detected (likely using application-level relationships)
- **Clean structure** with no system table pollution

## Environment Configuration

Both tools use the same environment variable configuration as other CryptoPrism-DB scripts:

```bash
DB_HOST=your_host
DB_NAME=your_main_db
DB_NAME_BT=your_backtest_db
DB_USER=your_user
DB_PASSWORD=your_password
DB_PORT=5432
```

## Integration with GitHub Actions

You can add database schema analysis to your CI/CD pipeline by creating a workflow:

```yaml
name: Database Schema Analysis
on:
  workflow_dispatch: {}
  
jobs:
  analyze:
    runs-on: ubuntu-latest
    environment: testsecrets
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Analyze database schema
        env:
          DB_HOST: ${{ secrets.DB_HOST }}
          DB_NAME: ${{ secrets.DB_NAME }}
          DB_NAME_BT: ${{ secrets.DB_NAME_BT }}
          DB_USER: ${{ secrets.DB_USER }}
          DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
          DB_PORT: ${{ secrets.DB_PORT }}
        run: python gcp_postgres_sandbox/simple_database_analyzer.py
```

## Troubleshooting

### Common Issues

1. **Visual ERD Generation Fails**
   - **Cause:** Graphviz system binaries not installed
   - **Solution:** Use the Simple Database Analyzer instead, or install Graphviz

2. **Connection Errors**
   - **Cause:** Environment variables not set
   - **Solution:** Ensure `.env` file exists with correct database credentials

3. **Unicode Errors on Windows**
   - **Cause:** Windows console encoding issues
   - **Solution:** Both tools have been tested and fixed for Windows compatibility

### Getting Help

If you encounter issues:
1. Check that your environment variables are properly configured
2. Test database connectivity with `python gcp_postgres_sandbox/test_scrtipts/funtional_tests/env_test.py`
3. Use the Simple Database Analyzer as it has fewer dependencies

## Example Analysis Output

The Simple Database Analyzer generates reports like this:

```
================================================================================
DATABASE SCHEMA ANALYSIS REPORT
Database: dbcp
Generated: 2025-09-01 18:24:23
================================================================================

SUMMARY
----------------------------------------
Total Tables: 24
Analyzed Tables: 24
Total Relationships: 0
Table Groups: 5

TABLE GROUPS (by prefix)
----------------------------------------
FE: 15 tables
  - FE_CC_INFO_URL
  - FE_DMV_ALL
  - FE_DMV_SCORES
  [... more tables ...]

FOREIGN KEY RELATIONSHIPS
----------------------------------------
[Relationship analysis...]

DETAILED TABLE ANALYSIS
----------------------------------------
[Column-by-column analysis for each table...]
```

This provides comprehensive insights into your database structure without requiring external dependencies.