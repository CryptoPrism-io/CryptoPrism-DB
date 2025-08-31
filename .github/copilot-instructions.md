# Copilot Instructions for CryptoPrism-DB

## Project Overview
CryptoPrism-DB is a data engineering and analytics codebase for cryptocurrency market data. It consists of Python and R scripts for ingesting, transforming, analyzing, and backtesting crypto datasets, primarily using Google Cloud PostgreSQL as the backend. The project is organized around modular scripts for data ETL, feature engineering, and quality assurance.

## Architecture & Data Flow
- **Data Ingestion**: Scripts like `cmc_listings.py` and `gcp_108k_1kcoins.R` fetch and normalize data from CoinMarketCap and other APIs, then write to GCP PostgreSQL tables.
- **Feature Engineering**: Scripts in `gcp_postgres_sandbox/` (e.g., `gcp_dmv_core.py`, `gcp_dmv_mom.py`, `gcp_dmv_osc.py`) join, aggregate, and compute technical indicators and signals, storing results in new DB tables.
- **Backtesting & QA**: Scripts like `gcp_dmv_mom_backtest.py`, `prod_qa_cp_ai_backtest.py`, and `test_backtest_mom_data.py` run historical analyses and data quality checks, often using separate backtest databases.
- **R Scripts**: Used for data pipeline orchestration and additional analytics, mirroring some Python workflows.

## Developer Workflows
- **Environment**: Set DB/API credentials via environment variables or `.env` files. Many scripts auto-create `.env` if missing.
- **Running Scripts**: Most scripts are standalone and can be run directly (e.g., `python gcp_postgres_sandbox/gcp_dmv_core.py`). R scripts are run with `Rscript`.
- **Testing**: Run Python tests with `python -m unittest gcp_postgres_sandbox/test_backtest_mom_data.py`.
- **Database**: All data flows through GCP PostgreSQL. Table names are consistent across scripts (e.g., `crypto_listings_latest_1000`, `1K_coins_ohlcv`).

## Project-Specific Patterns
- **Database Connections**: Use SQLAlchemy for Python, RPostgres for R. Credentials are hardcoded in some scripts but should be moved to env vars for production.
- **Batch Processing**: Data is often processed in chunks (e.g., API calls in batches of 199 for CoinMarketCap limits).
- **Signal/Feature Tables**: Feature engineering scripts output to tables prefixed with `FE_` (e.g., `FE_DMV_ALL`, `FE_MOMENTUM`).
- **Logging**: Most Python scripts use the `logging` module for progress and error reporting.
- **Schema Consistency**: Scripts enforce strict schemas (e.g., 30-field schema in `cmc_listings.py`).
- **Backtest vs. Live**: Many scripts have parallel logic for live and backtest DBs (see `DB_CONFIG` usage).

## Integration Points
- **CoinMarketCap API**: Used for listings, info, and fear/greed index. API keys are required.
- **Google Cloud PostgreSQL**: Central data store for all ETL and analytics.
- **R and Python Interop**: Some workflows are available in both languages for flexibility.

## Examples
- To add a new technical indicator, follow the pattern in `gcp_dmv_osc.py` or `gcp_dmv_mom.py` (define calculation, add to DataFrame, push to DB).
- For new data sources, see `cmc_listings.py` for API fetch, normalization, and DB upload.
- For QA/backtest, see `prod_qa_cp_ai_backtest.py` for environment setup and duplicate handling.

## Key Files & Directories
- `gcp_postgres_sandbox/`: Main scripts for ETL, feature engineering, and QA.
- `knowledge_base/`: Per-script documentation.
- `.env` or environment variables: Required for DB/API credentials.
- `.github/copilot-instructions.md`: This file.

---

If you add new scripts, document their workflow and DB integration in `knowledge_base/` and update this file with new patterns as needed.
