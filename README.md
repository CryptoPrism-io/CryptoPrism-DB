# CryptoPrism-DB

<div align="center">

**Production-Grade Cryptocurrency Technical Analysis System with Multi-Database Architecture**

[![Version](https://img.shields.io/badge/version-4.3.0-blue.svg)](CHANGELOG.md)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![R](https://img.shields.io/badge/R-4.0+-276DC3?logo=r&logoColor=white)](https://www.r-project.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-316192?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/license-MIT-orange.svg)](LICENSE)
[![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-Automated-2088FF?logo=github-actions&logoColor=white)](https://github.com/features/actions)

### Technology Stack

![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![R](https://img.shields.io/badge/R-276DC3?logo=r&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?logo=postgresql&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-D71F00?logo=sqlalchemy&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?logo=pandas&logoColor=white)
![NumPy](https://img.shields.io/badge/NumPy-013243?logo=numpy&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?logo=github-actions&logoColor=white)
![Docker](https://img.shields.io/badge/Docker_Ready-2496ED?logo=docker&logoColor=white)
![Google AI](https://img.shields.io/badge/Google_AI-4285F4?logo=google&logoColor=white)
![Telegram](https://img.shields.io/badge/Telegram-26A5E4?logo=telegram&logoColor=white)

*A comprehensive cryptocurrency data pipeline featuring multi-layered technical analysis, automated quality assurance, and sophisticated backtesting capabilities for 1000+ cryptocurrencies using 100+ technical indicators*

[Features](#-key-features) • [Quick Start](#-quick-start) • [Architecture](#-architecture) • [Pipeline](#-automated-pipeline) • [Documentation](#-documentation)

</div>

---

## What's New

### v4.3.0 (Current) - README Revamp
- **Modern Documentation** - Completely redesigned README with professional badges and structure
- **Technology Stack Badges** - 16 badges with official brand colors and logos
- **Quick Start Guide** - Streamlined 5-step installation process
- **Enhanced Navigation** - Better organization with emoji headers and internal links

### v4.2.2 - R Script Optimization
- **Timestamp-Based Duplicate Prevention** - Replaced complex per-table checking with efficient timestamp filtering
- **Performance Improvement** - Pre-filtering data reduces database operations significantly
- **Multi-table Validation** - Comprehensive duplicate detection across all OHLCV tables
- **Optimized SQL Queries** - Check fetched timestamps against existing data before insertion

### v4.2.1 - PostgreSQL Compatibility & Dependencies
- **Fixed R Script Dependencies** - Added missing `crypto2` and `dplyr` packages
- **PostgreSQL Table Name Handling** - Fixed SQL syntax for numeric-prefixed table names
- **Environment Configuration Recovery** - Restored critical `.env` file from backup

[View Full Changelog](CHANGELOG.md)

---

## Key Features

### Multi-Database Architecture
- **`dbcp`** - Primary production database for live market data
- **`cp_ai`** - AI-enhanced analysis database with processed indicators
- **`cp_backtest` / `cp_backtest_h`** - Historical data for backtesting and strategy validation
- **Separation of Concerns** - Optimized for different use cases (live/backtest/AI)

### Comprehensive Technical Analysis (100+ Indicators)
- **Momentum Indicators (21)** - RSI (5 periods), ROC, Williams %R, SMI, CMO, TSI
- **Oscillators (33)** - MACD, CCI, ADX System, Ultimate, Awesome, TRIX
- **Financial Ratios (23)** - Alpha/Beta vs Bitcoin, Sharpe, Sortino, Treynor, Information Ratio
- **Fundamental Metrics (15+)** - ATH/ATL tracking, Market cap categories, Coin age analysis
- **Volume/Value Analysis (33)** - OBV, VWAP, Keltner/Donchian Channels, CMF
- **Risk Metrics (5)** - VaR, CVaR, Daily returns, Volume changes

### Automated Pipeline System
- **4-Stage Sequential Pipeline** - LISTINGS → OHLCV → DMV → QA
- **Daily Automation** - Runs at 5:00 AM UTC via GitHub Actions
- **Multi-Language Integration** - Python + R script orchestration
- **Error Handling** - Pipeline stops on failure with detailed logging
- **Quality Assurance** - AI-powered monitoring with real-time alerts

### AI-Powered Quality Assurance
- **Google Gemini Integration** - Intelligent analysis and issue summarization
- **Telegram Notifications** - Real-time alerts for critical issues
- **Risk Classification** - LOW/MEDIUM/HIGH/CRITICAL issue prioritization
- **Automated Cleanup** - Duplicate removal and database optimization
- **Comprehensive Monitoring** - Data freshness, schema integrity, performance metrics

### Backtesting Infrastructure
- **Historical Data Preservation** - Complete backtesting capabilities
- **Strategy Validation** - Test trading strategies with historical data
- **Weekly Automation** - Backtest pipeline runs every Sunday at 2:00 AM UTC
- **Automated Reporting** - Validation reports uploaded as GitHub artifacts

---

## Quick Start

### 1. Clone and Install
```bash
# Clone the repository
git clone https://github.com/CryptoPrism-io/CryptoPrism-DB.git
cd CryptoPrism-DB

# Install Python dependencies
pip install -r requirements.txt

# Install R dependencies (if using R scripts)
Rscript requirements.R
```

### 2. Configure Environment
```bash
# Create .env file with your credentials
cat > .env << EOF
# Database Configuration
DB_HOST=your_postgresql_host
DB_NAME=dbcp
DB_USER=your_username
DB_PASSWORD=your_password
DB_PORT=5432

# CoinMarketCap API
CMC_API_KEY=your_cmc_api_key

# Optional: Telegram Notifications
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# Optional: Google AI for QA
GEMINI_API_KEY=your_gemini_api_key
EOF
```

### 3. Create Databases
```sql
CREATE DATABASE dbcp;           -- Primary production
CREATE DATABASE cp_ai;          -- AI analysis
CREATE DATABASE cp_backtest;    -- Historical backtesting
```

### 4. Run Data Collection
```bash
# Fetch cryptocurrency listings
python gcp_postgres_sandbox/data_ingestion/cmc_listings.py

# Collect historical OHLCV data
Rscript gcp_postgres_sandbox/data_ingestion/gcp_108k_1kcoins.R

# Generate technical indicators
python gcp_postgres_sandbox/technical_analysis/gcp_dmv_mom.py
python gcp_postgres_sandbox/technical_analysis/gcp_dmv_core.py
```

### 5. Verify Installation
```bash
# Run quality assurance check
python gcp_postgres_sandbox/quality_assurance/prod_qa_dbcp.py
```

---

## Architecture

### System Overview

CryptoPrism-DB processes **1000+ cryptocurrencies** using a **3-database architecture** with **16 specialized modules** running in a **sequential pipeline**.

### Core Module Structure

#### Data Ingestion & Processing
- [cmc_listings.py](gcp_postgres_sandbox/data_ingestion/cmc_listings.py) - CoinMarketCap API integration
- [gcp_cc_info.py](gcp_postgres_sandbox/data_ingestion/gcp_cc_info.py) - Cryptocurrency metadata fetcher
- [gcp_fear_greed_cmc.py](gcp_postgres_sandbox/data_ingestion/gcp_fear_greed_cmc.py) - Market sentiment via Fear & Greed Index
- [gcp_108k_1kcoins.R](gcp_postgres_sandbox/data_ingestion/gcp_108k_1kcoins.R) - OHLCV historical data collection

#### Technical Analysis Pipeline (Run in Sequence)
1. [gcp_dmv_met.py](gcp_postgres_sandbox/technical_analysis/gcp_dmv_met.py) - Fundamental metrics
2. [gcp_dmv_tvv.py](gcp_postgres_sandbox/technical_analysis/gcp_dmv_tvv.py) - Volume/value analysis
3. [gcp_dmv_pct.py](gcp_postgres_sandbox/technical_analysis/gcp_dmv_pct.py) - Risk metrics
4. [gcp_dmv_mom.py](gcp_postgres_sandbox/technical_analysis/gcp_dmv_mom.py) - Momentum indicators
5. [gcp_dmv_osc.py](gcp_postgres_sandbox/technical_analysis/gcp_dmv_osc.py) - Oscillators
6. [gcp_dmv_rat.py](gcp_postgres_sandbox/technical_analysis/gcp_dmv_rat.py) - Financial ratios
7. **[gcp_dmv_core.py](gcp_postgres_sandbox/technical_analysis/gcp_dmv_core.py)** - Final signal aggregation (always run last)

#### Quality Assurance System
- [prod_qa_dbcp.py](gcp_postgres_sandbox/quality_assurance/prod_qa_dbcp.py) - Production database monitoring
- [prod_qa_cp_ai.py](gcp_postgres_sandbox/quality_assurance/prod_qa_cp_ai.py) - AI database QA with alerts
- [prod_qa_cp_ai_backtest.py](gcp_postgres_sandbox/quality_assurance/prod_qa_cp_ai_backtest.py) - AI backtest validation
- [prod_qa_dbcp_backtest.py](gcp_postgres_sandbox/quality_assurance/prod_qa_dbcp_backtest.py) - Backtest database validation

#### Backtesting Infrastructure
- [gcp_dmv_mom_backtest.py](gcp_postgres_sandbox/backtesting/gcp_dmv_mom_backtest.py) - Historical momentum analysis
- [test_backtest_mom_data.py](gcp_postgres_sandbox/backtesting/test_backtest_mom_data.py) - Backtest data validation

### Database Schema

#### Primary Signal Tables
```
FE_MOMENTUM_SIGNALS       # Momentum-based trading signals
FE_OSCILLATORS_SIGNALS    # Technical oscillator signals
FE_RATIOS_SIGNALS        # Financial ratio signals
FE_METRICS_SIGNAL        # Fundamental analysis signals
FE_TVV_SIGNALS           # Volume/value technical signals
FE_DMV_ALL               # Aggregated signal matrix (primary output)
FE_DMV_SCORES            # Durability/Momentum/Valuation scores
```

#### Input Tables
```
crypto_listings_latest_1000   # CoinMarketCap listings data
1K_coins_ohlcv               # OHLCV historical price data
108_1K_coins_ohlcv           # Extended historical data
```

---

## Automated Pipeline

### GitHub Actions 4-Stage Pipeline

```
Stage 1: LISTINGS (Daily 5:00 AM UTC)
   ↓
Stage 2: OHLCV (Sequential after LISTINGS)
   ↓
Stage 3: DMV (Sequential after OHLCV)
   ↓
Stage 4: QA (Manual/On-demand)
```

### Stage Details

#### Stage 1: LISTINGS
- **Trigger**: `cron: '05 0 * * *'` (Daily 5:00 AM UTC)
- **Module**: [cmc_listings.py](gcp_postgres_sandbox/data_ingestion/cmc_listings.py)
- **Purpose**: Fetch top 1000 cryptocurrencies from CoinMarketCap API
- **Output**: Updated `crypto_listings_latest_1000` table

#### Stage 2: OHLCV
- **Trigger**: Sequential after LISTINGS completion
- **Module**: [gcp_108k_1kcoins.R](gcp_postgres_sandbox/data_ingestion/gcp_108k_1kcoins.R)
- **Purpose**: Collect OHLCV (Open, High, Low, Close, Volume) data
- **Technology**: R with crypto2 package integration

#### Stage 3: DMV (Durability, Momentum, Valuation)
- **Trigger**: Sequential after OHLCV completion
- **Modules**: 8 scripts run in sequence
  1. [gcp_fear_greed_cmc.py](gcp_postgres_sandbox/data_ingestion/gcp_fear_greed_cmc.py) - Market sentiment
  2. [gcp_dmv_met.py](gcp_postgres_sandbox/technical_analysis/gcp_dmv_met.py) - Fundamental metrics
  3. [gcp_dmv_tvv.py](gcp_postgres_sandbox/technical_analysis/gcp_dmv_tvv.py) - Volume/trend analysis
  4. [gcp_dmv_pct.py](gcp_postgres_sandbox/technical_analysis/gcp_dmv_pct.py) - Risk metrics
  5. [gcp_dmv_mom.py](gcp_postgres_sandbox/technical_analysis/gcp_dmv_mom.py) - Momentum indicators
  6. [gcp_dmv_osc.py](gcp_postgres_sandbox/technical_analysis/gcp_dmv_osc.py) - Technical oscillators
  7. [gcp_dmv_rat.py](gcp_postgres_sandbox/technical_analysis/gcp_dmv_rat.py) - Financial ratios
  8. **[gcp_dmv_core.py](gcp_postgres_sandbox/technical_analysis/gcp_dmv_core.py)** - Final aggregation

#### Stage 4: QA
- **Trigger**: Manual execution via `workflow_dispatch`
- **Module**: [prod_qa_dbcp.py](gcp_postgres_sandbox/quality_assurance/prod_qa_dbcp.py)
- **Features**: AI-powered analysis, Telegram alerts, automated cleanup

### Independent Workflows

#### Weekly Backtest Pipeline
- **Schedule**: Sunday 2:00 AM UTC
- **Purpose**: Historical data processing for strategy validation
- **Modules**:
  - [gcp_dmv_mom_backtest.py](gcp_postgres_sandbox/backtesting/gcp_dmv_mom_backtest.py)
  - [test_backtest_mom_data.py](gcp_postgres_sandbox/backtesting/test_backtest_mom_data.py)

---

## Project Structure

```
CryptoPrism-DB/
├── gcp_postgres_sandbox/              # Main processing modules
│   ├── data_ingestion/                # Data collection scripts
│   │   ├── cmc_listings.py           # CoinMarketCap API integration
│   │   ├── gcp_cc_info.py            # Crypto metadata fetcher
│   │   ├── gcp_fear_greed_cmc.py     # Market sentiment data
│   │   └── gcp_108k_1kcoins.R        # OHLCV data collection (R)
│   │
│   ├── technical_analysis/            # TA pipeline (run in order)
│   │   ├── gcp_dmv_met.py            # Fundamental metrics
│   │   ├── gcp_dmv_tvv.py            # Volume/value analysis
│   │   ├── gcp_dmv_pct.py            # Risk metrics
│   │   ├── gcp_dmv_mom.py            # Momentum indicators
│   │   ├── gcp_dmv_osc.py            # Oscillators
│   │   ├── gcp_dmv_rat.py            # Financial ratios
│   │   └── gcp_dmv_core.py           # Signal aggregation (run last)
│   │
│   ├── quality_assurance/             # QA and monitoring
│   │   ├── prod_qa_dbcp.py           # Production DB monitoring
│   │   ├── prod_qa_cp_ai.py          # AI DB QA with alerts
│   │   ├── prod_qa_cp_ai_backtest.py # AI backtest validation
│   │   └── prod_qa_dbcp_backtest.py  # Backtest DB validation
│   │
│   └── backtesting/                   # Historical analysis
│       ├── gcp_dmv_mom_backtest.py   # Historical momentum
│       └── test_backtest_mom_data.py # Data validation tests
│
├── .github/workflows/                 # GitHub Actions pipelines
│   ├── LISTINGS.yml                  # Stage 1: Data collection
│   ├── OHLCV.yml                     # Stage 2: OHLCV fetching
│   ├── DMV.yml                       # Stage 3: TA pipeline
│   └── QA.yml                        # Stage 4: Quality checks
│
├── CHANGELOG.md                       # Version history
├── CLAUDE.md                          # Project instructions
├── CRON_SCHEDULE_README.md           # Pipeline scheduling docs
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
└── requirements.R                     # R dependencies
```

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Primary Language** | Python 3.8+ | Data processing and technical analysis |
| **Secondary Language** | R 4.0+ | OHLCV data collection via crypto2 package |
| **Database** | PostgreSQL 12+ | Multi-database architecture (3 instances) |
| **ORM** | SQLAlchemy 2.0+ | Database connectivity and operations |
| **Data Processing** | Pandas, NumPy | DataFrame operations and calculations |
| **API Integration** | Requests | CoinMarketCap API calls |
| **AI Analysis** | Google Gemini | Quality assurance and issue analysis |
| **Notifications** | Telegram Bot | Real-time alerts and monitoring |
| **CI/CD** | GitHub Actions | Automated pipeline orchestration |
| **Environment** | python-dotenv | Secure credential management |

---

## Usage Examples

### Query Latest Bullish Signals
```sql
-- Get top 20 bullish cryptocurrencies
SELECT
    slug,
    timestamp,
    bullish,
    bearish,
    neutral,
    Durability_Score,
    Momentum_Score,
    Valuation_Score
FROM FE_DMV_ALL
WHERE bullish > bearish
  AND timestamp = (SELECT MAX(timestamp) FROM FE_DMV_ALL)
ORDER BY bullish DESC, Momentum_Score DESC
LIMIT 20;
```

### Risk Analysis Query
```sql
-- Identify high-risk assets
SELECT
    f.slug,
    f.sharpe_ratio,
    f.sortino_ratio,
    f.max_drawdown,
    m.rsi_9,
    m.williams_r_14
FROM FE_RATIOS_SIGNALS f
JOIN FE_MOMENTUM_SIGNALS m
    ON f.slug = m.slug
    AND f.timestamp = m.timestamp
WHERE f.sharpe_ratio < 0
   OR f.max_drawdown < -50
ORDER BY f.sharpe_ratio ASC;
```

### Technical Analysis Pipeline
```bash
# Complete pipeline execution
python gcp_postgres_sandbox/technical_analysis/gcp_dmv_met.py
python gcp_postgres_sandbox/technical_analysis/gcp_dmv_tvv.py
python gcp_postgres_sandbox/technical_analysis/gcp_dmv_pct.py
python gcp_postgres_sandbox/technical_analysis/gcp_dmv_mom.py
python gcp_postgres_sandbox/technical_analysis/gcp_dmv_osc.py
python gcp_postgres_sandbox/technical_analysis/gcp_dmv_rat.py
python gcp_postgres_sandbox/technical_analysis/gcp_dmv_core.py  # Always run last
```

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| **API Rate Limits** | CoinMarketCap has request limits; adjust timing in scripts or upgrade API plan |
| **Database Connections** | Ensure PostgreSQL allows multiple concurrent connections (check `max_connections`) |
| **Memory Usage** | Processing 1000+ coins requires 4GB+ RAM; consider batch processing |
| **Timezone Issues** | All timestamps stored in UTC; ensure proper timezone handling in queries |
| **R Script Errors** | Verify crypto2 and dplyr packages installed: `install.packages(c("crypto2", "dplyr"))` |
| **Duplicate Key Errors** | Run QA scripts to clean duplicates or check timestamp-based filtering |

### Performance Optimization
- Use connection pooling for high-frequency operations
- Implement database indexing on `(slug, timestamp)` columns
- Consider partitioning large historical tables by date
- Monitor and tune PostgreSQL configuration for your workload
- Review query execution plans with `EXPLAIN ANALYZE`

### Monitoring & Alerts
- **AI-Powered QA**: Run [prod_qa_cp_ai.py](gcp_postgres_sandbox/quality_assurance/prod_qa_cp_ai.py) for intelligent analysis
- **Telegram Notifications**: Configure bot token in `.env` for real-time alerts
- **Database Health**: Monitor key metrics via QA scripts
  - Data freshness and completeness
  - Schema integrity validation
  - Technical indicator value ranges
  - Database performance metrics

---

## Documentation

### Core Documentation
- [CHANGELOG.md](CHANGELOG.md) - Complete version history with commit references
- [CLAUDE.md](CLAUDE.md) - Project instructions and development patterns
- [CRON_SCHEDULE_README.md](CRON_SCHEDULE_README.md) - Pipeline scheduling documentation
- [EMERGENCY_ROLLBACK_STRATEGY.txt](EMERGENCY_ROLLBACK_STRATEGY.txt) - Production safety procedures

### External Resources
- [CoinMarketCap API Documentation](https://coinmarketcap.com/api/documentation/v1/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [crypto2 R Package](https://github.com/sstoeckl/crypto2)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

---

## Development Patterns

### Database Connections
All scripts use SQLAlchemy with PostgreSQL:
```python
from sqlalchemy import create_engine
import os

engine = create_engine(
    f'postgresql+pg8000://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}'
    f'@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}'
)
```

### Error Handling & Logging
```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
```

### Signal Generation Pattern
- Binary signals: bullish/bearish/neutral counts per cryptocurrency
- Multi-timeframe analysis (5, 9, 14, 21, 50, 200 periods)
- Bitcoin-relative performance metrics (Alpha/Beta vs BTC)
- Risk-adjusted ratios (Sharpe, Sortino, Information ratios)

---

## Contributing

Contributions welcome for:
- Additional technical indicators
- Performance optimizations
- New data sources integration
- Enhanced visualization capabilities
- Documentation improvements

### Development Setup
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run quality assurance checks
5. Submit a pull request

---

## Roadmap

### Planned Features
- Real-time streaming data support
- Machine learning model integration
- Advanced portfolio optimization
- Multi-exchange data aggregation
- Enhanced visualization dashboard
- API endpoint for external access

### Performance Goals
- Sub-second query performance for all signal tables
- Support for 2000+ cryptocurrencies
- Real-time indicator calculation
- Distributed processing capabilities

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Disclaimer

**This system is for research and educational purposes only.** Cryptocurrency trading involves significant financial risks. Always perform your own analysis and risk assessment before making trading decisions. Past performance does not guarantee future results.

---

<div align="center">

**Built for Cryptocurrency Traders and Researchers**

[Documentation](CLAUDE.md) • [Changelog](CHANGELOG.md) • [Issues](https://github.com/CryptoPrism-io/CryptoPrism-DB/issues)

[Back to Top](#cryptoprism-db)

</div>
