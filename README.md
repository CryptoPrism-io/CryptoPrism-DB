# CryptoPrism-DB: Advanced Cryptocurrency Technical Analysis System

A comprehensive cryptocurrency data pipeline featuring multi-layered technical analysis, automated quality assurance, and sophisticated backtesting capabilities for algorithmic trading research and development.

---

## 🔍 WHAT - System Overview

### Architecture
CryptoPrism-DB operates on a **3-database architecture** designed for separation of concerns:

- **`dbcp`** - Primary production database containing live market data
- **`cp_ai`** - AI-enhanced analysis database with processed indicators  
- **`cp_backtest` / `cp_backtest_h`** - Historical data for backtesting and strategy validation

### Core Modules (16 Specialized Components)

#### 📊 **Data Ingestion & Processing**
- **`cmc_listings.py`** - CoinMarketCap API integration for top 1000 cryptocurrencies
- **`gcp_cc_info.py`** - Cryptocurrency metadata and URL information fetcher
- **`gcp_fear_greed_cmc.py`** - Market sentiment data via Fear & Greed Index

#### 🔧 **Technical Analysis Engine** 
- **`gcp_dmv_core.py`** - Central aggregation hub merging all signal types
- **`gcp_dmv_mom.py`** - Momentum indicators (RSI, ROC, Williams %R, SMI, CMO, TSI)
- **`gcp_dmv_osc.py`** - Oscillators (MACD, CCI, ADX, Ultimate, Awesome, TRIX)  
- **`gcp_dmv_rat.py`** - Financial ratios (Alpha/Beta, Sharpe, Sortino, Information Ratio)
- **`gcp_dmv_met.py`** - Fundamental metrics (ATH/ATL, market cap categories, coin age)
- **`gcp_dmv_tvv.py`** - Technical volume/value (OBV, VWAP, Keltner/Donchian channels)
- **`gcp_dmv_pct.py`** - Risk metrics (VaR, CVaR, percentage returns)

#### 🔄 **Backtesting Infrastructure**
- **`gcp_dmv_mom_backtest.py`** - Historical momentum analysis for strategy testing
- **`test_backtest_mom_data.py`** - Comprehensive data validation test suite

#### 🛡️ **Quality Assurance System**
- **`prod_qa_cp_ai.py`** - AI-powered QA for analysis database
- **`prod_qa_cp_ai_backtest.py`** - Backtest data quality monitoring  
- **`prod_qa_dbcp.py`** - Production database health monitoring
- **`prod_qa_dbcp_backtest.py`** - Backtest database validation

### Technical Indicators Coverage (100+ Indicators)

| Category | Count | Key Indicators |
|----------|-------|----------------|
| **Momentum** | 21 | RSI (5 periods), ROC, Williams %R, SMI, CMO, TSI |
| **Oscillators** | 33 | MACD, CCI, ADX System, Ultimate, Awesome, TRIX |
| **Ratios** | 23 | Alpha/Beta vs Bitcoin, Sharpe, Sortino, Treynor, Information Ratio |
| **Metrics** | 15+ | ATH/ATL tracking, Market cap categories, Coin age analysis |
| **Volume/Value** | 33 | OBV, VWAP, Keltner/Donchian Channels, CMF |
| **Risk** | 5 | VaR, CVaR, Daily returns, Volume changes |

### Database Schema

#### Primary Tables Structure
```
FE_OSCILLATORS_SIGNALS    # Technical oscillator signals
FE_MOMENTUM_SIGNALS       # Momentum-based trading signals  
FE_METRICS_SIGNAL         # Fundamental analysis signals
FE_TVV_SIGNALS           # Volume/value technical signals
FE_RATIOS_SIGNALS        # Financial ratio signals
FE_DMV_ALL               # Aggregated signal matrix
FE_DMV_SCORES            # Durability/Momentum/Valuation scores
```

---

## 🎯 WHY - Business Rationale

### Cryptocurrency Trading Challenges

#### **Market Complexity**
- 1000+ actively traded cryptocurrencies with varying volatility profiles
- 24/7 global markets requiring continuous monitoring
- High-frequency price movements demanding real-time analysis
- Complex correlations between different crypto assets

#### **Risk Management Needs**  
- **Quantitative Risk Assessment** - VaR/CVaR calculations for portfolio protection
- **Multi-timeframe Analysis** - Indicators across different time horizons
- **Benchmark Comparison** - Bitcoin-relative performance metrics
- **Drawdown Protection** - Advanced risk metrics (Risk of Ruin, Gain-to-Pain ratios)

#### **Algorithmic Trading Requirements**
- **Signal Generation** - Binary buy/sell signals from 100+ technical indicators
- **Backtesting Infrastructure** - Historical validation of trading strategies  
- **Data Quality Assurance** - AI-powered monitoring for reliable decision-making
- **Scalable Architecture** - Handle high-volume real-time data processing

### Competitive Advantages

#### **Comprehensive Coverage**
- **Complete Technical Analysis Suite** - Most extensive indicator coverage available
- **Multi-Database Architecture** - Optimized for different use cases (live/backtest/AI)
- **Quality-First Approach** - AI-powered data validation with Telegram alerting
- **Bitcoin Benchmark Analysis** - Crypto-native relative performance metrics

#### **Research & Development Focus**  
- **Historical Data Preservation** - Complete backtesting capabilities
- **Modular Design** - Easy to extend with new indicators or data sources
- **Production-Ready** - Battle-tested QA systems with automated monitoring
- **Cost-Effective** - Open-source alternative to expensive trading platforms

---

## ⚙️ HOW - Implementation Guide

### Prerequisites

#### **System Requirements**
- Python 3.8+ 
- PostgreSQL 12+ databases (3 instances recommended)
- 4GB+ RAM (for processing 1000+ cryptocurrencies)
- CoinMarketCap Pro API key

#### **Database Setup**
```sql
-- Create three databases
CREATE DATABASE dbcp;           -- Primary production  
CREATE DATABASE cp_ai;          -- AI analysis
CREATE DATABASE cp_backtest;    -- Historical backtesting
```

### Installation

#### **1. Clone Repository**
```bash
git clone https://github.com/your-repo/CryptoPrism-DB.git
cd CryptoPrism-DB
```

#### **2. Install Dependencies**
```bash
pip install -r requirements.txt
```

#### **3. Environment Configuration**
Create `.env` file with required variables:
```env
# CoinMarketCap API
CMC_API_KEY=your_cmc_api_key_here
DB_URL=postgresql+psycopg2://user:pass@host:5432/dbcp

# Database Connections  
DB_HOST=your_postgresql_host
DB_NAME=dbcp
DB_USER=your_username  
DB_PASSWORD=your_password
DB_PORT=5432

# Telegram Notifications (Optional)
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id

# Google AI (for QA system)
GOOGLE_API_KEY=your_google_gemini_api_key
```

### Execution Workflows

#### **Data Collection Pipeline**
```bash
# Step 1: Fetch latest cryptocurrency listings
python gcp_postgres_sandbox/cmc_listings.py

# Step 2: Get cryptocurrency metadata  
python gcp_postgres_sandbox/gcp_cc_info.py

# Step 3: Collect sentiment data
python gcp_postgres_sandbox/gcp_fear_greed_cmc.py
```

#### **Technical Analysis Pipeline**
```bash
# Generate all technical indicators (run in sequence)
python gcp_postgres_sandbox/gcp_dmv_mom.py      # Momentum indicators
python gcp_postgres_sandbox/gcp_dmv_osc.py      # Oscillators  
python gcp_postgres_sandbox/gcp_dmv_rat.py      # Financial ratios
python gcp_postgres_sandbox/gcp_dmv_met.py      # Fundamental metrics
python gcp_postgres_sandbox/gcp_dmv_tvv.py      # Volume/value analysis
python gcp_postgres_sandbox/gcp_dmv_pct.py      # Risk metrics

# Final aggregation step
python gcp_postgres_sandbox/gcp_dmv_core.py     # Merge all signals
```

#### **Backtesting Mode**
```bash
# Generate historical data for backtesting
python gcp_postgres_sandbox/gcp_dmv_mom_backtest.py

# Validate backtest data quality
python gcp_postgres_sandbox/test_backtest_mom_data.py
```

#### **Quality Assurance**
```bash
# Production database monitoring
python gcp_postgres_sandbox/prod_qa_dbcp.py

# AI database quality checks  
python gcp_postgres_sandbox/prod_qa_cp_ai.py

# Backtest data validation
python gcp_postgres_sandbox/prod_qa_cp_ai_backtest.py
python gcp_postgres_sandbox/prod_qa_dbcp_backtest.py
```

### Automation Setup

#### **Cron Jobs for Live Trading** (Linux/macOS)
```bash
# Add to crontab (crontab -e)

# Data collection every 6 hours
0 */6 * * * /path/to/python /path/to/cmc_listings.py
30 */6 * * * /path/to/python /path/to/gcp_cc_info.py

# Technical analysis every hour  
0 * * * * /path/to/python /path/to/gcp_dmv_mom.py
5 * * * * /path/to/python /path/to/gcp_dmv_osc.py
10 * * * * /path/to/python /path/to/gcp_dmv_rat.py
15 * * * * /path/to/python /path/to/gcp_dmv_core.py

# Quality checks every 4 hours
0 */4 * * * /path/to/python /path/to/prod_qa_cp_ai.py
```

#### **Windows Task Scheduler**
Create scheduled tasks for each script with appropriate intervals.

### Usage Examples

#### **Query Latest Signals**
```sql
-- Get latest bullish signals for top market cap coins
SELECT slug, timestamp, bullish, bearish, neutral,
       Durability_Score, Momentum_Score, Valuation_Score
FROM FE_DMV_ALL 
WHERE bullish > bearish 
  AND timestamp = (SELECT MAX(timestamp) FROM FE_DMV_ALL)
ORDER BY bullish DESC, Momentum_Score DESC
LIMIT 20;
```

#### **Risk Analysis Query**  
```sql
-- Identify high-risk assets with poor ratios
SELECT f.slug, f.sharpe_ratio, f.sortino_ratio, f.max_drawdown,
       m.rsi_9, m.williams_r_14
FROM FE_RATIOS f
JOIN FE_MOMENTUM m ON f.slug = m.slug AND f.timestamp = m.timestamp  
WHERE f.sharpe_ratio < 0 OR f.max_drawdown < -50
ORDER BY f.sharpe_ratio ASC;
```

### Monitoring & Alerts

#### **AI-Powered Quality Assurance**
The system includes automated monitoring with:
- **Telegram Integration** - Real-time alerts for critical issues
- **Google Gemini AI** - Intelligent analysis and summarization
- **Risk Classification** - LOW/MEDIUM/HIGH/CRITICAL issue prioritization
- **Automated Cleanup** - Duplicate removal and database optimization

#### **Key Metrics Monitored**
- Data freshness and completeness
- Schema integrity validation  
- Technical indicator value ranges
- Database performance metrics
- API connectivity and rate limits

### Troubleshooting

#### **Common Issues**
1. **API Rate Limits** - CoinMarketCap has request limits; adjust timing in scripts
2. **Database Connections** - Ensure PostgreSQL allows multiple concurrent connections
3. **Memory Usage** - Processing 1000+ coins requires sufficient RAM
4. **Timezone Handling** - All timestamps stored in UTC

#### **Performance Optimization**  
- Use connection pooling for high-frequency operations
- Implement database indexing on slug+timestamp columns
- Consider partitioning large historical tables
- Monitor and tune PostgreSQL configuration

---

## 📈 Getting Started

1. **Start Small** - Begin with `cmc_listings.py` to understand data flow
2. **Add Indicators** - Run momentum analysis with `gcp_dmv_mom.py`  
3. **Quality Check** - Use QA scripts to validate data integrity
4. **Scale Up** - Add more indicator categories as needed
5. **Automate** - Set up cron jobs for continuous operation

## 🤝 Contributing

This project is designed for cryptocurrency trading research and development. Contributions welcome for:
- Additional technical indicators
- Performance optimizations  
- New data sources integration
- Enhanced visualization capabilities

---

**⚠️ Disclaimer**: This system is for research and educational purposes. Cryptocurrency trading involves significant risks. Always perform your own analysis and risk assessment before making trading decisions.
