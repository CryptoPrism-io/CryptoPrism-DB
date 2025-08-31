# gcp_dmv_mom_backtest.py - Historical Momentum Analysis Engine

## Overview
This script is the **backtesting-optimized momentum analysis module** of the CryptoPrism-DB system, designed to process complete historical OHLCV datasets without time restrictions for comprehensive backtesting and research purposes. It calculates the same 21 momentum indicators as the live version but operates on full historical data to enable strategy backtesting and performance analysis.

## Detailed Functionality

### **Backtesting Architecture Differences**

#### **1. No Time Filtering**
```python
def fetch_data_backtest(engine):
    query = """
    SELECT "public"."1K_coins_ohlcv".*
    FROM "public"."1K_coins_ohlcv"
    INNER JOIN "public"."crypto_listings_latest_1000"
    ON "public"."1K_coins_ohlcv"."slug" = "public"."crypto_listings_latest_1000"."slug"
    WHERE "public"."crypto_listings_latest_1000"."cmc_rank" <= 1000;
    """
```
- **Key Difference**: No `INTERVAL '110 days'` time restriction
- **Data Volume**: Processes complete historical datasets (potentially years of data)
- **Purpose**: Enables comprehensive backtesting across multiple market cycles

#### **2. Environment-Based Configuration**
```python
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "34.55.195.199"),
    "user": os.getenv("DB_USER", "yogass09"),
    "password": os.getenv("DB_PASSWORD", "jaimaakamakhya"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "dbcp"),
    "database_bt": os.getenv("DB_BT_NAME", "cp_backtest")
}
```
- **Flexibility**: Environment variables allow configuration changes without code modification
- **Security**: Credentials can be managed externally
- **Deployment**: Easier container and CI/CD integration

### **Identical Momentum Calculations**

The script implements the **same 21 momentum indicators** as `gcp_dmv_mom.py`:

#### **RSI Family (5 indicators)**
- RSI with periods: 9, 18, 27, 54, 108 days
- Uses Wilder's exponential smoothing method
- Range: 0-100 (overbought >70, oversold <30)

#### **Additional Momentum Indicators (16 indicators)**
- **Rate of Change (ROC)**: 9-day price momentum
- **Williams %R**: 14-day stochastic oscillator
- **Stochastic Momentum Index (SMI)**: Double-smoothed momentum
- **Chande Momentum Oscillator (CMO)**: 14-day momentum with overbought/oversold levels
- **True Strength Index (TSI)**: Double-smoothed price momentum
- **Standard Momentum**: 10-day simple momentum
- **Plus additional technical indicators for comprehensive analysis**

### **Backtesting-Specific Features**

#### **3. Historical Signal Generation**
```python
# Same binary signal logic as live version
m_rsi_9_signal = np.where(rsi_9 > 70, 1, np.where(rsi_9 < 30, -1, 0))
m_rsi_54_signal = np.where(rsi_54 > 70, 1, np.where(rsi_54 < 30, -1, 0))
m_williams_r_signal = np.where(williams_r_14 > -20, 1, np.where(williams_r_14 < -80, -1, 0))
m_roc_signal = np.where(roc_9 > 5, 1, np.where(roc_9 < -5, -1, 0))
m_momentum_signal = np.where(momentum_10 > 0, 1, np.where(momentum_10 < 0, -1, 0))
```
- **Historical Perspective**: Generates signals for every historical data point
- **Strategy Testing**: Enables comprehensive backtesting of momentum strategies
- **Performance Analysis**: Full signal history for statistical analysis

#### **4. Memory Management for Large Datasets**
```python
# Efficient processing for potentially massive historical datasets
logging.info(f"✅ ALL historical data fetched for backtest. Shape: {df.shape}")
```
- **Large Data Handling**: Designed for processing years of historical data
- **Memory Optimization**: Efficient pandas operations for large datasets
- **Progress Tracking**: Detailed logging for long-running operations

### **Database Strategy**

#### **5. Backtest Database Focus**
```python
# Writes to backtest database only
engine_bt = create_db_engine_backtest()
df.to_sql('FE_MOMENTUM_BACKTEST', con=engine_bt, if_exists='append', index=False)
```
- **Dedicated Storage**: Uses `cp_backtest` database exclusively
- **Append Mode**: Accumulates historical data over multiple runs
- **Research Infrastructure**: Supports quantitative research and strategy development

### **Use Cases**

#### **6. Strategy Backtesting**
- **Momentum Strategy Testing**: Evaluate RSI, ROC, and other momentum strategies
- **Signal Performance**: Analyze historical signal accuracy and profitability
- **Risk Assessment**: Understand drawdowns and volatility of momentum strategies

#### **7. Research Applications**
- **Market Cycle Analysis**: Study momentum behavior across different market conditions
- **Indicator Optimization**: Find optimal parameters for momentum indicators
- **Comparative Studies**: Compare performance across different cryptocurrencies

### **Integration Points**
- **Upstream**: Same as live version - `1K_coins_ohlcv` and `crypto_listings_latest_1000`
- **Research Tools**: Feeds quantitative research and academic studies
- **Strategy Development**: Supports trading algorithm development and testing
- **Pipeline Position**: **Parallel to live system** - research and backtesting branch

## Usage

### **Environment Setup**
```bash
# Optional environment variables (defaults provided)
export DB_HOST="your_postgres_host"
export DB_USER="your_username"
export DB_PASSWORD="your_password"
export DB_PORT="5432"
export DB_NAME="dbcp"
export DB_BT_NAME="cp_backtest"
```

### **Execution**
```bash
python gcp_postgres_sandbox/gcp_dmv_mom_backtest.py
# Runtime: 15-60+ minutes depending on historical data volume
# Memory: 500MB-2GB depending on dataset size
```

### **Automated Research Workflows**
```yaml
# GitHub Actions for periodic backtesting updates
- name: Update Historical Momentum Analysis
  run: python gcp_postgres_sandbox/gcp_dmv_mom_backtest.py
  # Scheduled for weekly execution to build historical database
```

## Dependencies
- **pandas>=2.2.2** - Large dataset manipulation and time series analysis
- **numpy>=1.26.4** - Mathematical operations and array processing
- **sqlalchemy>=2.0.32** - Database connectivity with connection pooling
- **logging** - Comprehensive progress tracking for long operations
- **time** - Performance monitoring for large dataset processing
- **os** - Environment variable management

## Output Schema
```sql
-- Same structure as live version but with complete historical data
CREATE TABLE FE_MOMENTUM_BACKTEST (
    slug VARCHAR(255),
    timestamp TIMESTAMP,
    
    -- All 21 momentum indicators (same as live version)
    rsi_9 DECIMAL(8,4),
    rsi_18 DECIMAL(8,4),
    rsi_27 DECIMAL(8,4),
    rsi_54 DECIMAL(8,4),
    rsi_108 DECIMAL(8,4),
    roc_9 DECIMAL(10,6),
    williams_r_14 DECIMAL(8,4),
    smi DECIMAL(8,4),
    smi_signal DECIMAL(8,4),
    cmo_14 DECIMAL(8,4),
    tsi DECIMAL(8,4),
    momentum_10 DECIMAL(15,8),
    -- Additional momentum indicators...
    
    -- Historical timestamps preserved for backtesting
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE FE_MOMENTUM_SIGNALS_BACKTEST (
    slug VARCHAR(255),
    timestamp TIMESTAMP,
    m_rsi_9_signal INTEGER,
    m_rsi_54_signal INTEGER,
    m_williams_r_signal INTEGER,
    m_roc_signal INTEGER,
    m_momentum_signal INTEGER
);
```

## Key Features
1. **Complete Historical Analysis**: No time restrictions for comprehensive backtesting
2. **Environment Configuration**: Flexible deployment with environment variables
3. **Large Dataset Optimization**: Efficient processing of multi-year historical data
4. **Research Infrastructure**: Dedicated backtesting database for quantitative analysis
5. **Strategy Development**: Full historical signal generation for strategy testing
6. **Academic Research**: Supports cryptocurrency market research and studies
7. **Performance Tracking**: Detailed logging for long-running historical analysis
8. **Memory Management**: Optimized for processing large historical datasets
