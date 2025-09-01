Note: All sensitive configuration values are redacted in this document.

# gcp_dmv_core.py - Central Signal Aggregation Hub

## Overview
This script is the **final aggregation engine** of the CryptoPrism-DB system, serving as the central hub that merges all technical analysis signals from 5 different modules into unified DMV (Durability, Momentum, Valuation) scores and comprehensive signal matrices.

## Detailed Functionality

### **Signal Collection Process**
The script performs a **FULL OUTER JOIN** operation across 5 signal tables:

```sql
SELECT *
FROM "FE_OSCILLATORS_SIGNALS" AS o
FULL OUTER JOIN "FE_MOMENTUM_SIGNALS" AS m USING (slug, timestamp)
FULL OUTER JOIN "FE_METRICS_SIGNAL" AS me USING (slug, timestamp)  
FULL OUTER JOIN "FE_TVV_SIGNALS" AS t USING (slug, timestamp)
FULL OUTER JOIN "FE_RATIOS_SIGNALS" AS r USING (slug, timestamp);
```

### **Data Processing Pipeline**

#### **1. Data Cleaning & Validation**
```python
# Remove duplicate columns from JOIN operations
df = df.loc[:, ~df.columns.duplicated()]

# Smart filtering: Keep Bitcoin always + complete signal rows
df = df.pipe(lambda d: d[d['slug'].eq('bitcoin') | 
                       d.drop(columns='slug').notna().all(axis=1)])

# Fill missing values with neutral signals (0)
df = df.fillna(0)
```

#### **2. Signal Aggregation (Bullish/Bearish/Neutral Counts)**
```python
# Initialize signal counters
for col in ['bullish', 'bearish', 'neutral']:
    df[col] = 0

# Count signals across all indicators (columns 4 onwards)
for index, row in df.iloc[:, 4:].iterrows():
    df.at[index, 'bullish'] = (row == 1).sum()    # Buy signals
    df.at[index, 'bearish'] = (row == -1).sum()   # Sell signals  
    df.at[index, 'neutral'] = (row == 0).sum()    # Neutral signals
```

#### **3. DMV Score Calculation**

##### **Durability Signals (d_ prefix)**
```python
Durability = df[['slug'] + [c for c in df.columns if c.startswith('d_')]]
Durability['Durability_Score'] = Durability.drop('slug', axis=1).sum(axis=1) / (Durability.shape[1] - 1) * 100
```
- **Purpose**: Long-term stability and risk assessment
- **Components**: Risk metrics, volatility measures, drawdown indicators
- **Scale**: 0-100 (higher = more durable/stable)

##### **Momentum Signals (m_ prefix)**  
```python
Momentum = df[['slug'] + [c for c in df.columns if c.startswith('m_')]]
Momentum['Momentum_Score'] = Momentum.drop('slug', axis=1).sum(axis=1) / (Momentum.shape[1] - 1) * 100
```
- **Purpose**: Short-term price movement strength and direction
- **Components**: RSI, ROC, Williams %R, momentum oscillators
- **Scale**: 0-100 (higher = stronger bullish momentum)

##### **Valuation Signals (v_ prefix)**
```python  
Valuation = df[['slug'] + [c for c in df.columns if c.startswith('v_')]]
Valuation['Valuation_Score'] = Valuation.drop('slug', axis=1).sum(axis=1) / (Valuation.shape[1] - 1) * 100
```
- **Purpose**: Fundamental value assessment and market positioning
- **Components**: Market cap ratios, ATH/ATL analysis, coin age metrics
- **Scale**: 0-100 (higher = better valuation opportunity)

#### **4. Latest Data Filtering**
```python
latest_timestamp = df['timestamp'].max()
df = df[df['timestamp'] == latest_timestamp]

# Validation check
if df['timestamp'].nunique() == 1:
    print("✅ Latest timestamp filtered.")
else:
    print("⚠️ Multiple timestamps still exist.")
```

### **Output Tables Generated**

#### **FE_DMV_ALL** (Complete Signal Matrix)
- **Content**: All aggregated signals with bullish/bearish/neutral counts
- **Purpose**: Comprehensive view of all technical indicators
- **Update Mode**: `if_exists='replace'` (latest data only)

#### **FE_DMV_SCORES** (DMV Scoring System)
```python
dmv_scores = pd.DataFrame({
    'slug': df['slug'],
    'timestamp': df['timestamp'], 
    'Durability_Score': Durability['Durability_Score'],
    'Momentum_Score': Momentum['Momentum_Score'],
    'Valuation_Score': Valuation['Valuation_Score']
})
```
- **Content**: Normalized 0-100 scores for each DMV category
- **Purpose**: High-level cryptocurrency ranking and comparison
- **Batch Size**: 10,000 records for optimized uploads

### **Performance Optimization**
```python
BATCH_SIZE = 10000

# Chunked database uploads for large datasets
dmv_scores.to_sql('FE_DMV_SCORES', con=gcp_engine, 
                  if_exists='replace', index=False, 
                  method='multi', chunksize=BATCH_SIZE)
```

### **Error Handling & Monitoring**
```python
# Comprehensive logging with file + console output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s", 
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler()]
)

# Database operation error handling
try:
    dmv_scores.to_sql('FE_DMV_SCORES', con=gcp_engine, ...)
    logger.info("FE_DMV_SCORES uploaded.")
except SQLAlchemyError as e:
    logger.error(f"Error uploading scores: {e}")
    exit(1)

# Execution time tracking
start_time = time.time()
logger.info(f"Done in {(time.time() - start_time) / 60:.2f} mins.")
```

### **Integration Points**
- **Upstream Dependencies**: All 5 signal modules must complete first
  - `FE_OSCILLATORS_SIGNALS` (from gcp_dmv_osc.py)
  - `FE_MOMENTUM_SIGNALS` (from gcp_dmv_mom.py) 
  - `FE_METRICS_SIGNAL` (from gcp_dmv_met.py)
  - `FE_TVV_SIGNALS` (from gcp_dmv_tvv.py)
  - `FE_RATIOS_SIGNALS` (from gcp_dmv_rat.py)
- **Pipeline Position**: **Final step** in DMV workflow (Stage 3.8)
- **Downstream**: Feeds QA systems and trading algorithms

### **Database Schema**
```sql
-- FE_DMV_ALL table (complete signal aggregation)
slug, timestamp, [all_signal_columns], bullish, bearish, neutral

-- FE_DMV_SCORES table (normalized scoring)  
slug, timestamp, Durability_Score, Momentum_Score, Valuation_Score
```

## Usage
```bash
# Manual execution (requires all upstream signals)
python gcp_postgres_sandbox/gcp_dmv_core.py

# Automated via GitHub Actions (DMV workflow)  
# Runs as FINAL step after all other DMV modules
```

## Dependencies
- pandas>=2.2.2 - Data manipulation and aggregation
- numpy>=1.26.4 - Numerical operations for signal counting
- sqlalchemy>=2.0.32 - Database operations with error handling  
- psycopg2-binary>=2.9.9 - PostgreSQL connectivity
- time - Performance monitoring
- logging - Comprehensive progress tracking

## Key Outputs
1. **Signal Aggregation**: 100+ technical indicators merged into unified view
2. **DMV Scoring**: Normalized 0-100 scores for systematic comparison  
3. **Signal Counts**: Bullish/bearish/neutral tallies for quick assessment
4. **Latest Data**: Real-time filtering for current market analysis
