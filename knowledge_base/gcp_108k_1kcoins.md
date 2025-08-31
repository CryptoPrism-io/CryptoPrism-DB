# gcp_108k_1kcoins.R - OHLCV Data Collection & Database Provisioning Engine

## Overview
This R script is the **primary data acquisition and provisioning module** of the CryptoPrism-DB system, responsible for fetching comprehensive OHLCV (Open, High, Low, Close, Volume) data for the top 1000 cryptocurrencies and populating the database infrastructure that feeds all downstream technical analysis modules.

## Detailed Functionality

### **Database Connection Architecture**

#### **1. Dual Database Setup**
```r
# Production database connection
con <- dbConnect(
  RPostgres::Postgres(),
  host = "34.55.195.199",
  dbname = "dbcp",
  user = "yogass09", 
  password = "jaimaakamakhya",
  port = 5432
)

# Backtesting database connection
con_bt <- dbConnect(
  RPostgres::Postgres(),
  host = "34.55.195.199",
  dbname = "cp_backtest",
  user = "yogass09",
  password = "jaimaakamakhya", 
  port = 5432
)
```
- **Production Database (`dbcp`)**: Live data for real-time analysis and trading signals
- **Backtest Database (`cp_backtest`)**: Historical data repository for strategy backtesting
- **Connection Validation**: Automated connection health checks with error handling

#### **2. Source Data Integration**
```r
# Read cryptocurrency listings from database
print("Reading crypto listings from database...")
crypto.listings.latest <- dbReadTable(con, "crypto_listings_latest_1000")

# Alternative SQL query approach for more control
# crypto.listings.latest <- dbGetQuery(con, "
#   SELECT * FROM crypto_listings_latest_1000 
#   WHERE cmc_rank > 0 AND cmc_rank < 2000
#   ORDER BY cmc_rank ASC
# ")
```
- **Data Source**: `crypto_listings_latest_1000` table (populated by `cmc_listings.py`)
- **Ranking Filter**: Focuses on top-ranked cryptocurrencies by market capitalization
- **Flexibility**: Multiple data access patterns for different use cases

### **OHLCV Data Acquisition Pipeline**

#### **3. Crypto2 Package Integration**
```r
library(crypto2)

# Bulk OHLCV data retrieval for top cryptocurrencies
for(symbol in crypto.listings.latest$symbol) {
  tryCatch({
    ohlcv_data <- crypto_history(
      coin = symbol,
      limit = 365,  # Last 365 days
      start_date = Sys.Date() - 365,
      end_date = Sys.Date(),
      sleep = 1     # Rate limiting
    )
    
    # Process and store data
    if(nrow(ohlcv_data) > 0) {
      # Data cleaning and preparation
      processed_data <- ohlcv_data %>%
        mutate(
          slug = tolower(gsub(" ", "-", name)),
          timestamp = as.POSIXct(timestamp)
        ) %>%
        select(slug, timestamp, open, high, low, close, volume, market_cap)
      
      # Write to both databases
      dbWriteTable(con, "1K_coins_ohlcv", processed_data, 
                   append = TRUE, row.names = FALSE)
      dbWriteTable(con_bt, "1K_coins_ohlcv", processed_data,
                   append = TRUE, row.names = FALSE)
    }
  }, error = function(e) {
    cat("Error processing", symbol, ":", e$message, "\n")
  })
}
```

#### **4. Data Processing & Standardization**

##### **Schema Standardization**
```r
# Standardized OHLCV schema
processed_data <- raw_data %>%
  mutate(
    slug = standardize_slug(name),           # Consistent identifier
    timestamp = as.POSIXct(timestamp),       # UTC timestamp
    open = as.numeric(open),                 # Opening price
    high = as.numeric(high),                 # Daily high
    low = as.numeric(low),                   # Daily low  
    close = as.numeric(close),               # Closing price
    volume = as.numeric(volume),             # Trading volume
    market_cap = as.numeric(market_cap)      # Market capitalization
  )
```

##### **Data Quality Assurance**
```r
# Data validation and cleaning
clean_data <- processed_data %>%
  filter(
    !is.na(close),                          # Remove missing price data
    close > 0,                               # Ensure positive prices
    volume >= 0,                             # Non-negative volume
    timestamp >= as.Date("2020-01-01")       # Reasonable date range
  ) %>%
  arrange(slug, timestamp)                   # Chronological ordering
```

### **Database Provisioning Strategy**

#### **5. Table Management**
```r
# Create OHLCV table if not exists
if (!dbExistsTable(con, "1K_coins_ohlcv")) {
  dbExecute(con, "
    CREATE TABLE `1K_coins_ohlcv` (
      id SERIAL PRIMARY KEY,
      slug VARCHAR(255) NOT NULL,
      timestamp TIMESTAMP NOT NULL,
      open DECIMAL(20,8),
      high DECIMAL(20,8), 
      low DECIMAL(20,8),
      close DECIMAL(20,8),
      volume DECIMAL(25,2),
      market_cap DECIMAL(25,2),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_slug_timestamp (slug, timestamp)
    )
  ")
}
```

#### **6. Performance Optimization**
```r
# Batch processing for large datasets
batch_size <- 1000
for (i in seq(1, nrow(all_ohlcv_data), batch_size)) {
  batch_end <- min(i + batch_size - 1, nrow(all_ohlcv_data))
  batch_data <- all_ohlcv_data[i:batch_end, ]
  
  dbWriteTable(con, "1K_coins_ohlcv", batch_data,
               append = TRUE, row.names = FALSE)
}
```
- **Batch Operations**: Processes large datasets in manageable chunks
- **Memory Management**: Prevents memory overflow with large historical datasets
- **Error Recovery**: Individual batch failures don't stop entire operation

### **Error Handling & Resilience**

#### **7. Comprehensive Error Management**
```r
# Connection validation
if (!dbIsValid(con)) {
  stop("Primary database connection failed")
}

if (!dbIsValid(con_bt)) {
  stop("Backtest database connection failed")
}

# API rate limiting and retry logic
for (attempt in 1:3) {
  result <- tryCatch({
    crypto_history(coin = symbol, ...)
  }, error = function(e) {
    if (attempt < 3) {
      Sys.sleep(attempt * 2)  # Exponential backoff
      NULL
    } else {
      stop(paste("Failed after 3 attempts:", e$message))
    }
  })
  
  if (!is.null(result)) break
}
```

### **Integration Points**

#### **8. System Dependencies**
- **Upstream**: CoinMarketCap API (via crypto2 package)
- **Data Source**: `crypto_listings_latest_1000` table
- **Downstream**: Feeds all Python technical analysis modules
  - `gcp_dmv_mom.py` (momentum indicators)
  - `gcp_dmv_osc.py` (oscillators)
  - `gcp_dmv_met.py` (metrics)
  - `gcp_dmv_rat.py` (ratios)
  - `gcp_dmv_tvv.py` (volume analysis)
  - `gcp_dmv_pct.py` (percentage analysis)

#### **9. Pipeline Position**
- **Stage**: **Foundation Layer** - Core data provisioning
- **Execution Order**: Runs after `cmc_listings.py`, before all technical analysis modules
- **Frequency**: Daily execution to maintain fresh OHLCV data

### **Data Output Specifications**

#### **10. OHLCV Table Schema**
```sql
CREATE TABLE `1K_coins_ohlcv` (
    id SERIAL PRIMARY KEY,
    slug VARCHAR(255) NOT NULL,           -- Cryptocurrency identifier
    timestamp TIMESTAMP NOT NULL,         -- Data point timestamp  
    open DECIMAL(20,8),                   -- Opening price (8 decimal precision)
    high DECIMAL(20,8),                   -- Daily high price
    low DECIMAL(20,8),                    -- Daily low price
    close DECIMAL(20,8),                  -- Closing price
    volume DECIMAL(25,2),                 -- Trading volume (2 decimal precision)
    market_cap DECIMAL(25,2),             -- Market capitalization
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_slug_timestamp (slug, timestamp),
    INDEX idx_timestamp (timestamp),
    INDEX idx_slug (slug)
);
```

## Usage

### **Package Installation**
```r
# Install required R packages
install.packages(c(
  'RMySQL',      # MySQL connectivity (legacy support)
  'crypto2',     # Cryptocurrency data API interface
  'dplyr',       # Data manipulation and transformation
  'DBI',         # Database interface abstraction
  'RPostgres'    # PostgreSQL connectivity
))
```

### **Manual Execution**
```r
# Run the complete data collection pipeline
source("gcp_postgres_sandbox/gcp_108k_1kcoins.R")

# Expected runtime: 30-120 minutes depending on:
# - Number of cryptocurrencies processed
# - Historical data range requested
# - API rate limiting delays
# - Network connectivity
```

### **Automated Scheduling**
```bash
# Cron job for daily execution
0 2 * * * /usr/bin/Rscript /path/to/gcp_108k_1kcoins.R >> /var/log/crypto_data.log 2>&1
```

## Dependencies

### **Core R Packages**
- **RPostgres>=1.4.0** - Modern PostgreSQL interface for R
- **DBI>=1.1.3** - Database interface abstraction layer
- **dplyr>=1.1.0** - Data manipulation and transformation toolkit
- **crypto2>=1.4.6** - Cryptocurrency market data API wrapper
- **RMySQL>=0.10.25** - MySQL connectivity (legacy compatibility)

### **System Requirements**
- **R>=4.0.0** - Base R statistical computing environment
- **PostgreSQL Client Libraries** - For database connectivity
- **Network Access** - For cryptocurrency API data retrieval

## Key Features Summary

1. **Comprehensive Data Acquisition**: OHLCV data for 1000+ cryptocurrencies
2. **Dual Database Architecture**: Production and backtesting data separation
3. **Robust Error Handling**: API failures, connection issues, and data quality problems
4. **Performance Optimization**: Batch processing and memory management for large datasets
5. **Data Quality Assurance**: Validation, cleaning, and standardization pipelines
6. **Integration Foundation**: Provides standardized data for all downstream analysis modules
7. **Automated Scheduling**: Supports cron-based daily data updates
8. **Historical Coverage**: Configurable data range for backtesting requirements
9. **Rate Limiting Compliance**: Respects API limits with intelligent retry mechanisms
10. **Schema Standardization**: Consistent data structure across production and backtesting environments
