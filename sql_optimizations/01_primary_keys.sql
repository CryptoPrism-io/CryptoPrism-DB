-- ============================================================================
-- PRIMARY KEY OPTIMIZATION STRATEGY FOR CRYPTOPRISM-DB
-- ============================================================================
-- 
-- This script creates optimal primary keys for time-series cryptocurrency data
-- 
-- Strategy:
-- 1. Time-series tables: Composite PRIMARY KEY (slug, timestamp)
-- 2. Reference tables: Single PRIMARY KEY (slug) or (id)
-- 3. Market-wide data: PRIMARY KEY (timestamp)
--
-- Benefits:
-- - Ensures data uniqueness and integrity
-- - Creates clustered indexes for fastest access
-- - Enables efficient JOINs between tables
-- - Optimizes GROUP BY slug operations
-- ============================================================================

-- ============================================================================
-- PHASE 1: OHLCV TABLES (Core Price Data)
-- ============================================================================

-- Primary OHLCV table for 1000 coins
ALTER TABLE "1K_coins_ohlcv" 
ADD CONSTRAINT pk_1k_coins_ohlcv 
PRIMARY KEY (slug, timestamp);

-- Historical OHLCV data
ALTER TABLE "108_1K_coins_ohlcv" 
ADD CONSTRAINT pk_108_1k_coins_ohlcv 
PRIMARY KEY (slug, timestamp);

-- Backup OHLCV table
ALTER TABLE "1K_coins_ohlcv_backup" 
ADD CONSTRAINT pk_1k_coins_ohlcv_backup 
PRIMARY KEY (slug, timestamp);

-- ============================================================================
-- PHASE 2: FEATURE ENGINEERING TABLES (Technical Indicators)
-- ============================================================================

-- DMV All-in-one indicators
ALTER TABLE "FE_DMV_ALL" 
ADD CONSTRAINT pk_fe_dmv_all 
PRIMARY KEY (slug, timestamp);

-- DMV Scores
ALTER TABLE "FE_DMV_SCORES" 
ADD CONSTRAINT pk_fe_dmv_scores 
PRIMARY KEY (slug, timestamp);

-- Core Metrics
ALTER TABLE "FE_METRICS" 
ADD CONSTRAINT pk_fe_metrics 
PRIMARY KEY (slug, timestamp);

-- Metrics Signals
ALTER TABLE "FE_METRICS_SIGNAL" 
ADD CONSTRAINT pk_fe_metrics_signal 
PRIMARY KEY (slug, timestamp);

-- Momentum Indicators
ALTER TABLE "FE_MOMENTUM" 
ADD CONSTRAINT pk_fe_momentum 
PRIMARY KEY (slug, timestamp);

-- Momentum Signals
ALTER TABLE "FE_MOMENTUM_SIGNALS" 
ADD CONSTRAINT pk_fe_momentum_signals 
PRIMARY KEY (slug, timestamp);

-- Oscillator Indicators
ALTER TABLE "FE_OSCILLATOR" 
ADD CONSTRAINT pk_fe_oscillator 
PRIMARY KEY (slug, timestamp);

-- Oscillator Signals
ALTER TABLE "FE_OSCILLATORS_SIGNALS" 
ADD CONSTRAINT pk_fe_oscillators_signals 
PRIMARY KEY (slug, timestamp);

-- Percentage Changes
ALTER TABLE "FE_PCT_CHANGE" 
ADD CONSTRAINT pk_fe_pct_change 
PRIMARY KEY (slug, timestamp);

-- Ratios
ALTER TABLE "FE_RATIOS" 
ADD CONSTRAINT pk_fe_ratios 
PRIMARY KEY (slug, timestamp);

-- Ratio Signals
ALTER TABLE "FE_RATIOS_SIGNALS" 
ADD CONSTRAINT pk_fe_ratios_signals 
PRIMARY KEY (slug, timestamp);

-- Trading Volume Variance
ALTER TABLE "FE_TVV" 
ADD CONSTRAINT pk_fe_tvv 
PRIMARY KEY (slug, timestamp);

-- TVV Signals
ALTER TABLE "FE_TVV_SIGNALS" 
ADD CONSTRAINT pk_fe_tvv_signals 
PRIMARY KEY (slug, timestamp);

-- ============================================================================
-- PHASE 3: REFERENCE AND METADATA TABLES
-- ============================================================================

-- Crypto listings (reference data - no timestamp needed)
ALTER TABLE "crypto_listings_latest_1000" 
ADD CONSTRAINT pk_crypto_listings_latest_1000 
PRIMARY KEY (slug);

-- Alternative: If id column is preferred as primary key
-- ALTER TABLE "crypto_listings_latest_1000" 
-- ADD CONSTRAINT pk_crypto_listings_latest_1000 
-- PRIMARY KEY (id);

-- Crypto listings (general)
ALTER TABLE "crypto_listings" 
ADD CONSTRAINT pk_crypto_listings 
PRIMARY KEY (slug);

-- Crypto ratings
ALTER TABLE "crypto_ratings" 
ADD CONSTRAINT pk_crypto_ratings 
PRIMARY KEY (slug);

-- Coin info and URLs (reference data)
ALTER TABLE "FE_CC_INFO_URL" 
ADD CONSTRAINT pk_fe_cc_info_url 
PRIMARY KEY (slug);

-- ============================================================================
-- PHASE 4: MARKET-WIDE DATA (Time-based only)
-- ============================================================================

-- Global market data (one record per timestamp)
ALTER TABLE "crypto_global_latest" 
ADD CONSTRAINT pk_crypto_global_latest 
PRIMARY KEY (timestamp);

-- Fear & Greed Index (one record per timestamp)
ALTER TABLE "FE_FEAR_GREED_CMC" 
ADD CONSTRAINT pk_fe_fear_greed_cmc 
PRIMARY KEY (timestamp);

-- ============================================================================
-- PHASE 5: NEWS AND EVENTS TABLES
-- ============================================================================

-- News Airdrops (assuming slug + timestamp combination)
ALTER TABLE "NEWS_AIRDROPS_W" 
ADD CONSTRAINT pk_news_airdrops_w 
PRIMARY KEY (slug, timestamp);

-- News Tokenomics (assuming slug + timestamp combination)
ALTER TABLE "NEWS_TOKENOMICS_W" 
ADD CONSTRAINT pk_news_tokenomics_w 
PRIMARY KEY (slug, timestamp);

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check all primary keys were created successfully
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE indexname LIKE '%pkey%' OR indexname LIKE 'pk_%'
ORDER BY tablename;

-- Check for any duplicate data that might prevent primary key creation
-- (Run these before executing the above ALTER statements if needed)

/*
-- Example duplicate check for OHLCV table:
SELECT slug, timestamp, COUNT(*) 
FROM "1K_coins_ohlcv" 
GROUP BY slug, timestamp 
HAVING COUNT(*) > 1;

-- Example duplicate check for FE tables:
SELECT slug, timestamp, COUNT(*) 
FROM "FE_DMV_ALL" 
GROUP BY slug, timestamp 
HAVING COUNT(*) > 1;
*/

-- ============================================================================
-- NOTES AND CONSIDERATIONS
-- ============================================================================

/*
IMPLEMENTATION NOTES:

1. BACKUP FIRST: Always backup your database before running these changes

2. DUPLICATE DATA: If any queries find duplicates, clean them first:
   - Decide which duplicate to keep (usually latest by insertion order)
   - Remove duplicates before adding primary keys

3. NULL VALUES: Primary key columns cannot be NULL. Check for NULL values:
   SELECT COUNT(*) FROM table_name WHERE slug IS NULL OR timestamp IS NULL;

4. PERFORMANCE IMPACT: Adding primary keys creates clustered indexes which:
   - May temporarily lock tables during creation
   - Will improve query performance significantly
   - May require table reorganization (VACUUM FULL after creation)

5. ALTERNATIVE APPROACHES:
   - If natural composite keys don't work, consider adding surrogate keys
   - For very large tables, consider adding primary keys during low-usage hours
   - Use CREATE UNIQUE INDEX CONCURRENTLY first, then ALTER TABLE ADD CONSTRAINT

6. MONITORING:
   - Monitor query performance before and after
   - Use EXPLAIN ANALYZE to verify index usage
   - Check pg_stat_user_indexes for index usage statistics
*/