-- ============================================================================
-- STRATEGIC INDEXES FOR SUPER-FAST TIME-SERIES QUERIES
-- ============================================================================
-- 
-- This script creates optimized indexes for cryptocurrency time-series data
-- focusing on the most common query patterns:
--
-- 1. GROUP BY slug (aggregate by cryptocurrency)
-- 2. JOIN ON timestamp (time-series correlations) 
-- 3. Time range filtering (recent data, historical analysis)
-- 4. Latest data lookups (current prices, signals)
-- 5. Symbol-based searches (ticker symbol lookups)
--
-- Performance Goals:
-- - 10-100x faster GROUP BY slug queries
-- - 5-50x faster timestamp range queries
-- - Near-instant latest data retrieval
-- - Efficient cross-table time-series joins
-- ============================================================================

-- ============================================================================
-- PHASE 1: CORE TIME-SERIES INDEXES (All Time-Series Tables)
-- ============================================================================

-- Template indexes that should be created for each time-series table
-- These will be applied to all OHLCV and FE_* tables

-- Index 1: slug + timestamp (descending) - Perfect for latest data by coin
-- Usage: SELECT * FROM table WHERE slug = 'bitcoin' ORDER BY timestamp DESC LIMIT 1;

-- Index 2: timestamp (descending) + slug - Perfect for time range + grouping
-- Usage: SELECT slug, AVG(close) FROM table WHERE timestamp > '2024-01-01' GROUP BY slug;

-- Index 3: timestamp only (descending) - Perfect for recent data filtering
-- Usage: SELECT * FROM table WHERE timestamp > NOW() - INTERVAL '7 days';

-- ============================================================================
-- OHLCV TABLES INDEXES
-- ============================================================================

-- 1K_coins_ohlcv (Primary OHLCV table)
CREATE INDEX CONCURRENTLY idx_1k_ohlcv_slug_timestamp 
ON "1K_coins_ohlcv" (slug, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_1k_ohlcv_timestamp_slug 
ON "1K_coins_ohlcv" (timestamp DESC, slug);

CREATE INDEX CONCURRENTLY idx_1k_ohlcv_timestamp 
ON "1K_coins_ohlcv" (timestamp DESC);

-- Symbol-based lookups for trading pairs
CREATE INDEX CONCURRENTLY idx_1k_ohlcv_symbol_timestamp 
ON "1K_coins_ohlcv" (symbol, timestamp DESC);

-- Price-based queries (high-performance trading)
CREATE INDEX CONCURRENTLY idx_1k_ohlcv_close_timestamp 
ON "1K_coins_ohlcv" (close, timestamp DESC);

-- Volume analysis
CREATE INDEX CONCURRENTLY idx_1k_ohlcv_volume_timestamp 
ON "1K_coins_ohlcv" (volume DESC, timestamp DESC);

-- 108_1K_coins_ohlcv (Historical data)
CREATE INDEX CONCURRENTLY idx_108_ohlcv_slug_timestamp 
ON "108_1K_coins_ohlcv" (slug, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_108_ohlcv_timestamp_slug 
ON "108_1K_coins_ohlcv" (timestamp DESC, slug);

CREATE INDEX CONCURRENTLY idx_108_ohlcv_timestamp 
ON "108_1K_coins_ohlcv" (timestamp DESC);

-- ============================================================================
-- FEATURE ENGINEERING TABLES INDEXES
-- ============================================================================

-- FE_DMV_ALL (All-in-one DMV indicators)
CREATE INDEX CONCURRENTLY idx_fe_dmv_all_slug_timestamp 
ON "FE_DMV_ALL" (slug, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_fe_dmv_all_timestamp_slug 
ON "FE_DMV_ALL" (timestamp DESC, slug);

-- Signal aggregation (bullish/bearish/neutral counts)
CREATE INDEX CONCURRENTLY idx_fe_dmv_all_signals 
ON "FE_DMV_ALL" (timestamp DESC, bullish, bearish, neutral);

-- FE_DMV_SCORES (Durability, Momentum, Valuation)
CREATE INDEX CONCURRENTLY idx_fe_dmv_scores_slug_timestamp 
ON "FE_DMV_SCORES" (slug, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_fe_dmv_scores_timestamp_slug 
ON "FE_DMV_SCORES" (timestamp DESC, slug);

-- Score-based filtering
CREATE INDEX CONCURRENTLY idx_fe_dmv_scores_durability 
ON "FE_DMV_SCORES" (Durability_Score DESC, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_fe_dmv_scores_momentum 
ON "FE_DMV_SCORES" (Momentum_Score DESC, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_fe_dmv_scores_valuation 
ON "FE_DMV_SCORES" (Valuation_Score DESC, timestamp DESC);

-- FE_METRICS (Core metrics)
CREATE INDEX CONCURRENTLY idx_fe_metrics_slug_timestamp 
ON "FE_METRICS" (slug, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_fe_metrics_timestamp_slug 
ON "FE_METRICS" (timestamp DESC, slug);

-- Percentage change analysis
CREATE INDEX CONCURRENTLY idx_fe_metrics_pct_1d 
ON "FE_METRICS" (m_pct_1d DESC, timestamp DESC);

-- ATH/ATL analysis
CREATE INDEX CONCURRENTLY idx_fe_metrics_ath_date 
ON "FE_METRICS" (ath_date DESC, slug);

-- FE_MOMENTUM (Momentum indicators)
CREATE INDEX CONCURRENTLY idx_fe_momentum_slug_timestamp 
ON "FE_MOMENTUM" (slug, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_fe_momentum_timestamp_slug 
ON "FE_MOMENTUM" (timestamp DESC, slug);

-- FE_OSCILLATOR (Oscillator indicators)
CREATE INDEX CONCURRENTLY idx_fe_oscillator_slug_timestamp 
ON "FE_OSCILLATOR" (slug, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_fe_oscillator_timestamp_slug 
ON "FE_OSCILLATOR" (timestamp DESC, slug);

-- FE_RATIOS (Financial ratios)
CREATE INDEX CONCURRENTLY idx_fe_ratios_slug_timestamp 
ON "FE_RATIOS" (slug, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_fe_ratios_timestamp_slug 
ON "FE_RATIOS" (timestamp DESC, slug);

-- FE_TVV (Trading Volume Variance)
CREATE INDEX CONCURRENTLY idx_fe_tvv_slug_timestamp 
ON "FE_TVV" (slug, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_fe_tvv_timestamp_slug 
ON "FE_TVV" (timestamp DESC, slug);

-- ============================================================================
-- PHASE 2: HOT DATA PARTIAL INDEXES (Recent Data Fast Access)
-- ============================================================================

-- Recent data (last 30 days) for ultra-fast access to current market conditions
-- These partial indexes are smaller and faster than full table indexes

-- Last 30 days OHLCV data
CREATE INDEX CONCURRENTLY idx_1k_ohlcv_recent_30d 
ON "1K_coins_ohlcv" (slug, timestamp DESC, close, volume)
WHERE timestamp > NOW() - INTERVAL '30 days';

-- Last 7 days for real-time trading
CREATE INDEX CONCURRENTLY idx_1k_ohlcv_recent_7d 
ON "1K_coins_ohlcv" (slug, timestamp DESC, close, volume)
WHERE timestamp > NOW() - INTERVAL '7 days';

-- Last 24 hours for intraday analysis
CREATE INDEX CONCURRENTLY idx_1k_ohlcv_recent_24h 
ON "1K_coins_ohlcv" (slug, timestamp DESC, close, volume)
WHERE timestamp > NOW() - INTERVAL '24 hours';

-- Recent DMV signals (last 7 days)
CREATE INDEX CONCURRENTLY idx_fe_dmv_all_recent_7d 
ON "FE_DMV_ALL" (slug, timestamp DESC, bullish, bearish, neutral)
WHERE timestamp > NOW() - INTERVAL '7 days';

-- Recent scores for live dashboard
CREATE INDEX CONCURRENTLY idx_fe_dmv_scores_recent_7d 
ON "FE_DMV_SCORES" (slug, timestamp DESC, Durability_Score, Momentum_Score, Valuation_Score)
WHERE timestamp > NOW() - INTERVAL '7 days';

-- ============================================================================
-- PHASE 3: REFERENCE TABLE INDEXES
-- ============================================================================

-- crypto_listings_latest_1000 (Coin metadata)
CREATE INDEX CONCURRENTLY idx_crypto_listings_symbol 
ON "crypto_listings_latest_1000" (symbol);

CREATE INDEX CONCURRENTLY idx_crypto_listings_name 
ON "crypto_listings_latest_1000" (name);

CREATE INDEX CONCURRENTLY idx_crypto_listings_cmc_rank 
ON "crypto_listings_latest_1000" (cmc_rank);

-- FE_CC_INFO_URL (Coin information)
CREATE INDEX CONCURRENTLY idx_cc_info_name 
ON "FE_CC_INFO_URL" (name);

-- ============================================================================
-- PHASE 4: MARKET-WIDE DATA INDEXES
-- ============================================================================

-- crypto_global_latest (Global market metrics)
-- Already has PRIMARY KEY (timestamp), additional indexes for specific metrics

-- FE_FEAR_GREED_CMC (Market sentiment)
CREATE INDEX CONCURRENTLY idx_fear_greed_sentiment 
ON "FE_FEAR_GREED_CMC" (sentiment, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_fear_greed_index 
ON "FE_FEAR_GREED_CMC" (fear_greed_index, timestamp DESC);

-- ============================================================================
-- PHASE 5: SPECIALIZED QUERY OPTIMIZATION INDEXES
-- ============================================================================

-- Cross-table correlation analysis (JOIN optimization)
-- These indexes optimize common JOIN patterns between tables

-- OHLCV + DMV correlation
CREATE INDEX CONCURRENTLY idx_correlation_ohlcv_dmv 
ON "1K_coins_ohlcv" (slug, timestamp DESC, close)
WHERE timestamp > '2024-01-01';

-- Price + Momentum correlation  
CREATE INDEX CONCURRENTLY idx_correlation_price_momentum 
ON "FE_MOMENTUM" (slug, timestamp DESC)
WHERE timestamp > '2024-01-01';

-- Multi-timeframe analysis (daily, weekly, monthly aggregations)
CREATE INDEX CONCURRENTLY idx_daily_aggregation 
ON "1K_coins_ohlcv" (slug, DATE(timestamp), close, volume);

-- ============================================================================
-- PHASE 6: ADVANCED PERFORMANCE INDEXES
-- ============================================================================

-- Covering indexes for common SELECT patterns (includes commonly selected columns)
-- These indexes can satisfy entire queries without touching the table data

-- OHLCV covering index for price queries
CREATE INDEX CONCURRENTLY idx_ohlcv_covering 
ON "1K_coins_ohlcv" (slug, timestamp DESC) 
INCLUDE (open, high, low, close, volume, market_cap);

-- DMV covering index for signal queries
CREATE INDEX CONCURRENTLY idx_dmv_covering 
ON "FE_DMV_ALL" (slug, timestamp DESC) 
INCLUDE (bullish, bearish, neutral);

-- Scores covering index
CREATE INDEX CONCURRENTLY idx_scores_covering 
ON "FE_DMV_SCORES" (slug, timestamp DESC) 
INCLUDE (Durability_Score, Momentum_Score, Valuation_Score);

-- ============================================================================
-- PHASE 7: MAINTENANCE AND MONITORING INDEXES
-- ============================================================================

-- Statistics collection for query optimization
CREATE INDEX CONCURRENTLY idx_table_stats_ohlcv 
ON "1K_coins_ohlcv" (slug) 
INCLUDE (timestamp);

-- Data quality monitoring
CREATE INDEX CONCURRENTLY idx_data_quality_nulls 
ON "1K_coins_ohlcv" (timestamp DESC) 
WHERE close IS NULL OR volume IS NULL;

-- ============================================================================
-- VERIFICATION AND MONITORING QUERIES
-- ============================================================================

-- Check all indexes were created successfully
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef,
    pg_size_pretty(pg_total_relation_size(indexname::regclass)) as index_size
FROM pg_indexes 
WHERE indexname LIKE 'idx_%'
ORDER BY tablename, indexname;

-- Monitor index usage (run after some query load)
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_tup_read,
    idx_tup_fetch,
    idx_scan
FROM pg_stat_user_indexes 
ORDER BY idx_tup_read DESC;

-- Check for unused indexes (after monitoring period)
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as index_size
FROM pg_stat_user_indexes 
WHERE idx_scan = 0
AND indexname NOT LIKE '%_pkey'
ORDER BY pg_relation_size(indexname::regclass) DESC;

-- ============================================================================
-- PERFORMANCE TESTING QUERIES
-- ============================================================================

-- These queries should run significantly faster after index creation:

/*
-- Test 1: Latest price for specific coin (should use idx_1k_ohlcv_slug_timestamp)
EXPLAIN ANALYZE
SELECT * FROM "1K_coins_ohlcv" 
WHERE slug = 'bitcoin' 
ORDER BY timestamp DESC 
LIMIT 1;

-- Test 2: Average prices by coin in time range (should use idx_1k_ohlcv_timestamp_slug)
EXPLAIN ANALYZE
SELECT slug, AVG(close) as avg_price 
FROM "1K_coins_ohlcv" 
WHERE timestamp > '2024-01-01' 
GROUP BY slug 
ORDER BY avg_price DESC;

-- Test 3: Recent high-volume coins (should use idx_1k_ohlcv_recent_7d)
EXPLAIN ANALYZE
SELECT slug, MAX(volume) as max_volume 
FROM "1K_coins_ohlcv" 
WHERE timestamp > NOW() - INTERVAL '7 days' 
GROUP BY slug 
ORDER BY max_volume DESC 
LIMIT 10;

-- Test 4: Cross-table correlation (OHLCV + DMV)
EXPLAIN ANALYZE
SELECT o.slug, o.close, d.bullish, d.bearish 
FROM "1K_coins_ohlcv" o
JOIN "FE_DMV_ALL" d ON o.slug = d.slug AND o.timestamp = d.timestamp
WHERE o.timestamp > '2024-01-01'
ORDER BY o.timestamp DESC
LIMIT 100;
*/

-- ============================================================================
-- NOTES AND BEST PRACTICES
-- ============================================================================

/*
IMPLEMENTATION NOTES:

1. CONCURRENT CREATION: All indexes use CONCURRENTLY to avoid locking
   - Allows normal operations during index creation
   - Takes longer but doesn't block queries
   - Monitor progress with: SELECT * FROM pg_stat_progress_create_index;

2. INDEX MAINTENANCE:
   - Run ANALYZE after creating indexes
   - Monitor index bloat with pg_stat_user_indexes
   - Consider periodic REINDEX for heavily updated tables

3. QUERY PLAN VERIFICATION:
   - Use EXPLAIN ANALYZE to verify index usage
   - Look for "Index Scan" or "Index Only Scan" in plans
   - "Seq Scan" indicates missing or unused indexes

4. STORAGE CONSIDERATIONS:
   - Indexes require additional disk space (plan for 2-3x table size)
   - More indexes = slower INSERTs (trade-off with query speed)
   - Monitor disk usage with pg_total_relation_size()

5. PERFORMANCE MONITORING:
   - Use pg_stat_statements to identify slow queries
   - Monitor with pg_stat_user_indexes for index efficiency
   - Remove unused indexes to improve INSERT performance

6. CUSTOMIZATION:
   - Adjust time intervals in partial indexes based on data retention
   - Add more covering indexes for frequently accessed column combinations
   - Consider expression indexes for computed values
*/