import pandas as pd
import numpy as np
import logging
from sqlalchemy import create_engine, text
import time

# 🔹 Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 🔹 Database Connection Parameters
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "34.55.195.199"),
    "user": os.getenv("DB_USER", "yogass09"),
    "password": os.getenv("DB_PASSWORD", "jaimaakamakhya"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "dbcp"),
    "database_bt": os.getenv("DB_BT_NAME", "cp_backtest"),
}

# 🔹 Create SQLAlchemy Engine for Backtest Database
def create_db_engine_backtest():
    return create_engine(f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database_bt"]}')

# 🔹 Test 1: Check Overall Data Count and Date Range
def test_data_overview(engine):
    logging.info("🔍 TEST 1: Checking overall data overview...")
    
    queries = {
        "total_rows": "SELECT COUNT(*) as total_rows FROM FE_MOMENTUM",
        "unique_slugs": "SELECT COUNT(DISTINCT slug) as unique_slugs FROM FE_MOMENTUM",
        "date_range": """
            SELECT 
                MIN(timestamp) as earliest_date,
                MAX(timestamp) as latest_date,
                COUNT(DISTINCT DATE(timestamp)) as unique_dates
            FROM FE_MOMENTUM
        """,
        "data_completeness": """
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT slug) as unique_slugs,
                COUNT(DISTINCT DATE(timestamp)) as unique_dates,
                ROUND(COUNT(*)::numeric / COUNT(DISTINCT slug) / COUNT(DISTINCT DATE(timestamp)), 2) as avg_records_per_slug_per_day
            FROM FE_MOMENTUM
        """
    }
    
    results = {}
    for test_name, query in queries.items():
        try:
            with engine.connect() as connection:
                result = pd.read_sql_query(query, connection)
                results[test_name] = result
                logging.info(f"✅ {test_name}: {result.to_dict('records')[0]}")
        except Exception as e:
            logging.error(f"❌ Error in {test_name}: {e}")
    
    return results

# 🔹 Test 2: Check Chronological Order and Gaps
def test_chronological_order(engine):
    logging.info("🔍 TEST 2: Checking chronological order and identifying gaps...")
    
    queries = {
        "timestamp_order_check": """
            WITH ordered_data AS (
                SELECT 
                    slug,
                    timestamp,
                    LAG(timestamp) OVER (PARTITION BY slug ORDER BY timestamp) as prev_timestamp,
                    LEAD(timestamp) OVER (PARTITION BY slug ORDER BY timestamp) as next_timestamp
                FROM FE_MOMENTUM
                ORDER BY slug, timestamp
            )
            SELECT 
                slug,
                COUNT(*) as total_records,
                MIN(timestamp) as first_record,
                MAX(timestamp) as last_record,
                COUNT(CASE WHEN timestamp < prev_timestamp THEN 1 END) as out_of_order_count
            FROM ordered_data
            GROUP BY slug
            HAVING COUNT(CASE WHEN timestamp < prev_timestamp THEN 1 END) > 0
            ORDER BY out_of_order_count DESC
            LIMIT 10
        """,
        
        "date_gaps_analysis": """
            WITH date_series AS (
                SELECT 
                    slug,
                    DATE(timestamp) as date_only,
                    COUNT(*) as records_per_day
                FROM FE_MOMENTUM
                GROUP BY slug, DATE(timestamp)
            ),
            date_ranges AS (
                SELECT 
                    slug,
                    MIN(date_only) as start_date,
                    MAX(date_only) as end_date,
                    COUNT(*) as total_days_with_data,
                    COUNT(CASE WHEN records_per_day = 0 THEN 1 END) as days_without_data
                FROM date_series
                GROUP BY slug
            )
            SELECT 
                slug,
                start_date,
                end_date,
                total_days_with_data,
                (end_date - start_date + 1) as expected_days,
                ((end_date - start_date + 1) - total_days_with_data) as missing_days,
                ROUND((total_days_with_data::numeric / (end_date - start_date + 1)) * 100, 2) as data_coverage_percent
            FROM date_ranges
            ORDER BY missing_days DESC
            LIMIT 20
        """
    }
    
    results = {}
    for test_name, query in queries.items():
        try:
            with engine.connect() as connection:
                result = pd.read_sql_query(query, connection)
                results[test_name] = result
                if len(result) == 0:
                    logging.info(f"✅ {test_name}: No issues found!")
                else:
                    logging.warning(f"⚠️ {test_name}: Found {len(result)} potential issues")
                    logging.info(f"Sample issues: {result.head(3).to_dict('records')}")
        except Exception as e:
            logging.error(f"❌ Error in {test_name}: {e}")
    
    return results

# 🔹 Test 3: Check Data Quality and Completeness
def test_data_quality(engine):
    logging.info("🔍 TEST 3: Checking data quality and completeness...")
    
    queries = {
        "null_values_check": """
            SELECT 
                'FE_MOMENTUM' as table_name,
                COUNT(*) as total_rows,
                COUNT(CASE WHEN id IS NULL THEN 1 END) as null_ids,
                COUNT(CASE WHEN slug IS NULL THEN 1 END) as null_slugs,
                COUNT(CASE WHEN timestamp IS NULL THEN 1 END) as null_timestamps,
                COUNT(CASE WHEN close IS NULL THEN 1 END) as null_close,
                COUNT(CASE WHEN m_mom_rsi_9 IS NULL THEN 1 END) as null_rsi_9,
                COUNT(CASE WHEN m_mom_roc IS NULL THEN 1 END) as null_roc
            FROM FE_MOMENTUM
            UNION ALL
            SELECT 
                'FE_MOMENTUM_SIGNALS' as table_name,
                COUNT(*) as total_rows,
                COUNT(CASE WHEN id IS NULL THEN 1 END) as null_ids,
                COUNT(CASE WHEN slug IS NULL THEN 1 END) as null_slugs,
                COUNT(CASE WHEN timestamp IS NULL THEN 1 END) as null_timestamps,
                0 as null_close,
                0 as null_rsi_9,
                COUNT(CASE WHEN m_mom_roc_bin IS NULL THEN 1 END) as null_roc
            FROM FE_MOMENTUM_SIGNALS
        """,
        
        "duplicate_check": """
            SELECT 
                slug,
                timestamp,
                COUNT(*) as duplicate_count
            FROM FE_MOMENTUM
            GROUP BY slug, timestamp
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
            LIMIT 10
        """,
        
        "indicator_range_check": """
            SELECT 
                'RSI_9' as indicator,
                MIN(m_mom_rsi_9) as min_value,
                MAX(m_mom_rsi_9) as max_value,
                AVG(m_mom_rsi_9) as avg_value,
                COUNT(CASE WHEN m_mom_rsi_9 < 0 OR m_mom_rsi_9 > 100 THEN 1 END) as out_of_range_count
            FROM FE_MOMENTUM
            WHERE m_mom_rsi_9 IS NOT NULL
            UNION ALL
            SELECT 
                'Williams_%R' as indicator,
                MIN(m_mom_williams_%) as min_value,
                MAX(m_mom_williams_%) as max_value,
                AVG(m_mom_williams_%) as avg_value,
                COUNT(CASE WHEN m_mom_williams_% < -100 OR m_mom_williams_% > 0 THEN 1 END) as out_of_range_count
            FROM FE_MOMENTUM
            WHERE m_mom_williams_% IS NOT NULL
        """
    }
    
    results = {}
    for test_name, query in queries.items():
        try:
            with engine.connect() as connection:
                result = pd.read_sql_query(query, connection)
                results[test_name] = result
                logging.info(f"✅ {test_name}: {result.to_dict('records')}")
        except Exception as e:
            logging.error(f"❌ Error in {test_name}: {e}")
    
    return results

# 🔹 Test 4: Sample Detailed Analysis for Specific Slugs
def test_sample_slugs(engine):
    logging.info("🔍 TEST 4: Detailed analysis for sample slugs...")
    
    # Get top 5 slugs by data volume
    sample_query = """
        SELECT slug, COUNT(*) as record_count
        FROM FE_MOMENTUM
        GROUP BY slug
        ORDER BY record_count DESC
        LIMIT 5
    """
    
    try:
        with engine.connect() as connection:
            sample_slugs = pd.read_sql_query(sample_query, connection)
        
        logging.info(f"Sample slugs for detailed analysis: {sample_slugs['slug'].tolist()}")
        
        results = {}
        for slug in sample_slugs['slug'].tolist():
            detailed_query = f"""
                SELECT 
                    slug,
                    MIN(timestamp) as first_record,
                    MAX(timestamp) as last_record,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT DATE(timestamp)) as unique_days,
                    ROUND(AVG(m_mom_rsi_9), 2) as avg_rsi_9,
                    ROUND(AVG(m_mom_roc), 2) as avg_roc,
                    ROUND(AVG(m_mom_williams_%), 2) as avg_williams_r
                FROM FE_MOMENTUM
                WHERE slug = '{slug}'
                GROUP BY slug
            """
            
            try:
                with engine.connect() as connection:
                    result = pd.read_sql_query(detailed_query, connection)
                    results[slug] = result
                    logging.info(f"✅ {slug}: {result.to_dict('records')[0]}")
            except Exception as e:
                logging.error(f"❌ Error analyzing {slug}: {e}")
        
        return results
        
    except Exception as e:
        logging.error(f"❌ Error getting sample slugs: {e}")
        return {}

# 🔹 Test 5: Compare with Source Data
def test_source_comparison(engine):
    logging.info("🔍 TEST 5: Comparing backtest data with source data...")
    
    # Connect to main database for comparison
    engine_main = create_engine(f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}')
    
    comparison_queries = {
        "source_data_overview": """
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT slug) as unique_slugs,
                MIN(timestamp) as earliest_date,
                MAX(timestamp) as latest_date
            FROM "public"."1K_coins_ohlcv"
            INNER JOIN "public"."crypto_listings_latest_1000"
            ON "public"."1K_coins_ohlcv"."slug" = "public"."crypto_listings_latest_1000"."slug"
            WHERE "public"."crypto_listings_latest_1000"."cmc_rank" <= 1000
        """,
        
        "backtest_data_overview": """
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT slug) as unique_slugs,
                MIN(timestamp) as earliest_date,
                MAX(timestamp) as latest_date
            FROM FE_MOMENTUM
        """
    }
    
    results = {}
    try:
        # Get source data stats
        with engine_main.connect() as connection:
            source_stats = pd.read_sql_query(comparison_queries["source_data_overview"], connection)
            logging.info(f"✅ Source data stats: {source_stats.to_dict('records')[0]}")
        
        # Get backtest data stats
        with engine.connect() as connection:
            backtest_stats = pd.read_sql_query(comparison_queries["backtest_data_overview"], connection)
            logging.info(f"✅ Backtest data stats: {backtest_stats.to_dict('records')[0]}")
        
        # Calculate differences
        source_row = source_stats.iloc[0]
        backtest_row = backtest_stats.iloc[0]
        
        logging.info(f"📊 COMPARISON SUMMARY:")
        logging.info(f"   Source rows: {source_row['total_rows']:,}")
        logging.info(f"   Backtest rows: {backtest_row['total_rows']:,}")
        logging.info(f"   Difference: {source_row['total_rows'] - backtest_row['total_rows']:,}")
        logging.info(f"   Source slugs: {source_row['unique_slugs']}")
        logging.info(f"   Backtest slugs: {backtest_row['unique_slugs']}")
        
        results = {
            "source": source_stats,
            "backtest": backtest_stats
        }
        
    except Exception as e:
        logging.error(f"❌ Error in comparison: {e}")
    
    engine_main.dispose()
    return results

# 🔹 Main Execution
if __name__ == "__main__":
    start_time = time.time()
    
    logging.info("🚀 Starting Backtest Momentum Data Validation...")
    
    engine = create_db_engine_backtest()
    
    # Run all tests
    test_results = {}
    
    test_results["overview"] = test_data_overview(engine)
    test_results["chronological"] = test_chronological_order(engine)
    test_results["quality"] = test_data_quality(engine)
    test_results["sample_analysis"] = test_sample_slugs(engine)
    test_results["comparison"] = test_source_comparison(engine)
    
    engine.dispose()
    
    end_time = time.time()
    elapsed_time = (end_time - start_time) / 60
    
    logging.info(f"✅ All tests completed in {elapsed_time:.2f} minutes")
    logging.info("📋 Test Summary:")
    logging.info("   - Data overview and counts")
    logging.info("   - Chronological order validation")
    logging.info("   - Data quality checks")
    logging.info("   - Sample slug analysis")
    logging.info("   - Source vs backtest comparison")
    
    # Save results to file for detailed review
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    filename = f"backtest_mom_validation_{timestamp}.txt"
    
    with open(filename, 'w') as f:
        f.write("BACKTEST MOMENTUM DATA VALIDATION REPORT\n")
        f.write("=" * 50 + "\n")
        f.write(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Duration: {elapsed_time:.2f} minutes\n\n")
        
        for test_name, results in test_results.items():
            f.write(f"\n{test_name.upper()} RESULTS:\n")
            f.write("-" * 30 + "\n")
            for key, value in results.items():
                f.write(f"\n{key}:\n")
                if isinstance(value, pd.DataFrame):
                    f.write(value.to_string())
                else:
                    f.write(str(value))
                f.write("\n")
    
    logging.info(f"📄 Detailed report saved to: {filename}") 