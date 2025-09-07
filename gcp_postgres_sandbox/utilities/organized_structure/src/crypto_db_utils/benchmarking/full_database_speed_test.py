#!/usr/bin/env python3
"""
Full Database Speed Test - Comprehensive baseline and post-optimization testing
Tests all 24 tables across different query patterns
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class FullDatabaseSpeedTest:
    """Comprehensive speed testing across all database tables."""
    
    def __init__(self):
        load_dotenv()
        
        # Database connection parameters
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'), 
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        self.database = os.getenv('DB_NAME', 'dbcp')
        
        # Validate environment variables
        required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise EnvironmentError(f"Missing required environment variables: {missing_vars}")
    
    def create_connection_string(self) -> str:
        """Create PostgreSQL connection string."""
        return (f"postgresql+psycopg2://{self.db_config['user']}:"
                f"{self.db_config['password']}@{self.db_config['host']}:"
                f"{self.db_config['port']}/{self.database}")
    
    def get_comprehensive_test_queries(self) -> Dict[str, Dict[str, Any]]:
        """Get comprehensive test queries covering all table types."""
        week_ago = datetime.now() - timedelta(days=7)
        
        return {
            # FE_DMV_ALL - Primary aggregated table
            "fe_dmv_all_full_scan": {
                "description": "FE_DMV_ALL full table scan with filtering",
                "query": """
                    SELECT slug, timestamp, bullish, bearish, neutral
                    FROM "FE_DMV_ALL"
                    WHERE timestamp >= :start_date
                    LIMIT 1000
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            "fe_dmv_all_symbol_filter": {
                "description": "FE_DMV_ALL symbol-specific filtering",
                "query": """
                    SELECT slug, timestamp, bullish, bearish, m_mom_roc_bin, m_osc_macd_crossover_bin
                    FROM "FE_DMV_ALL"
                    WHERE slug IN ('bitcoin', 'ethereum', 'cardano', 'solana', 'dogecoin')
                    AND timestamp >= :start_date
                    ORDER BY timestamp DESC
                    LIMIT 100
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            "fe_dmv_all_aggregation": {
                "description": "FE_DMV_ALL aggregation query",
                "query": """
                    SELECT slug, 
                           AVG(bullish) as avg_bullish,
                           AVG(bearish) as avg_bearish,
                           COUNT(*) as signal_count
                    FROM "FE_DMV_ALL"
                    WHERE timestamp >= :start_date
                    GROUP BY slug
                    HAVING COUNT(*) >= 5
                    ORDER BY avg_bullish DESC
                    LIMIT 50
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            # Signal tables JOINs
            "multi_signal_join": {
                "description": "JOIN multiple FE_*_SIGNALS tables",
                "query": """
                    SELECT d.slug, d.timestamp, d.bullish,
                           m.m_mom_roc_bin, m."m_mom_williams_%_bin",
                           o.m_osc_macd_crossover_bin, o.m_osc_cci_bin,
                           r.v_rat_sharpe_bin
                    FROM "FE_DMV_ALL" d
                    LEFT JOIN "FE_MOMENTUM_SIGNALS" m ON d.slug = m.slug AND d.timestamp = m.timestamp
                    LEFT JOIN "FE_OSCILLATORS_SIGNALS" o ON d.slug = o.slug AND d.timestamp = o.timestamp
                    LEFT JOIN "FE_RATIOS_SIGNALS" r ON d.slug = r.slug AND d.timestamp = r.timestamp
                    WHERE d.timestamp >= :start_date
                    AND d.slug IN ('bitcoin', 'ethereum', 'cardano')
                    ORDER BY d.timestamp DESC
                    LIMIT 200
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            # OHLCV tables
            "ohlcv_price_analysis": {
                "description": "1K_coins_ohlcv price and volume analysis",
                "query": """
                    SELECT slug, timestamp, close, volume, market_cap,
                           close - LAG(close) OVER (PARTITION BY slug ORDER BY timestamp) as price_change
                    FROM "1K_coins_ohlcv"
                    WHERE timestamp >= :start_date
                    AND slug IN ('bitcoin', 'ethereum', 'binancecoin', 'cardano', 'solana')
                    ORDER BY timestamp DESC, volume DESC
                    LIMIT 500
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            "ohlcv_volume_leaders": {
                "description": "1K_coins_ohlcv volume analysis",
                "query": """
                    SELECT slug, 
                           MAX(volume) as max_volume,
                           AVG(close) as avg_price,
                           COUNT(*) as records
                    FROM "1K_coins_ohlcv"
                    WHERE timestamp >= :start_date
                    GROUP BY slug
                    ORDER BY max_volume DESC
                    LIMIT 100
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            "ohlcv_backup_comparison": {
                "description": "1K_coins_ohlcv vs backup table comparison",
                "query": """
                    SELECT COUNT(*) as main_count,
                           (SELECT COUNT(*) FROM "1K_coins_ohlcv_backup" WHERE timestamp >= :start_date) as backup_count
                    FROM "1K_coins_ohlcv"
                    WHERE timestamp >= :start_date
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            # Crypto reference tables
            "crypto_listings_analysis": {
                "description": "crypto_listings_latest_1000 market analysis",
                "query": """
                    SELECT slug, name, symbol, cmc_rank, market_cap, 
                           percent_change_24h, percent_change_7d
                    FROM "crypto_listings_latest_1000"
                    WHERE market_cap > 1000000
                    ORDER BY market_cap DESC
                    LIMIT 200
                """,
                "params": {}
            },
            
            "crypto_global_metrics": {
                "description": "crypto_global_latest metrics",
                "query": """
                    SELECT timestamp, total_market_cap, total_volume_24h, 
                           bitcoin_percentage_of_market_cap, active_cryptocurrencies
                    FROM "crypto_global_latest"
                    ORDER BY timestamp DESC
                    LIMIT 100
                """,
                "params": {}
            },
            
            # Technical analysis tables
            "momentum_indicators": {
                "description": "FE_MOMENTUM technical indicators",
                "query": """
                    SELECT slug, timestamp, m_mom_rsi_9, m_mom_rsi_18, 
                           m_mom_roc, "m_mom_williams_%", m_mom_smi
                    FROM "FE_MOMENTUM"
                    WHERE timestamp >= :start_date
                    AND slug IN ('bitcoin', 'ethereum')
                    ORDER BY timestamp DESC
                    LIMIT 300
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            "oscillator_analysis": {
                "description": "FE_OSCILLATOR MACD and CCI analysis",
                "query": """
                    SELECT slug, timestamp, "MACD", "Signal", "CCI", "ADX"
                    FROM "FE_OSCILLATOR"
                    WHERE timestamp >= :start_date
                    AND "MACD" IS NOT NULL
                    ORDER BY timestamp DESC
                    LIMIT 400
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            # News tables (different date column)
            "news_tokenomics": {
                "description": "NEWS_TOKENOMICS_W recent events",
                "query": """
                    SELECT slug, title, event_date, source, market_cap, 
                           percent_change24h, percent_change7d
                    FROM "NEWS_TOKENOMICS_W"
                    WHERE event_date >= :start_date
                    ORDER BY event_date DESC
                    LIMIT 100
                """,
                "params": {"start_date": str(week_ago.date())}
            },
            
            "news_airdrops": {
                "description": "NEWS_AIRDROPS_W analysis",
                "query": """
                    SELECT slug, symbol, title, event_date, cmc_rank
                    FROM "NEWS_AIRDROPS_W"
                    WHERE event_date >= :start_date
                    ORDER BY cmc_rank ASC, event_date DESC
                    LIMIT 50
                """,
                "params": {"start_date": str(week_ago.date())}
            },
            
            # Cross-table analytical queries
            "price_vs_signals": {
                "description": "Price data vs signals correlation",
                "query": """
                    SELECT p.slug, p.timestamp, p.close, p.volume,
                           d.bullish, d.bearish, d.m_mom_roc_bin
                    FROM "1K_coins_ohlcv" p
                    JOIN "FE_DMV_ALL" d ON p.slug = d.slug AND p.timestamp = d.timestamp
                    WHERE p.timestamp >= :start_date
                    AND p.slug IN ('bitcoin', 'ethereum', 'cardano')
                    ORDER BY p.timestamp DESC, p.volume DESC
                    LIMIT 300
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            "market_overview": {
                "description": "Complete market overview query",
                "query": """
                    SELECT l.slug, l.name, l.market_cap, l.percent_change_24h,
                           d.bullish, d.bearish, d.neutral,
                           p.close as latest_price, p.volume as latest_volume
                    FROM "crypto_listings_latest_1000" l
                    LEFT JOIN "FE_DMV_ALL" d ON l.slug = d.slug
                    LEFT JOIN "1K_coins_ohlcv" p ON l.slug = p.slug
                    WHERE l.market_cap > 100000000
                    AND d.timestamp >= :start_date
                    AND p.timestamp >= :start_date
                    ORDER BY l.market_cap DESC
                    LIMIT 100
                """,
                "params": {"start_date": str(week_ago)}
            },
            
            # Large aggregation queries
            "signals_summary": {
                "description": "Cross-table signals summary",
                "query": """
                    SELECT 
                        COUNT(DISTINCT d.slug) as total_coins,
                        AVG(d.bullish) as avg_bullish,
                        AVG(d.bearish) as avg_bearish,
                        COUNT(*) as total_signals
                    FROM "FE_DMV_ALL" d
                    WHERE d.timestamp >= :start_date
                """,
                "params": {"start_date": str(week_ago)}
            }
        }
    
    def execute_query(self, engine, query_name: str, query_info: Dict[str, Any], runs: int = 3) -> Dict[str, Any]:
        """Execute a single query multiple times and collect timing data."""
        query = query_info["query"]
        params = query_info.get("params", {})
        description = query_info.get("description", "")
        
        execution_times = []
        row_counts = []
        errors = []
        
        for run in range(runs):
            try:
                start_time = time.time()
                with engine.connect() as conn:
                    result = conn.execute(text(query), params)
                    rows = result.fetchall()
                    end_time = time.time()
                    
                    execution_time = end_time - start_time
                    execution_times.append(execution_time)
                    row_counts.append(len(rows))
                    
            except Exception as e:
                logger.warning(f"Query {query_name} execution failed: {e}")
                errors.append(str(e))
                continue
        
        if not execution_times:
            logger.error(f"All executions failed for {query_name}")
            return {
                "query_name": query_name,
                "description": description,
                "status": "failed",
                "errors": errors
            }
        
        # Calculate statistics
        avg_time = sum(execution_times) / len(execution_times)
        min_time = min(execution_times)
        max_time = max(execution_times)
        avg_rows = sum(row_counts) / len(row_counts) if row_counts else 0
        
        logger.info(f"OK {query_name}: {avg_time*1000:.2f}ms avg ({len(execution_times)} runs)")
        
        return {
            "query_name": query_name,
            "description": description,
            "status": "success",
            "execution_times": execution_times,
            "avg_execution_time": avg_time,
            "min_execution_time": min_time,
            "max_execution_time": max_time,
            "avg_rows_returned": avg_rows,
            "total_runs": len(execution_times),
            "errors": errors
        }
    
    def run_full_speed_test(self) -> Dict[str, Any]:
        """Run comprehensive speed test across all tables."""
        logger.info(f"Starting full database speed test for {self.database}")
        
        try:
            # Create database connection
            conn_string = self.create_connection_string()
            engine = create_engine(conn_string)
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            queries = self.get_comprehensive_test_queries()
            results = {
                "database_name": self.database,
                "test_timestamp": datetime.now().isoformat(),
                "total_queries": len(queries),
                "query_results": {}
            }
            
            # Execute each query
            for i, (query_name, query_info) in enumerate(queries.items(), 1):
                logger.info(f"[{i}/{len(queries)}] Testing: {query_name}")
                result = self.execute_query(engine, query_name, query_info)
                results["query_results"][query_name] = result
            
            # Calculate summary statistics
            successful_queries = [r for r in results["query_results"].values() if r["status"] == "success"]
            if successful_queries:
                avg_times = [r["avg_execution_time"] for r in successful_queries]
                results["summary"] = {
                    "successful_queries": len(successful_queries),
                    "failed_queries": len(queries) - len(successful_queries),
                    "overall_avg_time": sum(avg_times) / len(avg_times),
                    "fastest_query": min(avg_times),
                    "slowest_query": max(avg_times),
                    "total_test_time": sum(avg_times)
                }
            
            engine.dispose()
            return results
            
        except Exception as e:
            logger.error(f"Speed test failed: {e}")
            return {
                "database_name": self.database,
                "test_timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
    
    def save_results(self, results: Dict[str, Any], output_dir: str = "database_analysis") -> str:
        """Save speed test results to JSON file."""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"full_database_speed_test_{timestamp}.json"
        filepath = output_path / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Speed test results saved to: {filepath}")
        return str(filepath)
    
    def generate_summary_report(self, results: Dict[str, Any]) -> str:
        """Generate human-readable speed test report."""
        report = []
        report.append("=" * 80)
        report.append("FULL DATABASE SPEED TEST RESULTS")
        report.append("=" * 80)
        report.append(f"Database: {results['database_name']}")
        report.append(f"Test Time: {results['test_timestamp']}")
        report.append("")
        
        if "error" in results:
            report.append(f"ERROR: {results['error']}")
            return "\n".join(report)
        
        if "summary" in results:
            summary = results["summary"]
            report.append("OVERALL SUMMARY:")
            report.append(f"  Successful queries: {summary['successful_queries']}")
            report.append(f"  Failed queries: {summary['failed_queries']}")
            report.append(f"  Average execution time: {summary['overall_avg_time']*1000:.2f}ms")
            report.append(f"  Fastest query: {summary['fastest_query']*1000:.2f}ms")
            report.append(f"  Slowest query: {summary['slowest_query']*1000:.2f}ms")
            report.append(f"  Total test time: {summary['total_test_time']:.2f}s")
            report.append("")
        
        # Group queries by category
        query_categories = {
            "FE_DMV_ALL Queries": [],
            "Signal JOINs": [],
            "OHLCV Queries": [],
            "Reference Tables": [],
            "Technical Analysis": [],
            "News Tables": [],
            "Cross-table Analytics": []
        }
        
        for query_name, result in results["query_results"].items():
            if "fe_dmv_all" in query_name:
                category = "FE_DMV_ALL Queries"
            elif "join" in query_name or "signals" in query_name:
                category = "Signal JOINs"
            elif "ohlcv" in query_name:
                category = "OHLCV Queries"
            elif "crypto_" in query_name:
                category = "Reference Tables"
            elif "momentum" in query_name or "oscillator" in query_name:
                category = "Technical Analysis"
            elif "news" in query_name:
                category = "News Tables"
            else:
                category = "Cross-table Analytics"
            
            query_categories[category].append((query_name, result))
        
        # Report by category
        for category, queries in query_categories.items():
            if queries:
                report.append(f"{category}:")
                for query_name, result in queries:
                    if result["status"] == "success":
                        report.append(f"  OK {query_name}: {result['avg_execution_time']*1000:.2f}ms "
                                    f"({result['avg_rows_returned']:.0f} rows)")
                    else:
                        report.append(f"  ERROR {query_name}: FAILED")
                report.append("")
        
        return "\n".join(report)


def main():
    """Main execution function."""
    try:
        tester = FullDatabaseSpeedTest()
        
        print("=" * 80)
        print("STARTING COMPREHENSIVE DATABASE SPEED TEST")
        print("=" * 80)
        print("This will test all 24 tables across multiple query patterns...")
        print()
        
        # Run full speed test
        results = tester.run_full_speed_test()
        
        # Save results
        filepath = tester.save_results(results)
        
        # Generate and display report
        report = tester.generate_summary_report(results)
        print(report)
        
        logger.info(f"Full database speed test completed! Results: {filepath}")
        return filepath
        
    except Exception as e:
        logger.error(f"Full database speed test failed: {e}")
        return None

if __name__ == "__main__":
    main()