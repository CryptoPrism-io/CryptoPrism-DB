#!/usr/bin/env python3
"""
Simple Query Benchmarker - Working with actual table structure
"""

import os
import json
import logging
import time
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

class SimpleBenchmarker:
    """Simple benchmarker with working queries."""
    
    def __init__(self):
        load_dotenv()
        
        # Database connection parameters
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'), 
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # Database name
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
    
    def get_simple_queries(self) -> Dict[str, Dict[str, Any]]:
        """Get working queries based on actual table structure."""
        week_ago = datetime.now() - timedelta(days=7)
        
        return {
            "simple_dmv_select": {
                "description": "Basic FE_DMV_ALL table scan",
                "query": """
                    SELECT slug, timestamp, id, name
                    FROM "FE_DMV_ALL"
                    WHERE timestamp >= :start_date
                    LIMIT 1000
                """,
                "params": {"start_date": str(week_ago)}
            },
            "simple_momentum_select": {
                "description": "Basic FE_MOMENTUM_SIGNALS table scan",
                "query": """
                    SELECT slug, timestamp, m_mom_roc_bin, "m_mom_williams_%_bin"
                    FROM "FE_MOMENTUM_SIGNALS"
                    WHERE timestamp >= :start_date
                    LIMIT 1000
                """,
                "params": {"start_date": str(week_ago)}
            },
            "simple_join": {
                "description": "Join FE_DMV_ALL with FE_MOMENTUM_SIGNALS",
                "query": """
                    SELECT f.slug, f.timestamp, f.id, 
                           m.m_mom_roc_bin, m."m_mom_williams_%_bin"
                    FROM "FE_DMV_ALL" f 
                    JOIN "FE_MOMENTUM_SIGNALS" m ON f.slug = m.slug AND f.timestamp = m.timestamp
                    WHERE f.timestamp >= :start_date
                    LIMIT 1000
                """,
                "params": {"start_date": str(week_ago)}
            },
            "dmv_count": {
                "description": "Count records in FE_DMV_ALL",
                "query": """
                    SELECT COUNT(*) as record_count
                    FROM "FE_DMV_ALL"
                    WHERE timestamp >= :start_date
                """,
                "params": {"start_date": str(week_ago)}
            },
            "slug_filter": {
                "description": "Filter by specific cryptocurrency slugs",
                "query": """
                    SELECT slug, timestamp, id, name
                    FROM "FE_DMV_ALL"
                    WHERE slug IN ('bitcoin', 'ethereum', 'cardano', 'solana', 'dogecoin')
                    AND timestamp >= :start_date
                    ORDER BY timestamp DESC
                    LIMIT 100
                """,
                "params": {"start_date": str(week_ago)}
            },
            "timestamp_range": {
                "description": "Date range query on FE_DMV_ALL",
                "query": """
                    SELECT slug, timestamp, id
                    FROM "FE_DMV_ALL"
                    WHERE timestamp BETWEEN :start_date AND :end_date
                    ORDER BY timestamp DESC
                    LIMIT 1000
                """,
                "params": {
                    "start_date": str(week_ago - timedelta(days=3)),
                    "end_date": str(week_ago)
                }
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
                logger.warning(f"Query execution failed: {e}")
                errors.append(str(e))
                continue
        
        if not execution_times:
            logger.error(f"❌ {query_name}: All executions failed")
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
        
        logger.info(f"OK {query_name}: {avg_time*1000:.2f}ms avg")
        
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
    
    def run_benchmark_suite(self) -> Dict[str, Any]:
        """Run complete benchmark suite."""
        logger.info(f"Starting simple benchmark for {self.database}")
        
        try:
            # Create database connection
            conn_string = self.create_connection_string()
            engine = create_engine(conn_string)
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            queries = self.get_simple_queries()
            results = {
                "database_name": self.database,
                "benchmark_timestamp": datetime.now().isoformat(),
                "total_queries": len(queries),
                "query_results": {}
            }
            
            # Execute each query
            for query_name, query_info in queries.items():
                logger.info(f"Benchmarking query: {query_name}")
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
                    "slowest_query": max(avg_times)
                }
            
            engine.dispose()
            return results
            
        except Exception as e:
            logger.error(f"Benchmark failed: {e}")
            return {
                "database_name": self.database,
                "benchmark_timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
    
    def save_results(self, results: Dict[str, Any], output_dir: str = "database_analysis") -> str:
        """Save benchmark results to JSON file."""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"simple_benchmark_results_{timestamp}.json"
        filepath = output_path / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Benchmark results saved to: {filepath}")
        return str(filepath)
    
    def generate_report(self, results: Dict[str, Any]) -> str:
        """Generate human-readable benchmark report."""
        report = []
        report.append("=" * 60)
        report.append("SIMPLE DATABASE BENCHMARK RESULTS")
        report.append("=" * 60)
        report.append(f"Database: {results['database_name']}")
        report.append(f"Timestamp: {results['benchmark_timestamp']}")
        report.append("")
        
        if "error" in results:
            report.append(f"❌ Benchmark failed: {results['error']}")
            return "\n".join(report)
        
        if "summary" in results:
            summary = results["summary"]
            report.append("SUMMARY:")
            report.append(f"   Successful queries: {summary['successful_queries']}")
            report.append(f"   Failed queries: {summary['failed_queries']}")
            report.append(f"   Average execution time: {summary['overall_avg_time']*1000:.2f}ms")
            report.append(f"   Fastest query: {summary['fastest_query']*1000:.2f}ms")
            report.append(f"   Slowest query: {summary['slowest_query']*1000:.2f}ms")
            report.append("")
        
        report.append("QUERY DETAILS:")
        for query_name, result in results["query_results"].items():
            if result["status"] == "success":
                report.append(f"   OK {query_name}:")
                report.append(f"      Description: {result['description']}")
                report.append(f"      Average time: {result['avg_execution_time']*1000:.2f}ms")
                report.append(f"      Rows returned: {result['avg_rows_returned']:.0f}")
            else:
                report.append(f"   ERROR {query_name}: FAILED")
                if result["errors"]:
                    report.append(f"      Error: {result['errors'][0][:100]}...")
            report.append("")
        
        return "\n".join(report)


def main():
    """Main execution function."""
    try:
        benchmarker = SimpleBenchmarker()
        results = benchmarker.run_benchmark_suite()
        
        # Save results
        filepath = benchmarker.save_results(results)
        
        # Generate and display report
        report = benchmarker.generate_report(results)
        print(report)
        
        logger.info("Simple benchmark completed successfully!")
        return filepath
        
    except Exception as e:
        logger.error(f"Simple benchmark failed: {e}")
        return None

if __name__ == "__main__":
    main()