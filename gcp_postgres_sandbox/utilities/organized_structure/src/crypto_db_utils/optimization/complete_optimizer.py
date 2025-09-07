#!/usr/bin/env python3
"""
Complete Database Optimizer - Apply primary keys and indexes to all 24 tables
Handles different table patterns and column structures across the entire database
"""

import os
import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class CompleteDatabaseOptimizer:
    """Complete database optimization for all tables."""
    
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
    
    def get_table_columns(self, engine, table_name: str) -> List[str]:
        """Get column names for a table."""
        try:
            inspector = inspect(engine)
            columns = inspector.get_columns(table_name)
            return [col['name'] for col in columns]
        except Exception as e:
            logger.warning(f"Could not get columns for {table_name}: {e}")
            return []
    
    def check_existing_constraint(self, engine, table_name: str, constraint_name: str) -> bool:
        """Check if a constraint already exists."""
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM information_schema.table_constraints 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table_name}'
                    AND constraint_name = '{constraint_name}'
                """))
                return result.scalar() > 0
        except Exception:
            return False
    
    def check_existing_index(self, engine, index_name: str) -> bool:
        """Check if an index already exists."""
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM pg_indexes 
                    WHERE schemaname = 'public' 
                    AND indexname = '{index_name}'
                """))
                return result.scalar() > 0
        except Exception:
            return False
    
    def get_optimization_plan(self) -> Dict[str, Dict[str, Any]]:
        """Get comprehensive optimization plan for all 24 tables."""
        return {
            # FE_* Signal Tables - Primary output tables
            "FE_DMV_ALL": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_fe_dmv_all_timestamp", "(timestamp DESC)"),
                    ("idx_fe_dmv_all_slug", "(slug)"),
                    ("idx_fe_dmv_all_slug_timestamp", "(slug, timestamp DESC)"),
                    ("idx_fe_dmv_all_recent", "(slug, timestamp DESC) WHERE timestamp >= (now() - INTERVAL '7 days')")
                ]
            },
            "FE_MOMENTUM_SIGNALS": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_fe_momentum_signals_timestamp", "(timestamp DESC)"),
                    ("idx_fe_momentum_signals_slug", "(slug)"),
                    ("idx_fe_momentum_signals_slug_timestamp", "(slug, timestamp DESC)")
                ]
            },
            "FE_OSCILLATORS_SIGNALS": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_fe_oscillators_signals_timestamp", "(timestamp DESC)"),
                    ("idx_fe_oscillators_signals_slug", "(slug)"),
                    ("idx_fe_oscillators_signals_slug_timestamp", "(slug, timestamp DESC)")
                ]
            },
            "FE_RATIOS_SIGNALS": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_fe_ratios_signals_timestamp", "(timestamp DESC)"),
                    ("idx_fe_ratios_signals_slug", "(slug)"),
                    ("idx_fe_ratios_signals_slug_timestamp", "(slug, timestamp DESC)")
                ]
            },
            "FE_TVV_SIGNALS": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_fe_tvv_signals_timestamp", "(timestamp DESC)"),
                    ("idx_fe_tvv_signals_slug", "(slug)"),
                    ("idx_fe_tvv_signals_slug_timestamp", "(slug, timestamp DESC)")
                ]
            },
            "FE_METRICS_SIGNAL": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_fe_metrics_signal_timestamp", "(timestamp DESC)"),
                    ("idx_fe_metrics_signal_slug", "(slug)")
                ]
            },
            
            # FE_* Processing Tables - Intermediate calculation tables
            "FE_MOMENTUM": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_fe_momentum_timestamp", "(timestamp DESC)"),
                    ("idx_fe_momentum_slug", "(slug)")
                ]
            },
            "FE_OSCILLATOR": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_fe_oscillator_timestamp", "(timestamp DESC)"),
                    ("idx_fe_oscillator_slug", "(slug)")
                ]
            },
            "FE_TVV": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_fe_tvv_timestamp", "(timestamp DESC)"),
                    ("idx_fe_tvv_slug", "(slug)")
                ]
            },
            "FE_METRICS": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_fe_metrics_timestamp", "(timestamp DESC)"),
                    ("idx_fe_metrics_slug", "(slug)")
                ]
            },
            "FE_RATIOS": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_fe_ratios_timestamp", "(timestamp DESC)"),
                    ("idx_fe_ratios_slug", "(slug)")
                ]
            },
            "FE_PCT_CHANGE": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_fe_pct_change_timestamp", "(timestamp DESC)"),
                    ("idx_fe_pct_change_slug", "(slug)")
                ]
            },
            "FE_DMV_SCORES": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_fe_dmv_scores_timestamp", "(timestamp DESC)"),
                    ("idx_fe_dmv_scores_slug", "(slug)")
                ]
            },
            
            # OHLCV Price Data Tables - Core market data
            "1K_coins_ohlcv": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_1k_coins_ohlcv_timestamp", "(timestamp DESC)"),
                    ("idx_1k_coins_ohlcv_slug", "(slug)"),
                    ("idx_1k_coins_ohlcv_slug_timestamp", "(slug, timestamp DESC)"),
                    ("idx_1k_coins_ohlcv_volume", "(volume DESC) WHERE volume > 1000"),
                    ("idx_1k_coins_ohlcv_recent", "(slug, timestamp DESC) WHERE timestamp >= (now() - INTERVAL '30 days')")
                ]
            },
            "108_1K_coins_ohlcv": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_108_1k_coins_ohlcv_timestamp", "(timestamp DESC)"),
                    ("idx_108_1k_coins_ohlcv_slug", "(slug)")
                ]
            },
            "1K_coins_ohlcv_backup": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_1k_coins_ohlcv_backup_timestamp", "(timestamp DESC)"),
                    ("idx_1k_coins_ohlcv_backup_slug", "(slug)")
                ]
            },
            
            # Crypto Reference Tables - Metadata and listings
            "crypto_listings_latest_1000": {
                "primary_key": "(slug)",
                "indexes": [
                    ("idx_crypto_listings_latest_market_cap", "(market_cap DESC)"),
                    ("idx_crypto_listings_latest_cmc_rank", "(cmc_rank ASC)"),
                    ("idx_crypto_listings_latest_symbol", "(symbol)")
                ]
            },
            "crypto_listings": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_crypto_listings_timestamp", "(timestamp DESC)"),
                    ("idx_crypto_listings_slug", "(slug)")
                ]
            },
            "crypto_global_latest": {
                "primary_key": "(id)",  # Assuming it has an id column
                "indexes": [
                    ("idx_crypto_global_latest_timestamp", "(last_updated DESC)"),  # Using last_updated instead of timestamp
                ]
            },
            "crypto_ratings": {
                "primary_key": "(slug, timestamp)",
                "indexes": [
                    ("idx_crypto_ratings_timestamp", "(timestamp DESC)"),
                    ("idx_crypto_ratings_slug", "(slug)")
                ]
            },
            
            # News Tables - Event-based data with event_date
            "NEWS_TOKENOMICS_W": {
                "primary_key": "(slug, event_date)",
                "indexes": [
                    ("idx_news_tokenomics_event_date", "(event_date DESC)"),
                    ("idx_news_tokenomics_slug", "(slug)"),
                    ("idx_news_tokenomics_slug_event_date", "(slug, event_date DESC)")
                ]
            },
            "NEWS_AIRDROPS_W": {
                "primary_key": "(slug, event_date)",
                "indexes": [
                    ("idx_news_airdrops_event_date", "(event_date DESC)"),
                    ("idx_news_airdrops_slug", "(slug)"),
                    ("idx_news_airdrops_cmc_rank", "(cmc_rank ASC)")
                ]
            },
            
            # Special tables
            "FE_CC_INFO_URL": {
                "primary_key": "(slug)",
                "indexes": [
                    ("idx_fe_cc_info_url_name", "(name)"),
                ]
            },
            "FE_FEAR_GREED_CMC": {
                "primary_key": "(timestamp)",
                "indexes": [
                    ("idx_fe_fear_greed_timestamp", "(timestamp DESC)"),
                ]
            }
        }
    
    def apply_primary_key(self, engine, table_name: str, pk_definition: str) -> bool:
        """Apply primary key to a table."""
        constraint_name = f"pk_{table_name.lower().replace('\"', '')}"
        
        # Check if constraint already exists
        if self.check_existing_constraint(engine, table_name, constraint_name):
            logger.info(f"Primary key already exists on {table_name}, skipping...")
            return True
        
        try:
            pk_sql = f'ALTER TABLE "{table_name}" ADD CONSTRAINT "{constraint_name}" PRIMARY KEY {pk_definition};'
            
            start_time = time.time()
            with engine.connect() as conn:
                conn.execute(text(pk_sql))
                conn.commit()
            
            execution_time = time.time() - start_time
            logger.info(f"Primary key added to {table_name} in {execution_time:.2f}s")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add primary key to {table_name}: {e}")
            return False
    
    def apply_index(self, engine, table_name: str, index_name: str, index_definition: str) -> bool:
        """Apply index to a table."""
        # Check if index already exists
        if self.check_existing_index(engine, index_name):
            logger.info(f"Index {index_name} already exists, skipping...")
            return True
        
        try:
            index_sql = f'CREATE INDEX CONCURRENTLY "{index_name}" ON "{table_name}" {index_definition};'
            
            start_time = time.time()
            with engine.connect() as conn:
                conn.execute(text(index_sql))
                conn.commit()
            
            execution_time = time.time() - start_time
            logger.info(f"Index {index_name} created in {execution_time:.2f}s")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create index {index_name}: {e}")
            return False
    
    def optimize_table(self, engine, table_name: str, optimization_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize a single table with primary key and indexes."""
        logger.info(f"Optimizing table: {table_name}")
        
        results = {
            "table_name": table_name,
            "primary_key_added": False,
            "indexes_added": [],
            "indexes_failed": [],
            "total_time": 0
        }
        
        start_time = time.time()
        
        # Apply primary key
        if "primary_key" in optimization_plan:
            pk_success = self.apply_primary_key(engine, table_name, optimization_plan["primary_key"])
            results["primary_key_added"] = pk_success
        
        # Apply indexes
        if "indexes" in optimization_plan:
            for index_name, index_definition in optimization_plan["indexes"]:
                index_success = self.apply_index(engine, table_name, index_name, index_definition)
                
                if index_success:
                    results["indexes_added"].append(index_name)
                else:
                    results["indexes_failed"].append(index_name)
        
        results["total_time"] = time.time() - start_time
        logger.info(f"Table {table_name} optimization completed in {results['total_time']:.2f}s")
        
        return results
    
    def run_analyze(self, engine, table_names: List[str]) -> bool:
        """Run ANALYZE on optimized tables."""
        logger.info("Running ANALYZE to update table statistics...")
        
        try:
            start_time = time.time()
            with engine.connect() as conn:
                # Run ANALYZE on specific tables
                for table_name in table_names:
                    try:
                        conn.execute(text(f'ANALYZE "{table_name}";'))
                        conn.commit()
                        logger.info(f"ANALYZE completed for {table_name}")
                    except Exception as e:
                        logger.warning(f"ANALYZE failed for {table_name}: {e}")
                        continue
                
                # Run global ANALYZE
                conn.execute(text("ANALYZE;"))
                conn.commit()
            
            execution_time = time.time() - start_time
            logger.info(f"ANALYZE completed for all tables in {execution_time:.2f}s")
            return True
            
        except Exception as e:
            logger.error(f"ANALYZE failed: {e}")
            return False
    
    def verify_optimizations(self, engine) -> Dict[str, Any]:
        """Verify that optimizations were applied successfully."""
        logger.info("Verifying optimization results...")
        
        verification = {
            "total_primary_keys": 0,
            "total_indexes": 0,
            "tables_optimized": 0,
            "verification_timestamp": datetime.now().isoformat()
        }
        
        try:
            with engine.connect() as conn:
                # Count primary keys
                pk_result = conn.execute(text("""
                    SELECT COUNT(*) FROM information_schema.table_constraints 
                    WHERE constraint_type = 'PRIMARY KEY' 
                    AND table_schema = 'public'
                """))
                verification["total_primary_keys"] = pk_result.scalar()
                
                # Count indexes
                idx_result = conn.execute(text("""
                    SELECT COUNT(*) FROM pg_indexes 
                    WHERE schemaname = 'public'
                """))
                verification["total_indexes"] = idx_result.scalar()
                
                # Count optimized tables (tables with primary keys)
                tables_result = conn.execute(text("""
                    SELECT COUNT(DISTINCT table_name) FROM information_schema.table_constraints 
                    WHERE constraint_type = 'PRIMARY KEY' 
                    AND table_schema = 'public'
                """))
                verification["tables_optimized"] = tables_result.scalar()
            
            logger.info(f"Verification complete: {verification['total_primary_keys']} PKs, "
                       f"{verification['total_indexes']} indexes, "
                       f"{verification['tables_optimized']} tables optimized")
            
            return verification
            
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return verification
    
    def optimize_all_tables(self) -> Dict[str, Any]:
        """Run complete database optimization on all 24 tables."""
        logger.info("Starting complete database optimization for all 24 tables...")
        
        try:
            # Create database connection
            conn_string = self.create_connection_string()
            engine = create_engine(conn_string)
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            optimization_plan = self.get_optimization_plan()
            
            results = {
                "database_name": self.database,
                "optimization_timestamp": datetime.now().isoformat(),
                "total_tables": len(optimization_plan),
                "table_results": {},
                "summary": {}
            }
            
            # Optimize each table
            total_start_time = time.time()
            successful_tables = []
            
            for i, (table_name, table_plan) in enumerate(optimization_plan.items(), 1):
                logger.info(f"[{i}/{len(optimization_plan)}] Optimizing {table_name}...")
                
                table_result = self.optimize_table(engine, table_name, table_plan)
                results["table_results"][table_name] = table_result
                
                if table_result["primary_key_added"] and table_result["indexes_added"]:
                    successful_tables.append(table_name)
            
            # Run ANALYZE on all optimized tables
            if successful_tables:
                self.run_analyze(engine, successful_tables)
            
            # Verify results
            verification = self.verify_optimizations(engine)
            results["verification"] = verification
            
            # Calculate summary
            total_time = time.time() - total_start_time
            successful_optimizations = len([r for r in results["table_results"].values() 
                                          if r["primary_key_added"]])
            total_indexes_created = sum(len(r["indexes_added"]) for r in results["table_results"].values())
            
            results["summary"] = {
                "total_optimization_time": total_time,
                "successful_table_optimizations": successful_optimizations,
                "total_indexes_created": total_indexes_created,
                "optimization_success_rate": (successful_optimizations / len(optimization_plan)) * 100
            }
            
            logger.info(f"Complete database optimization finished in {total_time:.2f}s")
            logger.info(f"Successfully optimized {successful_optimizations}/{len(optimization_plan)} tables")
            logger.info(f"Created {total_indexes_created} indexes total")
            
            engine.dispose()
            return results
            
        except Exception as e:
            logger.error(f"Complete database optimization failed: {e}")
            return {
                "database_name": self.database,
                "optimization_timestamp": datetime.now().isoformat(),
                "error": str(e)
            }


def main():
    """Main execution function."""
    print("=" * 80)
    print("COMPLETE DATABASE OPTIMIZATION - ALL 24 TABLES")
    print("=" * 80)
    print("This will apply primary keys and strategic indexes to every table...")
    print()
    
    try:
        optimizer = CompleteDatabaseOptimizer()
        
        # Run complete optimization
        results = optimizer.optimize_all_tables()
        
        if "error" in results:
            print(f"ERROR: {results['error']}")
            return False
        
        # Display results
        print("OPTIMIZATION RESULTS:")
        print("=" * 80)
        
        if "summary" in results:
            summary = results["summary"]
            print(f"Total time: {summary['total_optimization_time']:.2f}s")
            print(f"Tables optimized: {summary['successful_table_optimizations']}")
            print(f"Indexes created: {summary['total_indexes_created']}")
            print(f"Success rate: {summary['optimization_success_rate']:.1f}%")
            print()
        
        if "verification" in results:
            verification = results["verification"]
            print(f"Final state:")
            print(f"  Primary keys: {verification['total_primary_keys']}")
            print(f"  Total indexes: {verification['total_indexes']}")
            print(f"  Tables with PKs: {verification['tables_optimized']}")
            print()
        
        # Show per-table results
        print("PER-TABLE RESULTS:")
        for table_name, table_result in results["table_results"].items():
            pk_status = "✓" if table_result["primary_key_added"] else "✗"
            idx_count = len(table_result["indexes_added"])
            time_taken = table_result["total_time"]
            
            print(f"  {pk_status} {table_name}: PK + {idx_count} indexes ({time_taken:.1f}s)")
        
        print("\nDatabase optimization completed successfully!")
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    main()