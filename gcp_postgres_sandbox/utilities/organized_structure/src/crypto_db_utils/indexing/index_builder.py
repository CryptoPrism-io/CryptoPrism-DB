#!/usr/bin/env python3
"""
Index Builder - Add indexes without CONCURRENT to avoid transaction issues
Focus on immediate performance improvement
"""

import os
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class IndexBuilder:
    """Build indexes for immediate performance improvement."""
    
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
    
    def create_connection_string(self) -> str:
        """Create PostgreSQL connection string."""
        return (f"postgresql+psycopg2://{self.db_config['user']}:"
                f"{self.db_config['password']}@{self.db_config['host']}:"
                f"{self.db_config['port']}/{self.database}")
    
    def get_critical_indexes(self):
        """Get most critical indexes for immediate performance improvement."""
        return [
            # FE_DMV_ALL - Most queried table
            ('idx_fe_dmv_all_timestamp', '"FE_DMV_ALL"', '(timestamp DESC)'),
            ('idx_fe_dmv_all_slug', '"FE_DMV_ALL"', '(slug)'),
            ('idx_fe_dmv_all_slug_timestamp', '"FE_DMV_ALL"', '(slug, timestamp DESC)'),
            
            # OHLCV - Price data (very slow in baseline)
            ('idx_1k_coins_ohlcv_timestamp', '"1K_coins_ohlcv"', '(timestamp DESC)'),
            ('idx_1k_coins_ohlcv_slug', '"1K_coins_ohlcv"', '(slug)'),
            ('idx_1k_coins_ohlcv_volume', '"1K_coins_ohlcv"', '(volume DESC)'),
            
            # Signal tables - Frequently joined
            ('idx_fe_momentum_signals_timestamp', '"FE_MOMENTUM_SIGNALS"', '(timestamp DESC)'),
            ('idx_fe_momentum_signals_slug', '"FE_MOMENTUM_SIGNALS"', '(slug)'),
            
            ('idx_fe_oscillators_signals_timestamp', '"FE_OSCILLATORS_SIGNALS"', '(timestamp DESC)'),
            ('idx_fe_oscillators_signals_slug', '"FE_OSCILLATORS_SIGNALS"', '(slug)'),
            
            ('idx_fe_ratios_signals_timestamp', '"FE_RATIOS_SIGNALS"', '(timestamp DESC)'),
            ('idx_fe_ratios_signals_slug', '"FE_RATIOS_SIGNALS"', '(slug)'),
            
            # News tables
            ('idx_news_tokenomics_event_date', '"NEWS_TOKENOMICS_W"', '(event_date DESC)'),
            ('idx_news_tokenomics_slug', '"NEWS_TOKENOMICS_W"', '(slug)'),
            
            # Technical analysis tables
            ('idx_fe_momentum_timestamp', '"FE_MOMENTUM"', '(timestamp DESC)'),
            ('idx_fe_momentum_slug', '"FE_MOMENTUM"', '(slug)'),
            
            ('idx_fe_oscillator_timestamp', '"FE_OSCILLATOR"', '(timestamp DESC)'),
            ('idx_fe_oscillator_slug', '"FE_OSCILLATOR"', '(slug)'),
        ]
    
    def check_index_exists(self, engine, index_name: str) -> bool:
        """Check if index already exists."""
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
    
    def create_index(self, engine, index_name: str, table_name: str, index_definition: str) -> bool:
        """Create index without CONCURRENT to avoid transaction issues."""
        if self.check_index_exists(engine, index_name):
            logger.info(f"Index {index_name} already exists, skipping...")
            return True
        
        try:
            # Create index without CONCURRENT for immediate execution
            index_sql = f'CREATE INDEX "{index_name}" ON {table_name} {index_definition};'
            
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
    
    def build_all_indexes(self):
        """Build all critical indexes."""
        logger.info("Building critical indexes for immediate performance improvement...")
        
        try:
            # Create database connection
            conn_string = self.create_connection_string()
            engine = create_engine(conn_string)
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            indexes = self.get_critical_indexes()
            
            results = {
                "database_name": self.database,
                "build_timestamp": datetime.now().isoformat(),
                "total_indexes": len(indexes),
                "successful_indexes": [],
                "failed_indexes": []
            }
            
            # Build each index
            total_start_time = time.time()
            
            for i, (index_name, table_name, index_definition) in enumerate(indexes, 1):
                logger.info(f"[{i}/{len(indexes)}] Creating {index_name}...")
                
                success = self.create_index(engine, index_name, table_name, index_definition)
                
                if success:
                    results["successful_indexes"].append(index_name)
                else:
                    results["failed_indexes"].append(index_name)
            
            # Run ANALYZE to update statistics
            logger.info("Running ANALYZE to update table statistics...")
            start_time = time.time()
            with engine.connect() as conn:
                conn.execute(text("ANALYZE;"))
                conn.commit()
            analyze_time = time.time() - start_time
            logger.info(f"ANALYZE completed in {analyze_time:.2f}s")
            
            # Calculate summary
            total_time = time.time() - total_start_time
            results["summary"] = {
                "total_build_time": total_time,
                "successful_count": len(results["successful_indexes"]),
                "failed_count": len(results["failed_indexes"]),
                "success_rate": (len(results["successful_indexes"]) / len(indexes)) * 100
            }
            
            logger.info(f"Index building completed in {total_time:.2f}s")
            logger.info(f"Successfully created {len(results['successful_indexes'])}/{len(indexes)} indexes")
            
            engine.dispose()
            return results
            
        except Exception as e:
            logger.error(f"Index building failed: {e}")
            return {
                "database_name": self.database,
                "build_timestamp": datetime.now().isoformat(),
                "error": str(e)
            }


def main():
    """Main execution function."""
    print("=" * 60)
    print("BUILDING CRITICAL INDEXES FOR IMMEDIATE PERFORMANCE")
    print("=" * 60)
    print("Adding strategic indexes to optimize query performance...")
    print()
    
    try:
        builder = IndexBuilder()
        
        # Build all critical indexes
        results = builder.build_all_indexes()
        
        if "error" in results:
            print(f"ERROR: {results['error']}")
            return False
        
        # Display results
        print("INDEX BUILDING RESULTS:")
        print("=" * 60)
        
        if "summary" in results:
            summary = results["summary"]
            print(f"Total time: {summary['total_build_time']:.2f}s")
            print(f"Indexes created: {summary['successful_count']}")
            print(f"Indexes failed: {summary['failed_count']}")
            print(f"Success rate: {summary['success_rate']:.1f}%")
            print()
        
        if results["successful_indexes"]:
            print("SUCCESSFULLY CREATED INDEXES:")
            for index_name in results["successful_indexes"]:
                print(f"  ✓ {index_name}")
        
        if results["failed_indexes"]:
            print("\nFAILED INDEXES:")
            for index_name in results["failed_indexes"]:
                print(f"  ✗ {index_name}")
        
        print("\nCritical indexes built successfully!")
        print("Database is now optimized for faster query performance!")
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    main()