#!/usr/bin/env python3
"""
Simple QA Script for DBCP Database
Quick health check and basic statistics
"""

import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def load_config():
    """Load and validate environment configuration"""
    if not os.getenv("GITHUB_ACTIONS"):
        load_dotenv()
    
    config = {
        'DB_HOST': os.getenv("DB_HOST"),
        'DB_USER': os.getenv("DB_USER"), 
        'DB_PASSWORD': os.getenv("DB_PASSWORD"),
        'DB_PORT': os.getenv("DB_PORT", "5432")
    }
    
    missing = [k for k, v in config.items() if not v]
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
    
    return config

def quick_qa_check(config, db_name='dbcp'):
    """Quick quality assurance check"""
    engine = create_engine(f'postgresql+pg8000://{config["DB_USER"]}:{config["DB_PASSWORD"]}@{config["DB_HOST"]}:{config["DB_PORT"]}/{db_name}')
    
    key_tables = ['FE_DMV_ALL', 'FE_DMV_SCORES', '1K_coins_ohlcv', 'crypto_listings_latest_1000']
    
    print("DBCP Database Quick QA Check")
    print("=" * 40)
    
    with engine.connect() as conn:
        for table in key_tables:
            try:
                # Check if table exists and get row count
                result = conn.execute(text(f'SELECT COUNT(*) FROM public."{table}"')).scalar()
                print(f"OK {table}: {result:,} rows")
                
                # Check for recent data
                if table in ['1K_coins_ohlcv', 'crypto_listings_latest_1000']:
                    latest = conn.execute(text(f'SELECT MAX(timestamp) FROM public."{table}"')).scalar()
                    if latest:
                        print(f"   Latest data: {latest}")
                        
            except Exception as e:
                print(f"ERROR {table}: {e}")
    
    print("\nQA Check Complete!")

if __name__ == "__main__":
    try:
        config = load_config()
        quick_qa_check(config)
    except Exception as e:
        print(f"ERROR QA Check Failed: {e}")