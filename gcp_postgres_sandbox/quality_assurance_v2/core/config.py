"""
Configuration management for CryptoPrism-DB QA system v2.
Handles environment variables, validation, and QA thresholds.
"""

import os
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class QAConfig:
    """
    Centralized configuration management for QA system.
    Handles database connections, API keys, and QA thresholds.
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize QA configuration.
        
        Args:
            config_file: Optional path to custom .env file
        """
        if config_file:
            self.config_file = config_file
        else:
            # Look for .env in project root (three levels up from this file)
            # Path: core/config.py -> quality_assurance_v2 -> gcp_postgres_sandbox -> CryptoPrism-DB
            current_dir = Path(__file__).parent
            self.config_file = str(current_dir.parent.parent.parent / ".env")
        self._load_environment()
        self._validate_required_vars()
        self._set_qa_thresholds()
    
    def _load_environment(self):
        """Load environment variables from .env file if not in GitHub Actions."""
        if not os.getenv("GITHUB_ACTIONS"):
            if os.path.exists(self.config_file):
                load_dotenv(self.config_file, override=True)
                logger.info(f"Environment loaded from {self.config_file}")
            else:
                logger.warning(f"Config file {self.config_file} not found")
        else:
            logger.info("Running in GitHub Actions: Using environment secrets")
    
    def _validate_required_vars(self):
        """Validate all required environment variables are present."""
        required_vars = [
            "DB_HOST", "DB_USER", "DB_PASSWORD", "DB_PORT"
        ]
        
        optional_vars = [
            "GEMINI_API_KEY", "OPENAI_API_KEY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"
        ]
        
        missing_required = []
        missing_optional = []
        
        # Check required variables
        for var in required_vars:
            if not os.getenv(var):
                missing_required.append(var)
                
        # Check optional variables
        for var in optional_vars:
            if not os.getenv(var):
                missing_optional.append(var)
        
        if missing_required:
            error_msg = f"❌ Missing required environment variables: {', '.join(missing_required)}"
            logger.error(error_msg)
            raise SystemExit(error_msg)
        
        if missing_optional:
            logger.warning(f"⚠️ Missing optional variables (some features disabled): {', '.join(missing_optional)}")
        
        logger.info("✅ Environment validation passed")
    
    def _set_qa_thresholds(self):
        """Set default QA thresholds with environment variable overrides."""
        self.thresholds = {
            # Query Performance Thresholds
            'slow_query_threshold': float(os.getenv('QA_SLOW_QUERY_THRESHOLD', '1.0')),  # seconds
            'critical_query_threshold': float(os.getenv('QA_CRITICAL_QUERY_THRESHOLD', '5.0')),  # seconds
            
            # Data Integrity Thresholds
            'null_ratio_low': float(os.getenv('QA_NULL_RATIO_LOW', '0.2')),
            'null_ratio_medium': float(os.getenv('QA_NULL_RATIO_MEDIUM', '0.49')),
            'null_ratio_high': float(os.getenv('QA_NULL_RATIO_HIGH', '0.89')),
            
            # Row Count Validation
            'min_expected_rows': int(os.getenv('QA_MIN_EXPECTED_ROWS', '100')),
            'row_count_deviation_threshold': float(os.getenv('QA_ROW_DEVIATION_THRESHOLD', '0.1')),  # 10%
            
            # Timestamp Validation
            'min_timestamp_range_days': int(os.getenv('QA_MIN_TIMESTAMP_RANGE_DAYS', '4')),
            
            # Index Performance
            'min_index_usage_threshold': float(os.getenv('QA_MIN_INDEX_USAGE', '0.1')),  # 10%
            'index_scan_efficiency_threshold': float(os.getenv('QA_INDEX_EFFICIENCY', '0.8')),  # 80%
        }
    
    @property
    def database_configs(self) -> Dict[str, Dict[str, str]]:
        """Get database configuration for all three databases."""
        base_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        return {
            'dbcp': {**base_config, 'database': os.getenv('DB_NAME', 'dbcp')},
            'cp_ai': {**base_config, 'database': os.getenv('DB_NAME_AI', 'cp_ai')},
            'cp_backtest': {**base_config, 'database': os.getenv('DB_NAME_BT', 'cp_backtest')}
        }
    
    @property
    def notification_config(self) -> Dict[str, Optional[str]]:
        """Get notification system configuration."""
        return {
            'gemini_api_key': os.getenv('GEMINI_API_KEY'),
            'openai_api_key': os.getenv('OPENAI_API_KEY'),
            'telegram_bot_token': os.getenv('TELEGRAM_BOT_TOKEN'),
            'telegram_chat_id': os.getenv('TELEGRAM_CHAT_ID')
        }
    
    @property
    def key_tables(self) -> List[str]:
        """Get list of critical tables for QA monitoring."""
        return [
            'FE_DMV_ALL', 'FE_DMV_SCORES', 'FE_MOMENTUM_SIGNALS', 
            'FE_OSCILLATORS_SIGNALS', 'FE_RATIOS_SIGNALS',
            'FE_METRICS_SIGNAL', 'FE_TVV_SIGNALS',
            '1K_coins_ohlcv', 'crypto_listings_latest_1000'
        ]
    
    @property
    def fe_tables(self) -> List[str]:
        """Get list of FE_ tables that should have matching timestamps."""
        return [
            'FE_DMV_ALL', 'FE_DMV_SCORES', 'FE_MOMENTUM', 'FE_MOMENTUM_SIGNALS',
            'FE_OSCILLATORS', 'FE_OSCILLATORS_SIGNALS', 'FE_RATIOS', 'FE_RATIOS_SIGNALS',
            'FE_METRICS', 'FE_METRICS_SIGNAL', 'FE_TVV', 'FE_TVV_SIGNALS', 'FE_PCT'
        ]
    
    @property
    def critical_columns(self) -> List[str]:
        """Get list of critical columns that should not have high null rates."""
        return ['slug', 'symbol', 'timestamp', 'price', 'market_cap']
    
    def get_connection_string(self, database: str) -> str:
        """
        Get PostgreSQL connection string for specified database.
        
        Args:
            database: Database name ('dbcp', 'cp_ai', or 'cp_backtest')
            
        Returns:
            SQLAlchemy connection string
        """
        if database not in self.database_configs:
            raise ValueError(f"Unknown database: {database}")
        
        config = self.database_configs[database]
        return f"postgresql+pg8000://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    
    def classify_risk_level(self, metric_type: str, value: float) -> str:
        """
        Classify risk level based on metric type and value.
        
        Args:
            metric_type: Type of metric ('null_ratio', 'query_time', etc.)
            value: Metric value
            
        Returns:
            Risk level: 'LOW', 'MEDIUM', 'HIGH', or 'CRITICAL'
        """
        if metric_type == 'null_ratio':
            if value < self.thresholds['null_ratio_low']:
                return 'LOW'
            elif value < self.thresholds['null_ratio_medium']:
                return 'MEDIUM'
            elif value < self.thresholds['null_ratio_high']:
                return 'HIGH'
            else:
                return 'CRITICAL'
        
        elif metric_type == 'query_time':
            if value < self.thresholds['slow_query_threshold']:
                return 'LOW'
            elif value < self.thresholds['critical_query_threshold']:
                return 'MEDIUM'
            else:
                return 'CRITICAL'
        
        else:
            return 'UNKNOWN'
    
    def should_alert(self, risk_level: str) -> bool:
        """Determine if an issue should trigger an alert."""
        alert_levels = os.getenv('QA_ALERT_LEVELS', 'HIGH,CRITICAL').split(',')
        return risk_level in alert_levels
    
    def get_performance_queries(self) -> List[Dict[str, Any]]:
        """Get list of performance test queries based on real-world patterns."""
        return [
            {
                'name': 'fe_dmv_all_latest_signals',
                'description': 'Latest bullish signals from aggregated data',
                'query': '''
                    SELECT slug, timestamp, bullish, bearish, neutral, 
                           Durability_Score, Momentum_Score, Valuation_Score
                    FROM "FE_DMV_ALL" 
                    WHERE timestamp = (SELECT MAX(timestamp) FROM "FE_DMV_ALL")
                      AND bullish > bearish
                    ORDER BY bullish DESC, Momentum_Score DESC
                    LIMIT 20;
                ''',
                'expected_max_time': 2.0
            },
            {
                'name': 'momentum_signals_performance',
                'description': 'High-performance momentum signals query',
                'query': '''
                    SELECT m.slug, m.rsi_14, m.roc_21, o.macd_signal
                    FROM "FE_MOMENTUM_SIGNALS" m
                    JOIN "FE_OSCILLATORS_SIGNALS" o ON m.slug = o.slug AND m.timestamp = o.timestamp
                    WHERE m.timestamp = (SELECT MAX(timestamp) FROM "FE_MOMENTUM_SIGNALS")
                      AND m.rsi_14 > 70
                    ORDER BY m.roc_21 DESC
                    LIMIT 50;
                ''',
                'expected_max_time': 1.5
            },
            {
                'name': 'ohlcv_range_query',
                'description': 'OHLCV data range query with aggregation',
                'query': '''
                    SELECT slug, COUNT(*) as record_count,
                           MIN(timestamp) as first_date,
                           MAX(timestamp) as last_date,
                           AVG(close) as avg_price
                    FROM "1K_coins_ohlcv"
                    WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY slug
                    HAVING COUNT(*) > 20
                    ORDER BY avg_price DESC
                    LIMIT 100;
                ''',
                'expected_max_time': 3.0
            },
            {
                'name': 'cross_table_signal_analysis',
                'description': 'Complex cross-table signal correlation',
                'query': '''
                    SELECT 
                        d.slug,
                        d.bullish as dmv_bullish,
                        m.rsi_14,
                        r.sharpe_ratio,
                        t.obv_signal
                    FROM "FE_DMV_ALL" d
                    LEFT JOIN "FE_MOMENTUM_SIGNALS" m ON d.slug = m.slug AND d.timestamp = m.timestamp
                    LEFT JOIN "FE_RATIOS_SIGNALS" r ON d.slug = r.slug AND d.timestamp = r.timestamp  
                    LEFT JOIN "FE_TVV_SIGNALS" t ON d.slug = t.slug AND d.timestamp = t.timestamp
                    WHERE d.timestamp = (SELECT MAX(timestamp) FROM "FE_DMV_ALL")
                      AND d.bullish > 5
                    ORDER BY d.Momentum_Score DESC
                    LIMIT 25;
                ''',
                'expected_max_time': 2.5
            },
            {
                'name': 'duplicate_detection_query',
                'description': 'Duplicate detection across key tables',
                'query': '''
                    SELECT table_name, duplicate_count FROM (
                        SELECT 'FE_DMV_ALL' as table_name, 
                               COUNT(*) - COUNT(DISTINCT (slug, timestamp)) as duplicate_count
                        FROM "FE_DMV_ALL"
                        UNION ALL
                        SELECT 'FE_MOMENTUM_SIGNALS' as table_name,
                               COUNT(*) - COUNT(DISTINCT (slug, timestamp)) as duplicate_count  
                        FROM "FE_MOMENTUM_SIGNALS"
                    ) duplicates
                    WHERE duplicate_count > 0;
                ''',
                'expected_max_time': 1.0
            }
        ]