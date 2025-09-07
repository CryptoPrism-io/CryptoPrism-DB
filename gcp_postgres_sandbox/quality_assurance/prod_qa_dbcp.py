#!/usr/bin/env python3
"""
Streamlined Production QA for DBCP Database
- Duplicate cleanup
- Data quality analysis  
- AI-powered issue detection
- Telegram alerts
"""

import os
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import google.generativeai as genai

# Configure logging
logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def load_config():
    """Load and validate environment configuration"""
    if not os.getenv("GITHUB_ACTIONS"):
        load_dotenv()
    
    config = {
        'DB_HOST': os.getenv("DB_HOST"),
        'DB_USER': os.getenv("DB_USER"), 
        'DB_PASSWORD': os.getenv("DB_PASSWORD"),
        'DB_PORT': os.getenv("DB_PORT", "5432"),
        'GEMINI_API_KEY': os.getenv("GEMINI_API_KEY"),
        'TELEGRAM_BOT_TOKEN': os.getenv("TELEGRAM_BOT_TOKEN"),
        'TELEGRAM_CHAT_ID': os.getenv("TELEGRAM_CHAT_ID")
    }
    
    missing = [k for k, v in config.items() if not v and k != 'TELEGRAM_CHAT_ID']
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
    
    return config

def clean_and_analyze_database(config, db_name='dbcp'):
    """Single-pass duplicate cleanup and quality analysis"""
    engine = create_engine(f'postgresql+pg8000://{config["DB_USER"]}:{config["DB_PASSWORD"]}@{config["DB_HOST"]}:{config["DB_PORT"]}/{db_name}')
    
    issues = []
    stats = {'tables_processed': 0, 'duplicates_removed': 0}
    
    with engine.connect() as conn:
        # Get all tables with required columns
        tables_query = text("""
            SELECT t.table_name,
                   array_agg(c.column_name) as columns
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c ON t.table_name = c.table_name 
            WHERE t.table_schema = 'public' 
              AND c.table_schema = 'public'
            GROUP BY t.table_name
        """)
        
        for row in conn.execute(tables_query):
            table_name = row[0]
            columns = row[1] if row[1] else []
            
            try:
                stats['tables_processed'] += 1
                
                # Quick row count
                count_result = conn.execute(text(f'SELECT COUNT(*) FROM public."{table_name}"')).scalar()
                if count_result == 0:
                    continue
                
                # Clean duplicates if possible
                if 'slug' in columns and 'timestamp' in columns:
                    duplicate_query = text(f"""
                        WITH duplicates AS (
                            DELETE FROM public."{table_name}" 
                            WHERE ctid NOT IN (
                                SELECT ctid FROM (
                                    SELECT ctid, ROW_NUMBER() OVER (PARTITION BY slug, timestamp ORDER BY ctid) as rn
                                    FROM public."{table_name}"
                                ) ranked WHERE rn = 1
                            )
                            RETURNING 1
                        )
                        SELECT COUNT(*) FROM duplicates
                    """)
                    duplicates_removed = conn.execute(duplicate_query).scalar() or 0
                    stats['duplicates_removed'] += duplicates_removed
                    
                    if duplicates_removed > 0:
                        conn.execute(text(f'VACUUM ANALYZE public."{table_name}"'))
                
                # Check for critical issues
                timestamp_cols = [c for c in columns if 'timestamp' in c.lower()]
                if timestamp_cols:
                    ts_col = timestamp_cols[0]
                    ts_query = text(f"""
                        SELECT 
                            MIN("{ts_col}") as first_ts,
                            MAX("{ts_col}") as last_ts,
                            COUNT(CASE WHEN "{ts_col}" IS NULL THEN 1 END) as null_count
                        FROM public."{table_name}"
                    """)
                    ts_result = conn.execute(ts_query).fetchone()
                    
                    if ts_result and ts_result[2] > 0:  # null timestamps
                        issues.append(f"❌ {table_name}: {ts_result[2]} NULL timestamps")
                    
                    if ts_result and ts_result[0] and ts_result[1]:
                        first_ts, last_ts = ts_result[0], ts_result[1]
                        
                        # Timestamp validation rules
                        if table_name.startswith('FE_') and first_ts != last_ts:
                            issues.append(f"⚠️ {table_name}: FE table timestamps don't match ({first_ts} != {last_ts})")
                        elif not table_name.startswith('FE_') and (last_ts - first_ts).days < 4:
                            issues.append(f"⚠️ {table_name}: Insufficient timestamp range ({(last_ts - first_ts).days} days)")
                
                # Check for excessive nulls in key columns
                key_columns = ['slug', 'symbol', 'price', 'market_cap']
                for col in key_columns:
                    if col in columns:
                        null_pct = conn.execute(text(f"""
                            SELECT ROUND(COUNT(CASE WHEN "{col}" IS NULL THEN 1 END) * 100.0 / COUNT(*), 1)
                            FROM public."{table_name}"
                        """)).scalar()
                        
                        if null_pct and null_pct > 50:
                            issues.append(f"🔴 {table_name}.{col}: {null_pct}% NULL values")
            
            except Exception as e:
                issues.append(f"❌ {table_name}: Processing error - {str(e)[:50]}")
    
    return issues, stats

def generate_ai_summary(issues, stats, config):
    """Generate concise AI summary of critical issues"""
    if not issues:
        return "✅ No critical issues detected"
    
    genai.configure(api_key=config['GEMINI_API_KEY'])
    model = genai.GenerativeModel('gemini-2.0-flash-exp')
    
    prompt = f"""Database QA Report - DBCP Production

ISSUES FOUND:
{chr(10).join(issues)}

STATS: {stats['tables_processed']} tables processed, {stats['duplicates_removed']} duplicates removed

Generate a concise Telegram alert (max 300 chars) highlighting the most critical issues that need immediate attention. Focus on data corruption, missing timestamps, or high null rates that could affect trading signals."""

    try:
        response = model.generate_content(prompt)
        return response.text.strip() if response else "AI summary unavailable"
    except Exception as e:
        logger.warning(f"AI summary failed: {e}")
        return f"🚨 {len(issues)} issues detected - manual review needed"

def send_telegram_alert(message, config):
    """Send alert to Telegram"""
    if not config.get('TELEGRAM_CHAT_ID'):
        print("Telegram alert:", message)
        return
    
    try:
        url = f"https://api.telegram.org/bot{config['TELEGRAM_BOT_TOKEN']}/sendMessage"
        response = requests.post(url, json={
            'chat_id': config['TELEGRAM_CHAT_ID'],
            'text': f"🔍 DBCP QA Alert\n\n{message}\n\n⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
        }, timeout=10)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Telegram notification failed: {e}")

def main():
    """Main QA execution"""
    try:
        config = load_config()
        print("Starting DBCP quality assurance...")
        
        # Run analysis
        issues, stats = clean_and_analyze_database(config)
        
        # Generate summary
        summary = generate_ai_summary(issues, stats, config)
        
        # Send alert
        send_telegram_alert(summary, config)
        
        print(f"✅ QA Complete: {stats['tables_processed']} tables, {stats['duplicates_removed']} duplicates removed")
        if issues:
            print(f"⚠️ {len(issues)} issues found - check Telegram for details")
        
    except Exception as e:
        error_msg = f"🚨 DBCP QA FAILED: {str(e)}"
        logger.error(error_msg)
        try:
            config = load_config()
            send_telegram_alert(error_msg, config)
        except:
            pass
        raise

if __name__ == "__main__":
    main()