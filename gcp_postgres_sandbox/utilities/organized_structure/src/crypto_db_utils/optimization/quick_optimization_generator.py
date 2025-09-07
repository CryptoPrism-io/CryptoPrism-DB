#!/usr/bin/env python3
"""
Quick Optimization Generator - Generate primary keys and indexes for dbcp database
"""

import os
from datetime import datetime
from pathlib import Path

def generate_optimization_scripts():
    """Generate SQL optimization scripts based on our schema analysis."""
    
    # All tables from our quick analysis that need primary keys
    tables_needing_pk = [
        "FE_TVV", "FE_MOMENTUM", "1K_coins_ohlcv", "crypto_global_latest",
        "FE_MOMENTUM_SIGNALS", "FE_DMV_ALL", "FE_DMV_SCORES", "NEWS_TOKENOMICS_W",
        "FE_CC_INFO_URL", "FE_OSCILLATORS_SIGNALS", "FE_FEAR_GREED_CMC",
        "FE_TVV_SIGNALS", "FE_OSCILLATOR", "108_1K_coins_ohlcv", "crypto_ratings",
        "1K_coins_ohlcv_backup", "crypto_listings", "FE_METRICS", "FE_METRICS_SIGNAL",
        "FE_PCT_CHANGE", "FE_RATIOS", "FE_RATIOS_SIGNALS", "crypto_listings_latest_1000",
        "NEWS_AIRDROPS_W"
    ]
    
    # Generate primary key script
    pk_script = [
        "-- PRIMARY KEY OPTIMIZATION SCRIPT FOR CRYPTOPRISM-DB",
        "-- Generated: " + datetime.now().isoformat(),
        "-- Database: dbcp",
        "",
        "-- WARNING: Test these on a backup database first!",
        "-- Execute during maintenance window",
        "",
        "BEGIN;",
        "",
    ]
    
    # Add primary keys for tables with slug+timestamp pattern
    tables_with_slug_timestamp = [
        "FE_TVV", "FE_MOMENTUM", "1K_coins_ohlcv", "FE_MOMENTUM_SIGNALS", 
        "FE_DMV_ALL", "FE_DMV_SCORES", "NEWS_TOKENOMICS_W", "FE_OSCILLATORS_SIGNALS",
        "FE_TVV_SIGNALS", "FE_OSCILLATOR", "108_1K_coins_ohlcv", "crypto_ratings",
        "1K_coins_ohlcv_backup", "crypto_listings", "FE_METRICS", "FE_METRICS_SIGNAL",
        "FE_PCT_CHANGE", "FE_RATIOS", "FE_RATIOS_SIGNALS", "crypto_listings_latest_1000",
        "NEWS_AIRDROPS_W"
    ]
    
    for table in tables_with_slug_timestamp:
        pk_script.append(f'-- Add composite primary key for {table}')
        pk_script.append(f'ALTER TABLE "{table}" ADD CONSTRAINT "pk_{table.lower()}" PRIMARY KEY (slug, timestamp);')
        pk_script.append('')
    
    # Special cases for tables with different patterns
    pk_script.extend([
        '-- Add primary key for crypto_global_latest (timestamp only)',
        'ALTER TABLE "crypto_global_latest" ADD CONSTRAINT "pk_crypto_global_latest" PRIMARY KEY (timestamp);',
        '',
        '-- Add primary key for FE_CC_INFO_URL (slug only)',
        'ALTER TABLE "FE_CC_INFO_URL" ADD CONSTRAINT "pk_fe_cc_info_url" PRIMARY KEY (slug);',
        '',
        '-- Add primary key for FE_FEAR_GREED_CMC (timestamp only)', 
        'ALTER TABLE "FE_FEAR_GREED_CMC" ADD CONSTRAINT "pk_fe_fear_greed_cmc" PRIMARY KEY (timestamp);',
        '',
        'COMMIT;',
        '',
        '-- Verification queries:',
        '-- SELECT COUNT(*) FROM "FE_DMV_ALL";',
        '-- SELECT slug, timestamp FROM "FE_DMV_ALL" LIMIT 5;',
    ])
    
    # Generate strategic indexes script
    index_script = [
        "-- STRATEGIC INDEX OPTIMIZATION SCRIPT FOR CRYPTOPRISM-DB",
        "-- Generated: " + datetime.now().isoformat(),
        "-- Database: dbcp",
        "",
        "-- WARNING: Test these on a backup database first!",
        "-- Execute AFTER primary keys are added",
        "",
        "BEGIN;",
        "",
        "-- Performance-focused indexes for most queried tables",
        "",
    ]
    
    # Key performance indexes
    performance_indexes = [
        {
            "table": "FE_DMV_ALL",
            "indexes": [
                ('idx_fe_dmv_all_timestamp', 'timestamp DESC'),
                ('idx_fe_dmv_all_slug', 'slug'),
                ('idx_fe_dmv_all_slug_timestamp', 'slug, timestamp DESC'),
            ]
        },
        {
            "table": "FE_MOMENTUM_SIGNALS", 
            "indexes": [
                ('idx_fe_momentum_signals_timestamp', 'timestamp DESC'),
                ('idx_fe_momentum_signals_slug', 'slug'),
            ]
        },
        {
            "table": "1K_coins_ohlcv",
            "indexes": [
                ('idx_1k_coins_ohlcv_timestamp', 'timestamp DESC'),
                ('idx_1k_coins_ohlcv_slug', 'slug'),
            ]
        },
        {
            "table": "FE_OSCILLATORS_SIGNALS",
            "indexes": [
                ('idx_fe_oscillators_signals_timestamp', 'timestamp DESC'),
                ('idx_fe_oscillators_signals_slug', 'slug'),
            ]
        },
        {
            "table": "FE_RATIOS_SIGNALS",
            "indexes": [
                ('idx_fe_ratios_signals_timestamp', 'timestamp DESC'),
                ('idx_fe_ratios_signals_slug', 'slug'),
            ]
        }
    ]
    
    for table_info in performance_indexes:
        table_name = table_info["table"]
        index_script.append(f'-- Indexes for {table_name}')
        
        for index_name, columns in table_info["indexes"]:
            index_script.append(f'CREATE INDEX CONCURRENTLY "{index_name}" ON "{table_name}" ({columns});')
        
        index_script.append('')
    
    index_script.extend([
        'COMMIT;',
        '',
        '-- Verification queries:',
        '-- SELECT schemaname, tablename, indexname FROM pg_indexes WHERE schemaname = \'public\';',
        '-- ANALYZE; -- Update table statistics after creating indexes',
    ])
    
    # Generate rollback script
    rollback_script = [
        "-- ROLLBACK SCRIPT FOR OPTIMIZATION",
        "-- Generated: " + datetime.now().isoformat(),
        "-- Use this script to remove optimizations if needed",
        "",
        "BEGIN;",
        "",
        "-- Remove indexes first",
    ]
    
    # Add index drops
    for table_info in performance_indexes:
        table_name = table_info["table"]
        rollback_script.append(f'-- Drop indexes for {table_name}')
        
        for index_name, _ in table_info["indexes"]:
            rollback_script.append(f'DROP INDEX IF EXISTS "{index_name}";')
        
        rollback_script.append('')
    
    rollback_script.append("-- Remove primary key constraints")
    for table in tables_with_slug_timestamp:
        rollback_script.append(f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "pk_{table.lower()}";')
    
    rollback_script.extend([
        'ALTER TABLE "crypto_global_latest" DROP CONSTRAINT IF EXISTS "pk_crypto_global_latest";',
        'ALTER TABLE "FE_CC_INFO_URL" DROP CONSTRAINT IF EXISTS "pk_fe_cc_info_url";',
        'ALTER TABLE "FE_FEAR_GREED_CMC" DROP CONSTRAINT IF EXISTS "pk_fe_fear_greed_cmc";',
        '',
        'COMMIT;'
    ])
    
    return {
        'primary_keys': '\n'.join(pk_script),
        'strategic_indexes': '\n'.join(index_script),
        'rollback': '\n'.join(rollback_script)
    }

def save_optimization_scripts(scripts_dict, output_dir="sql_optimizations"):
    """Save optimization scripts to files."""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    files_created = {}
    
    # Save primary keys script
    pk_file = output_path / f"01_primary_keys_{timestamp}.sql"
    with open(pk_file, 'w', encoding='utf-8') as f:
        f.write(scripts_dict['primary_keys'])
    files_created['primary_keys'] = str(pk_file)
    
    # Save indexes script  
    idx_file = output_path / f"02_strategic_indexes_{timestamp}.sql"
    with open(idx_file, 'w', encoding='utf-8') as f:
        f.write(scripts_dict['strategic_indexes'])
    files_created['strategic_indexes'] = str(idx_file)
    
    # Save rollback script
    rollback_file = output_path / f"03_rollback_{timestamp}.sql"
    with open(rollback_file, 'w', encoding='utf-8') as f:
        f.write(scripts_dict['rollback'])
    files_created['rollback'] = str(rollback_file)
    
    return files_created

def main():
    """Generate and save optimization scripts."""
    print("=" * 60)
    print("QUICK DATABASE OPTIMIZATION SCRIPT GENERATOR")
    print("=" * 60)
    
    try:
        # Generate scripts
        scripts = generate_optimization_scripts()
        
        # Save scripts
        files = save_optimization_scripts(scripts)
        
        print("DATABASE OPTIMIZATION SCRIPTS GENERATED")
        print("=" * 60)
        
        for script_type, filepath in files.items():
            print(f"OK {script_type}: {filepath}")
        
        print("\nNEXT STEPS:")
        print("1. Review the generated SQL scripts carefully")
        print("2. Test on a backup database first")
        print("3. Execute primary key script first")
        print("4. Execute index script second") 
        print("5. Run ANALYZE; to update statistics")
        print("6. Re-run benchmark to measure improvements")
        
        print(f"\nEXPECTED PERFORMANCE IMPROVEMENTS:")
        print("- JOIN operations: 50-80% faster with primary keys")
        print("- WHERE filtering: 70-90% faster with indexes")
        print("- Overall query performance: 60-90% improvement")
        
        return files
        
    except Exception as e:
        print(f"ERROR: {e}")
        return None

if __name__ == "__main__":
    main()