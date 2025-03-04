# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import time
import logging
from sqlalchemy import create_engine

# 🔹 Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 🔹 Start Timer
start_time = time.time()

# 🔹 Database Connection Parameters
DB_CONFIG = {
    "host": "34.55.195.199",
    "user": "yogass09",
    "password": "jaimaakamakhya",
    "port": 5432,
    "database": "dbcp",
    "database_bt": "cp_backtest"  # Backtest database
}

# 🔹 Create Database Engine for Live Data
def create_db_engine():
    return create_engine(f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}')

# 🔹 Create Database Engine for Backtest Data
def create_db_engine_backtest():
    return create_engine(f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database_bt"]}')

# 🔹 Fetch Data
def fetch_data(engine):
    logging.info("Fetching OHLCV data from PostgreSQL...")
    
    query = 'SELECT * FROM "108_1K_coins_ohlcv"'
    with engine.connect() as connection:
        df = pd.read_sql_query(query, connection)

    # Ensure proper formatting
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.sort_values(by=["slug", "timestamp"], inplace=True)

    logging.info(f"✅ Data fetched. Shape: {df.shape}")
    return df

# 🔹 Calculate Percentage Change (Returns)
def calculate_pct_change(df):
    logging.info("Calculating Percentage Change and Cumulative Returns...")

    df["m_pct_1d"] = df.groupby("slug")["close"].pct_change()
    df["d_pct_cum_ret"] = (1 + df["m_pct_1d"]).groupby(df["slug"]).cumprod() - 1

    return df

# 🔹 Calculate VaR & CVaR
def calculate_var_cvar(df, confidence_level=0.95):
    logging.info("Calculating VaR & CVaR...")

    # Historical VaR
    var_df = df.groupby("slug")["m_pct_1d"].quantile(1 - confidence_level).reset_index(name="d_pct_var")

    # Conditional VaR (Expected Shortfall)
    cvar_df = df.groupby("slug").apply(lambda x: x["m_pct_1d"][x["m_pct_1d"] <= x["m_pct_1d"].quantile(1 - confidence_level)].mean()).reset_index(name="d_pct_cvar")

    # Merge results back to the main dataframe
    df = df.merge(var_df, on="slug", how="left")
    df = df.merge(cvar_df, on="slug", how="left")

    return df

# 🔹 Calculate Volume Percentage Changes
def calculate_volume_pct_change(df):
    logging.info("Calculating Volume Percentage Change...")

    df["d_pct_vol_1d"] = df.groupby("slug")["volume"].pct_change()

    return df

# 🔹 Keep Only Latest Timestamp Per Slug
def filter_latest_data(df):
    logging.info("Filtering latest data per slug...")
    
    latest_df = df[df["timestamp"] == df.groupby("slug")["timestamp"].transform("max")]
    return latest_df

# 🔹 Clean and Remove Infinite Values
def clean_data(df):
    logging.info("Cleaning Data (Handling Inf/NaN values)...")

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(inplace=True)

    return df

# 🔹 Ensure PCT_CHANGE Required Columns Exist
def ensure_required_pct_columns(df):
    REQUIRED_COLUMNS = [
        # 🔹 Identifiers
        "id", "slug","name", "timestamp",

        # 🔹 Returns & Risk Metrics
        "m_pct_1d", "d_pct_cum_ret",
        "d_pct_var", "d_pct_cvar",
        
        # 🔹 Volume-Based Metrics
        "d_pct_vol_1d"
    ]

    return df[[col for col in REQUIRED_COLUMNS if col in df.columns]]

# 🔹 Push Data to Live Database
def push_to_db(df, table_name):
    engine = create_db_engine()
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    engine.dispose()
    logging.info(f"✅ {table_name} uploaded successfully to LIVE DB!")

# 🔹 Push Data to Backtest Database (Append Mode)
def push_to_db_backtest(df, table_name):
    engine = create_db_engine_backtest()
    df.to_sql(table_name, con=engine, if_exists="append", index=False)
    engine.dispose()
    logging.info(f"✅ {table_name} uploaded successfully to BACKTEST DB!")

# 🔹 Main Execution
if __name__ == "__main__":
    engine = create_db_engine()
    df = fetch_data(engine)

    df = (df.pipe(calculate_pct_change)
           .pipe(calculate_var_cvar)
           .pipe(calculate_volume_pct_change)
           .pipe(filter_latest_data)
           .pipe(clean_data))

    # Ensure required columns exist **after** calculations
    df = ensure_required_pct_columns(df)

    # Keep only the latest timestamp records
    latest_timestamp  = df["timestamp"].max()
    df = df[df["timestamp"] == latest_timestamp]

    # Push to both Live & Backtest databases
    push_to_db(df, "FE_PCT_CHANGE")           # Live database (replace)
    push_to_db_backtest(df, "FE_PCT_CHANGE")  # Backtest database (append)

    logging.info(f"✅ Process completed in {round((time.time() - start_time) / 60, 2)} minutes!")
