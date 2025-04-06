# @title Oscillators
import pandas as pd
import numpy as np
import logging
from sqlalchemy import create_engine
import time

# 🔹 Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 🔹 Database Connection Parameters
DB_CONFIG = {
    "host": "34.55.195.199",
    "user": "yogass09",
    "password": "jaimaakamakhya",
    "port": 5432,
    "database": "dbcp",
    "database_bt": "cp_backtest",
}

# 🔹 Create SQLAlchemy Engine
def create_db_engine():
    return create_engine(f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}')

# 🔹 Create SQLAlchemy Engine for Backtesting
def create_db_engine_backtest():
    return create_engine(f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database_bt"]}')

# 🔹 Fetch Data Efficiently
def fetch_data(engine):
    logging.info("Fetching data from PostgreSQL...")
    query = """    SELECT
              "public"."1K_coins_ohlcv".*
            FROM
              "public"."1K_coins_ohlcv"
            INNER JOIN
              "public"."crypto_listings_latest_1000"
            ON
              "public"."1K_coins_ohlcv"."slug" = "public"."crypto_listings_latest_1000"."slug"
            WHERE
              "public"."crypto_listings_latest_1000"."cmc_rank" <= 1000
              AND "public"."1K_coins_ohlcv"."timestamp" >= NOW() - INTERVAL '110 days';
              """

    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.sort_values(by=["slug", "timestamp"], inplace=True)
        logging.info(f"✅ Data fetched. Shape: {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        raise

# 🔹 Calculate 1 Day Percentage Change
def calculate_pct_change(df):
    logging.info("Calculating 1-day percentage change...")
    df['m_pct_1d'] = df.groupby('slug')['close'].transform(lambda x: x.pct_change())
    return df

# 🔹 Calculate Cumulative Returns
def calculate_cum_ret(df):
    logging.info("Calculating cumulative returns...")
    df['d_pct_cum_ret'] = df.groupby('slug')['m_pct_1d'].transform(lambda x: (1 + x).cumprod() - 1)
    return df

# 🔹 Calculate MACD (Vectorized)
def calculate_macd(df, short_period=12, long_period=26, signal_period=9):
    logging.info(f"Calculating MACD with short period {short_period}, long period {long_period}, signal period {signal_period}")

    df = df.groupby('slug', group_keys=False).apply(lambda x: x.assign(
        EMA_12=x['close'].ewm(span=short_period, adjust=False).mean(),
        EMA_26=x['close'].ewm(span=long_period, adjust=False).mean()
    ))
    df['MACD'] = df['EMA_12'] - df['EMA_26']
    df['Signal'] = df.groupby('slug')['MACD'].transform(lambda x: x.ewm(span=signal_period, adjust=False).mean())
    return df

# 🔹 Calculate CCI (Vectorized)
def calculate_cci(df, period=20):
    logging.info(f"Calculating CCI with period {period}")

    df['TP'] = (df['high'] + df['low'] + df['close']) / 3
    df = df.groupby('slug', group_keys=False).apply(lambda x: x.assign(
        SMA_TP=x['TP'].rolling(window=period, min_periods=1).mean(),
        MAD=lambda y: (y['TP'] - y['SMA_TP']).abs().rolling(window=period, min_periods=1).mean()
    ))
    df['CCI'] = (df['TP'] - df['SMA_TP']) / (0.015 * df['MAD'])
    return df

# 🔹 Calculate ADX (Vectorized, with correct Wilder's Smoothing and naming)
def calculate_adx(df, period=14):
    logging.info(f"Calculating ADX with period {period}")

    df['prev_close'] = df.groupby('slug')['close'].shift(1)
    df['TR'] = np.maximum(
        df['high'] - df['low'],
        np.maximum(abs(df['high'] - df['prev_close']), abs(df['low'] - df['prev_close']))
    )

    df['+DM'] = np.where(((df['high'] - df['high'].shift(1)) > (df['low'].shift(1) - df['low'])),
                        np.maximum(df['high'] - df['high'].shift(1), 0), 0)
    df['-DM'] = np.where(((df['low'].shift(1) - df['low']) > (df['high'] - df['high'].shift(1))),
                        np.maximum(df['low'].shift(1) - df['low'], 0), 0)

    # Wilder's Smoothing (EMA) and .assign() within transform, CORRECT NAMING
    df = df.groupby('slug', group_keys=False).apply(lambda x: x.assign(
        Smoothed_TR=x['TR'].ewm(alpha=1/period, adjust=False).mean(),
        Smoothed_plus_DM=x['+DM'].ewm(alpha=1/period, adjust=False).mean(),
        Smoothed_minus_DM=x['-DM'].ewm(alpha=1/period, adjust=False).mean()
    ))

    df['+DI'] = 100 * (df['Smoothed_plus_DM'] / df['Smoothed_TR'])
    df['-DI'] = 100 * (df['Smoothed_minus_DM'] / df['Smoothed_TR'])
    df['DX'] = 100 * (abs(df['+DI'] - df['-DI']) / (df['+DI'] + df['-DI']))
    df['ADX'] = df.groupby('slug')['DX'].transform(lambda x: x.ewm(alpha=1/period, adjust=False).mean())  # Use EMA for ADX
    return df

# 🔹 Calculate Ultimate Oscillator (Vectorized, with correct periods)
def calculate_ultimate_oscillator(df, short_period=7, intermediate_period=14, long_period=28):
    logging.info(f"Calculating Ultimate Oscillator with periods {short_period}, {intermediate_period}, {long_period}")

    df['prev_close'] = df.groupby('slug')['close'].shift(1)
    df['BP'] = df['close'] - np.minimum(df['low'], df['prev_close'])
    df['TR'] = np.maximum(
        df['high'] - df['low'],
        np.maximum(abs(df['high'] - df['prev_close']), abs(df['low'] - df['prev_close']))
    )

    # Use .assign() and .transform() for rolling sums
    df = df.groupby('slug', group_keys=False).apply(lambda x: x.assign(
        Avg_BP_short=x['BP'].rolling(window=short_period, min_periods=1).sum(),
        Avg_TR_short=x['TR'].rolling(window=short_period, min_periods=1).sum(),
        Avg_BP_intermediate=x['BP'].rolling(window=intermediate_period, min_periods=1).sum(),
        Avg_TR_intermediate=x['TR'].rolling(window=intermediate_period, min_periods=1).sum(),
        Avg_BP_long=x['BP'].rolling(window=long_period, min_periods=1).sum(),
        Avg_TR_long=x['TR'].rolling(window=long_period, min_periods=1).sum()
    ))

    df['UO'] = 100 * (
        (4 * df['Avg_BP_short'] + 2 * df['Avg_BP_intermediate'] + df['Avg_BP_long']) /
        (4 * df['Avg_TR_short'] + 2 * df['Avg_TR_intermediate'] + df['Avg_TR_long'])
    )
    return df

# 🔹 Calculate Awesome Oscillator (Vectorized)
def calculate_awesome_oscillator(df):
    logging.info("Calculating Awesome Oscillator")

    df['MP'] = (df['high'] + df['low']) / 2
     # Use .assign() for rolling means within transform
    df = df.groupby('slug', group_keys=False).apply(lambda x: x.assign(
        SMA_5=x['MP'].rolling(window=5, min_periods=1).mean(),
        SMA_34=x['MP'].rolling(window=34, min_periods=1).mean()
    ))
    df['AO'] = df['SMA_5'] - df['SMA_34']
    return df

# 🔹 Calculate TRIX (Vectorized)
def calculate_trix(df, period=15):
    logging.info(f"Calculating TRIX with period {period}")

    # Use .assign() for EMAs within transform
    df = df.groupby('slug', group_keys=False).apply(lambda x: x.assign(
        EMA1=x['close'].ewm(span=period, adjust=False).mean()
    ))
    df = df.groupby('slug', group_keys=False).apply(lambda x: x.assign(
        EMA2=x['EMA1'].ewm(span=period, adjust=False).mean()
    ))
    df = df.groupby('slug', group_keys=False).apply(lambda x: x.assign(
        EMA3=x['EMA2'].ewm(span=period, adjust=False).mean()
    ))
    df['TRIX'] = df.groupby('slug')['EMA3'].transform(lambda x: x.pct_change() * 100)
    return df

# 🔹 Generate Binary Signals (Oscillators)
def generate_binary_signals_oscillators(df):
    logging.info("Generating binary signals for Oscillator indicators...")

    df['m_osc_macd_crossover_bin'] = np.select([df['MACD'] > df['Signal'], df['MACD'] < df['Signal']], [1, -1], default=0)
    df['m_osc_cci_bin'] = np.select([df['CCI'] > 108, df['CCI'] < -108], [1, -1], default=0)
    df['m_osc_adx_bin'] = np.select(
        [(df['+DI'] > df['-DI']) & (df['ADX'] >= 20), (df['-DI'] > df['+DI']) & (df['ADX'] >= 20)],
        [1, -1],
        default=0
    )
    df['m_osc_uo_bin'] = np.select([df['UO'] < 33, df['UO'] > 67], [1, -1], default=0)
    df['m_osc_ao_bin'] = np.select([df['AO'] > 0, df['AO'] < 0], [1, -1], default=0)
    df['m_osc_trix_bin'] = np.select([df['TRIX'] > 0, df['TRIX'] < 0], [1, -1], default=0)
    return df

# 🔹 Ensure DataFrame Columns Match Database Schema
def rename_columns_for_db(df):
    rename_mapping = {
        "Smoothed_plus_DM": "Smoothed_+DM",
        "Smoothed_minus_DM": "Smoothed_-DM"
    }
    return df.rename(columns=rename_mapping)

# --- Push Data to Database ---
def push_to_db(df, table_name, engine, if_exists="replace"):
    try:
        df = rename_columns_for_db(df)  # ✅ Ensure proper column names before upload
        df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
        logging.info(f"✅ {table_name} uploaded successfully!")
    except Exception as e:
        logging.error(f"Error pushing data to {table_name}: {e}")
        raise

# 🔹 Ensure Required Columns Exist and Fill NaN
def ensure_required_columns(df, required_columns):
    """Ensures all required columns exist, filling missing ones with NaN."""
    for col in required_columns:
        if col not in df.columns:
            logging.warning(f"Column {col} missing, filling with NaN")
            df[col] = np.nan  # Fill missing columns with NaN
    return df

# 🔹 Main Execution Block
if __name__ == "__main__":
    start_time = time.time()

    # --- Database Engines ---
    engine = create_db_engine()
    engine_bt = create_db_engine_backtest()

    # --- Fetch Data (Standalone execution) ---
    df = fetch_data(engine)

    # --- Calculate prerequisite indicators ---
    df = (df.pipe(calculate_pct_change)
          .pipe(calculate_cum_ret)
          )

    # --- Calculate Oscillators using pipe() ---
    df = (df.pipe(calculate_macd)
          .pipe(calculate_cci)
          .pipe(calculate_adx)
          .pipe(calculate_ultimate_oscillator)
          .pipe(calculate_awesome_oscillator)
          .pipe(calculate_trix)
          .pipe(generate_binary_signals_oscillators)
          )

    # --- Data Filtering (Keep only the rows with the absolute latest timestamp) ---
    latest_timestamp = df['timestamp'].max()
    latest_data = df[df['timestamp'] == latest_timestamp]

    # --- Column Selection (GUARANTEED TO MATCH YOUR LIST) ---
    oscillator_cols = [
    # 🔹 Identifiers
    "id", "slug", "name", "timestamp",

    # 🔹 Market Data (OHLCV + Market Cap)
    "open", "high", "low", "close", "volume", "market_cap",

    # 🔹 Momentum & Trend Indicators
    "m_pct_1d", "d_pct_cum_ret",  # Daily percentage change & cumulative return

    # 🔹 Moving Averages & MACD
    "EMA_12", "EMA_26",  # Exponential Moving Averages
    "MACD", "Signal",    # MACD & Signal Line

    # 🔹 Typical Price & Mean Deviation
    "TP", "SMA_TP", "MAD",  # Typical Price, Simple Moving Average of TP, Mean Absolute Deviation

    # 🔹 Commodity Channel Index (CCI)
    "CCI",

    # 🔹 True Range & Directional Movement (ADX System)
    "TR", "+DM", "-DM",  # True Range, Positive/Negative Directional Movement
    "Smoothed_TR", "Smoothed_plus_DM", "Smoothed_minus_DM",  # Smoothed TR and DM values
    "+DI", "-DI", "DX", "ADX",  # Directional Indicators & ADX

    # 🔹 Price Action (Previous Close)
    "prev_close",

    # 🔹 Ultimate Oscillator (UO) & Supporting Calculations
    "BP",  # Buying Pressure
    "Avg_BP_short", "Avg_TR_short",  # Short-Term Averages
    "Avg_BP_intermediate", "Avg_TR_intermediate",  # Intermediate-Term Averages
    "Avg_BP_long", "Avg_TR_long",  # Long-Term Averages
    "UO",  # Ultimate Oscillator

    # 🔹 Awesome Oscillator (AO)
    "MP",  # Median Price
    "SMA_5", "SMA_34",  # AO Components
    "AO",  # Awesome Oscillator

    # 🔹 TRIX (Triple Exponential Moving Average)
    "EMA1", "EMA2", "EMA3",  # EMA Components for TRIX
    "TRIX"  # TRIX Indicator
    ]

    # --- Binary Oscillator Signal Columns ---
    oscillator_signals_cols = [
        # 🔹 Identifiers
        "id", "slug", "name", "timestamp",

        # 🔹 Binary Signals (Momentum-Based)
        "m_osc_macd_crossover_bin",  # MACD Crossover Signal
        "m_osc_cci_bin",  # Commodity Channel Index (CCI) Signal
        "m_osc_adx_bin",  # ADX Strength Signal
        "m_osc_uo_bin",  # Ultimate Oscillator (UO) Signal
        "m_osc_ao_bin",  # Awesome Oscillator (AO) Signal
        "m_osc_trix_bin"  # TRIX Trend Signal
    ]

    # --- Ensure Columns and select ---
    latest_data = ensure_required_columns(latest_data, oscillator_cols)
    oscillator_df = latest_data[oscillator_cols]

    latest_data = ensure_required_columns(latest_data, oscillator_signals_cols)
    oscillator_signals_df = latest_data[oscillator_signals_cols]

    # --- Handle Infinite Values ---
    oscillator_df = oscillator_df.replace([np.inf, -np.inf], np.nan)
    oscillator_signals_df = oscillator_signals_df.replace([np.inf, -np.inf], np.nan)

    # --- Push to Database ---
    push_to_db(oscillator_df, "FE_OSCILLATOR", engine)
    push_to_db(oscillator_signals_df, "FE_OSCILLATORS_SIGNALS", engine)

    # --- Push to Backtest Database ---
    push_to_db(oscillator_df, "FE_OSCILLATOR", engine_bt, if_exists="append")
    push_to_db(oscillator_signals_df, "FE_OSCILLATORS_SIGNALS", engine_bt, if_exists="append")

    engine.dispose()
    engine_bt.dispose()
    end_time = time.time()
    elapsed_time = (end_time - start_time) / 60
    logging.info(f"Process completed in {elapsed_time:.2f} minutes")
