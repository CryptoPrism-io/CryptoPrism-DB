# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import time
import logging
from sqlalchemy import create_engine

# 🔹 Logging setup for debugging and monitoring
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
    "database_bt":"cp_backtest"
}

# 🔹 Create SQLAlchemy Engine
def create_db_engine():
    return create_engine(f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}')

# 🔹 Fetch Data Efficiently
def fetch_data(engine):
    logging.info("Fetching data from PostgreSQL...")

    query = """
        SELECT o.*, r.cmc_rank
        FROM "108_1K_coins_ohlcv" o
        LEFT JOIN crypto_listings_latest_1000 r ON o.slug = r.slug
    """

    with engine.connect() as connection:
        df = pd.read_sql_query(query, connection)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.sort_values(by=["slug", "timestamp"], inplace=True)

    logging.info(f"✅ Data fetched. Shape: {df.shape}")
    return df

# 🔹 OBV Calculation (Vectorized)
def calculate_obv(df):
    logging.info("Calculating OBV...")
    
    def obv_calc(group):
        obv = np.zeros(len(group))
        volume_changes = np.where(group["close"].diff().fillna(0) > 0, group["volume"], 
                                  np.where(group["close"].diff().fillna(0) < 0, -group["volume"], 0))
        obv[1:] = np.cumsum(volume_changes[:-1])  
        return obv

    df["obv"] = df.groupby("slug", group_keys=False,include_groups=False).apply(lambda g: pd.Series(obv_calc(g), index=g.index))
    df["m_tvv_obv_1d"] = df.groupby("slug")["obv"].pct_change()

    return df

# 🔹 Moving Averages
def calculate_moving_averages(df):
    logging.info("Calculating Moving Averages...")
    
    df = df.assign(
        SMA9=df.groupby("slug")["close"].transform(lambda x: x.rolling(9, min_periods=1).mean()),
        SMA18=df.groupby("slug")["close"].transform(lambda x: x.rolling(18, min_periods=1).mean()),
        EMA9=df.groupby("slug")["close"].transform(lambda x: x.ewm(span=9, adjust=False).mean()),
        EMA18=df.groupby("slug")["close"].transform(lambda x: x.ewm(span=18, adjust=False).mean()),
        SMA21=df.groupby("slug")["close"].transform(lambda x: x.rolling(21, min_periods=1).mean()),
        SMA108=df.groupby("slug")["close"].transform(lambda x: x.rolling(108, min_periods=1).mean()),
        EMA21=df.groupby("slug")["close"].transform(lambda x: x.ewm(span=21, adjust=False).mean()),
        EMA108=df.groupby("slug")["close"].transform(lambda x: x.ewm(span=108, adjust=False).mean())
    )
    
    return df

# 🔹 ATR Calculation
def calculate_atr(df, window=14):
    logging.info("Calculating ATR...")

    df["prev_close"] = df.groupby("slug")["close"].shift(1)
    df["tr1"] = df["high"] - df["low"]
    df["tr2"] = abs(df["high"] - df["prev_close"])
    df["tr3"] = abs(df["low"] - df["prev_close"])
    df["TR"] = df[["tr1", "tr2", "tr3"]].max(axis=1)
    df["ATR"] = df.groupby("slug")["TR"].transform(lambda x: x.rolling(window, min_periods=1).mean())

    return df

# 🔹 Keltner & Donchian Channels
def calculate_channels(df):
    logging.info("Calculating Keltner & Donchian Channels...")

    df["Keltner_Upper"] = df["EMA9"] + (df["ATR"] * 1.5)
    df["Keltner_Lower"] = df["EMA9"] - (df["ATR"] * 1.5)
    df["Donchian_Upper"] = df.groupby("slug")["high"].transform(lambda x: x.rolling(20, min_periods=1).max())
    df["Donchian_Lower"] = df.groupby("slug")["low"].transform(lambda x: x.rolling(20, min_periods=1).min())

    return df

# 🔹 VWAP Calculation
def calculate_vwap(df):
    logging.info("Calculating VWAP...")
    
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    df["cum_price_volume"] = df.groupby("slug")["typical_price"].cumsum() * df.groupby("slug")["volume"].cumsum()
    df["cum_volume"] = df.groupby("slug")["volume"].cumsum()
    df["VWAP"] = df["cum_price_volume"] / df["cum_volume"]
    
    return df

# 🔹 CMF & ADL Calculation
def calculate_cmf(df, period=21):
    logging.info("Calculating CMF & ADL...")

    # Correct ADL Calculation
    df["ADL"] = ((df["close"] - df["low"] - (df["high"] - df["close"])) / (df["high"] - df["low"])) * df["volume"]

    # Cumulative ADL
    df["cum_adl"] = df.groupby("slug")["ADL"].cumsum()

    # CMF Calculation
    df["CMF"] = df.groupby("slug")["cum_adl"].transform(lambda x: x.rolling(period, min_periods=1).sum()) / \
                df.groupby("slug")["volume"].transform(lambda x: x.rolling(period, min_periods=1).sum())

    return df


# 🔹 Ensure 37 Required Columns Exist
def ensure_required_columns(df):
    REQUIRED_COLUMNS = [
    # 🔹 Identifiers
    "id", "slug", "name", "timestamp",
    
    # 🔹 Market Data
    "open", "high", "low", "close", "volume", "market_cap",

    # 🔹 On-Balance Volume (OBV)
    "obv", "m_tvv_obv_1d",

    # 🔹 Moving Averages
    "SMA9", "SMA18", "EMA9", "EMA18",
    "SMA21", "SMA108", "EMA21", "EMA108",

    # 🔹 True Range & ATR
    "prev_close", "tr1", "tr2", "tr3", "TR", "ATR",

    # 🔹 Keltner & Donchian Channels
    "Keltner_Upper", "Keltner_Lower",
    "Donchian_Upper", "Donchian_Lower",

    # 🔹 VWAP & CMF
    "typical_price", "cum_price_volume", "cum_volume", "VWAP",
    "ADL", "cum_adl", "CMF"
]


    return df[REQUIRED_COLUMNS]

# 🔹 TVV Binary Signals
def generate_binary_signals(df):
    logging.info("Generating Binary Signals...")

    df["m_tvv_obv_1d_binary"] = np.sign(df["m_tvv_obv_1d"])
    df["d_tvv_sma9_18"] = np.sign(df["SMA9"] - df["SMA18"])
    df["d_tvv_ema9_18"] = np.sign(df["EMA9"] - df["EMA18"])
    df["d_tvv_sma21_108"] = np.sign(df["SMA21"] - df["SMA108"])
    df["d_tvv_ema21_108"] = np.sign(df["EMA21"] - df["EMA108"])
    df["m_tvv_cmf"] = np.sign(df["CMF"])

    return df    

# 🔹 Ensure TVV Signals Required Columns Exist
def ensure_signals_columns(df):
    REQUIRED_SIGNAL_COLUMNS = [
        # 🔹 Identifiers
        "id", "slug", "timestamp",

        # 🔹 OBV-Based Signals
        "m_tvv_obv_1d_binary", 
        
        # 🔹 Moving Average Crossovers
        "d_tvv_sma9_18", "d_tvv_ema9_18",
        "d_tvv_sma21_108", "d_tvv_ema21_108",
        
        # 🔹 CMF-Based Signal
        "m_tvv_cmf"
    ]
    
    # Ensure only existing columns are selected (prevent KeyErrors)
    return df[[col for col in REQUIRED_SIGNAL_COLUMNS if col in df.columns]]


# 🔹 Push Data to Database
def push_to_db(df, table_name):
    engine = create_db_engine()
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    engine.dispose()
    logging.info(f"✅ {table_name} uploaded successfully!")

# 🔹 Create SQLAlchemy Engine
def create_db_engine_backtest():
    return create_engine(f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database_bt"]}')

# 🔹 Push Data to Database
def push_to_db_backtest(df, table_name):
    engine = create_db_engine_backtest()
    df.to_sql(table_name, con=engine, if_exists="append", index=False)
    engine.dispose()
    logging.info(f"✅ {table_name} uploaded successfully!")

if __name__ == "__main__":
    engine = create_db_engine()
    df = fetch_data(engine)

    df = (df.pipe(calculate_obv)
           .pipe(calculate_moving_averages)
           .pipe(calculate_atr)
           .pipe(calculate_channels)      # ✅ Added Keltner & Donchian calculation
           .pipe(calculate_vwap)          # ✅ Added VWAP calculation
           .pipe(calculate_cmf)
           .pipe(generate_binary_signals))

    # Keep only the latest timestamp records
    latest_timestamp = df["timestamp"].max()
    latest_data = df[df["timestamp"] == latest_timestamp]

    # Ensure required columns exist **after** all calculations
    latest_data_cols = ensure_required_columns(latest_data)
    latest_data_signals_cols = ensure_signals_columns(latest_data) 

    # Separate TVV and TVV Signals
    tvv = latest_data_cols.copy()
    tvv_signals = latest_data_signals_cols.copy()

    # Push to database
    push_to_db(tvv, "FE_TVV")
    push_to_db(tvv_signals, "FE_TVV_SIGNALS")

    push_to_db_backtest(tvv, "FE_TVV")
    push_to_db_backtest(tvv_signals, "FE_TVV_SIGNALS")

    logging.info(f"✅ Process completed in {round((time.time() - start_time) / 60, 2)} minutes!")