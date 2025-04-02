import os
import pandas as pd
import numpy as np
import time
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# For batch writing on DB (memory efficient)
BATCH_SIZE = 10000

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),  # Log to a file
        logging.StreamHandler()          # Log to console
    ]
)
logger = logging.getLogger(__name__)

# Start time tracking
start_time = time.time()

# GCP Database Connection Parameters
db_host = "34.55.195.199"
db_name = "dbcp"
db_user = "yogass09"
db_password = "jaimaakamakhya"
db_port = 5432

# Create SQLAlchemy engine with error handling
try:
    gcp_engine = create_engine(
        f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}',
        pool_size=10,  # Connection pool
        max_overflow=5,  # Allow up to 5 extra connections
        echo=False
    )
    logger.info("Database connection established successfully.")
except SQLAlchemyError as e:
    logger.error(f"Database connection failed: {e}")
    exit(1)

# Define table queries
table_queries = {
    "ratios_bin": "SELECT * FROM \"FE_RATIOS_SIGNALS\"",
    "df_oscillator_bin": "SELECT * FROM \"FE_OSCILLATORS_SIGNALS\"",
    "df_momentum": "SELECT * FROM \"FE_MOMENTUM_SIGNALS\"",
    "metrics_signal": "SELECT * FROM \"FE_METRICS_SIGNAL\"",
    "tvv_signals": "SELECT * FROM \"FE_TVV_SIGNALS\""
}

# Fetch data from database
data_frames = {}
try:
    with gcp_engine.connect() as connection:
        for df_name, query in table_queries.items():
            logger.info(f"Fetching data for {df_name}...")
            data_frames[df_name] = pd.read_sql_query(query, connection)
    logger.info("All data fetched successfully.")
except SQLAlchemyError as e:
    logger.error(f"Error fetching data from database: {e}")
    exit(1)

# Extract data into variables
ratios_bin = data_frames["ratios_bin"]
df_oscillator_bin = data_frames["df_oscillator_bin"]
df_momentum = data_frames["df_momentum"]
metrics_signal = data_frames["metrics_signal"]
tvv_signals = data_frames["tvv_signals"]

# Merge data
logger.info("Merging DataFrames into DMV...")
dfs_to_join = [ratios_bin, df_oscillator_bin, df_momentum, tvv_signals, metrics_signal]

try:
    DMV = dfs_to_join[0]
    for df in dfs_to_join[1:]:
        overlapping_cols = DMV.columns.intersection(df.columns).difference(['slug'])
        df = df.drop(overlapping_cols, axis=1)
        DMV = pd.merge(DMV, df, on='slug', how='outer')
    logger.info("DataFrames merged successfully.")
except Exception as e:
    logger.error(f"Error merging DataFrames: {e}")
    exit(1)

# Extract and reorder columns
try:
    first_four_cols = DMV[['id', 'slug', 'name', 'timestamp']]
    remaining_cols = DMV.drop(['id', 'slug', 'name', 'timestamp'], axis=1).sort_index(axis=1)
    DMV_sorted = pd.concat([first_four_cols, remaining_cols], axis=1)
    logger.info("DataFrame sorted successfully.")
except KeyError as e:
    logger.error(f"Missing expected columns: {e}")
    exit(1)

# Compute bullish, bearish, and neutral counts
logger.info("Computing bullish, bearish, and neutral counts...")
DMV_sorted['bullish'] = (DMV_sorted.iloc[:, 4:] == 1).sum(axis=1)
DMV_sorted['bearish'] = (DMV_sorted.iloc[:, 4:] == -1).sum(axis=1)
DMV_sorted['neutral'] = (DMV_sorted.iloc[:, 4:] == 0).sum(axis=1)

# Compute scores
try:
    Durability = DMV_sorted[['slug'] + [col for col in DMV_sorted.columns if col.startswith('d_')]]
    Momentum = DMV_sorted[['slug'] + [col for col in DMV_sorted.columns if col.startswith('m_')]]
    Valuation = DMV_sorted[['slug'] + [col for col in DMV_sorted.columns if col.startswith('v_')]]

    Durability['Durability_Score'] = (Durability.drop('slug', axis=1).sum(axis=1) / (Durability.shape[1] - 1)) * 100
    Momentum['Momentum_Score'] = (Momentum.drop('slug', axis=1).sum(axis=1) / (Momentum.shape[1] - 1)) * 100
    Valuation['Valuation_Score'] = (Valuation.drop('slug', axis=1).sum(axis=1) / (Valuation.shape[1] - 1)) * 100

    dmv_scores = pd.DataFrame({
        'slug': Durability['slug'],
        'Durability_Score': Durability['Durability_Score'],
        'Momentum_Score': Momentum['Momentum_Score'],
        'Valuation_Score': Valuation['Valuation_Score']
    })
    logger.info("Scores calculated successfully.")
except Exception as e:
    logger.error(f"Error calculating scores: {e}")
    exit(1)

# Find the latest timestamp
latest_timestamp = DMV_sorted['timestamp'].max()

# Keep only rows with the latest timestamp
DMV_sorted = DMV_sorted[DMV_sorted['timestamp'] == latest_timestamp]

# Check if filtering worked
unique_timestamps = DMV_sorted['timestamp'].nunique()

if unique_timestamps == 1:
    print("✅ Successfully retained only the latest timestamp.")
else:
    print(f"⚠️ There are still {unique_timestamps} unique timestamps after filtering!")

# Confirming latest timestamp count
latest_timestamp_count = DMV_sorted.shape[0]
logger.info(f"Rows remaining after filtering: {latest_timestamp_count}")


# Upload scores to database using batch insert
logger.info("Uploading DMV Scores to database...")
try:
    dmv_scores.to_sql('FE_DMV_SCORES', con=gcp_engine, if_exists='replace', index=False, method='multi', chunksize=BATCH_SIZE)
    logger.info("DMV scores uploaded successfully!")
except SQLAlchemyError as e:
    logger.error(f"Error uploading DMV scores: {e}")
    exit(1)

# Upload scores to database using batch insert
logger.info("Uploading DMV Scores to database...")
try:
    DMV_sorted.to_sql('FE_DMV_ALL', con=gcp_engine, if_exists='replace', index=False, method='multi', chunksize=BATCH_SIZE)
    logger.info("DMV scores uploaded successfully!")
except SQLAlchemyError as e:
    logger.error(f"Error uploading DMV scores: {e}")
    exit(1)

# Execution time
end_time = time.time()
elapsed_time_minutes = (end_time - start_time) / 60
print(elapsed_time_minutes)
logger.info(f"Script execution completed in {elapsed_time_minutes:.2f} minutes.")

# Dispose connection
gcp_engine.dispose()
logger.info("Database connection closed.")

