import os
import pandas as pd
import numpy as np
import time
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

BATCH_SIZE = 10000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

start_time = time.time()

db_host = "34.55.195.199"
db_name = "dbcp"
db_user = "yogass09"
db_password = "jaimaakamakhya"
db_port = 5432

gcp_engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

query = """
SELECT *
FROM "FE_OSCILLATORS_SIGNALS" AS o
NATURAL JOIN "FE_MOMENTUM_SIGNALS" AS m
NATURAL JOIN "FE_METRICS_SIGNAL" AS me
NATURAL JOIN "FE_TVV_SIGNALS" AS t
NATURAL JOIN "FE_RATIOS_SIGNALS" AS r;
"""

def fetch_data_from_gcp(query, engine):
    try:
        return pd.read_sql_query(query, engine)
    except SQLAlchemyError as e:
        logger.error(f"Error fetching data: {e}")
        return None

try:
    df = fetch_data_from_gcp(query, gcp_engine)
    if df is not None:
        print(df.info())
except Exception as e:
    logger.error(f"Unexpected error: {e}")

for col in ['bullish', 'bearish', 'neutral']:
    df[col] = 0

for index, row in df.iloc[:, 4:].iterrows():
    df.loc[index, 'bullish'] = (row == 1).sum()
    df.loc[index, 'bearish'] = (row == -1).sum()
    df.loc[index, 'neutral'] = (row == 0).sum()

DMV_sorted = df
DMV_sorted.to_sql('FE_DMV_ALL', con=gcp_engine, if_exists='replace', index=False)
print("DMV_sorted uploaded successfully!")

# Score Computation
try:
    Durability = df[['slug'] + [c for c in df.columns if c.startswith('d_')]]
    Momentum = df[['slug'] + [c for c in df.columns if c.startswith('m_')]]
    Valuation = df[['slug'] + [c for c in df.columns if c.startswith('v_')]]

    Durability['Durability_Score'] = Durability.drop('slug', axis=1).mean(axis=1) * 100
    Momentum['Momentum_Score'] = Momentum.drop('slug', axis=1).mean(axis=1) * 100
    Valuation['Valuation_Score'] = Valuation.drop('slug', axis=1).mean(axis=1) * 100

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

latest_timestamp = df['timestamp'].max()
DMV_sorted = df[df['timestamp'] == latest_timestamp]

if DMV_sorted['timestamp'].nunique() == 1:
    print("✅ Latest timestamp retained.")
else:
    print("⚠️ Multiple timestamps still present.")

logger.info(f"Rows after filtering: {DMV_sorted.shape[0]}")

try:
    dmv_scores.to_sql('FE_DMV_SCORES', con=gcp_engine, if_exists='replace', index=False, method='multi', chunksize=BATCH_SIZE)
    logger.info("DMV scores uploaded.")
except SQLAlchemyError as e:
    logger.error(f"Upload error: {e}")
    exit(1)

elapsed_time_minutes = (time.time() - start_time) / 60
print(elapsed_time_minutes)
logger.info(f"Script completed in {elapsed_time_minutes:.2f} minutes.")

gcp_engine.dispose()
logger.info("DB connection closed.")
