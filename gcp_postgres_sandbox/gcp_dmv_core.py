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
FULL OUTER JOIN "FE_MOMENTUM_SIGNALS" AS m USING (slug, timestamp)
FULL OUTER JOIN "FE_METRICS_SIGNAL" AS me USING (slug, timestamp)
FULL OUTER JOIN "FE_TVV_SIGNALS" AS t USING (slug, timestamp)
FULL OUTER JOIN "FE_RATIOS_SIGNALS" AS r USING (slug, timestamp);
"""

def fetch_data(query, engine):
    try:
        return pd.read_sql_query(query, engine)
    except SQLAlchemyError as e:
        logger.error(f"Error fetching data: {e}")
        return pd.DataFrame()

df = fetch_data(query, gcp_engine)

if df.empty:
    logger.warning("Fetched DataFrame is empty. Exiting.")
    exit(1)
df = (
    df.loc[:, ~df.columns.duplicated()]
      .pipe(lambda d: d[d['slug'].eq('bitcoin') | d.drop(columns='slug').notna().all(axis=1)])
      .fillna(0)
)

for col in ['bullish', 'bearish', 'neutral']:
    df[col] = 0

for index, row in df.iloc[:, 4:].iterrows():
    df.at[index, 'bullish'] = (row == 1).sum()
    df.at[index, 'bearish'] = (row == -1).sum()
    df.at[index, 'neutral'] = (row == 0).sum()

df.to_sql('FE_DMV_ALL', con=gcp_engine, if_exists='replace', index=False)
logger.info("FE_DMV_ALL uploaded.")

Durability = df[['slug'] + [c for c in df.columns if c.startswith('d_')]]
Momentum = df[['slug'] + [c for c in df.columns if c.startswith('m_')]]
Valuation = df[['slug'] + [c for c in df.columns if c.startswith('v_')]]

Durability['Durability_Score'] = Durability.drop('slug', axis=1).sum(axis=1) / (Durability.shape[1] - 1) * 100
Momentum['Momentum_Score'] = Momentum.drop('slug', axis=1).sum(axis=1) / (Momentum.shape[1] - 1) * 100
Valuation['Valuation_Score'] = Valuation.drop('slug', axis=1).sum(axis=1) / (Valuation.shape[1] - 1) * 100

dmv_scores = pd.DataFrame({
    'slug': df['slug'],
    'timestamp': df['timestamp'],
    'Durability_Score': Durability['Durability_Score'],
    'Momentum_Score': Momentum['Momentum_Score'],
    'Valuation_Score': Valuation['Valuation_Score']
})

latest_timestamp = df['timestamp'].max()
df = df[df['timestamp'] == latest_timestamp]

if df['timestamp'].nunique() == 1:
    print("✅ Latest timestamp filtered.")
else:
    print("⚠️ Multiple timestamps still exist.")

logger.info(f"Remaining rows: {df.shape[0]}")

try:
    dmv_scores.to_sql('FE_DMV_SCORES', con=gcp_engine, if_exists='replace', index=False, method='multi', chunksize=BATCH_SIZE)
    logger.info("FE_DMV_SCORES uploaded.")
except SQLAlchemyError as e:
    logger.error(f"Error uploading scores: {e}")
    exit(1)

logger.info(f"Done in {(time.time() - start_time) / 60:.2f} mins.")
gcp_engine.dispose()
