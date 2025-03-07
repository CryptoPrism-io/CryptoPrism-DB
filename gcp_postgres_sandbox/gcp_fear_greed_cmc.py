import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

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

# 🔹 Create SQLAlchemy Engine for Backtest Database
def create_db_engine_backtest():
    return create_engine(f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database_bt"]}')

# 🔹 API Key
API_KEY = "92a8ca59-b7a1-4314-860d-0e6f3a58421d"

def fetch_fear_and_greed_data(api_key, limit=500, start=1):
    """ Fetches paginated Fear & Greed Index data from the API. """
    url = "https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical"

    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": api_key,
    }

    params = {
        "start": start,  # Start from record 1
        "limit": limit  # Max 500 records per request
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None


def fetch_full_year_data(api_key):
    """ Fetches Fear & Greed data for 1 year using pagination. """
    all_data = []
    start = 1  # Start index for pagination
    limit = 500  # Max per request

    while True:
        data = fetch_fear_and_greed_data(api_key, limit, start)
        if not data or "data" not in data or not data["data"]:
            break  # Stop if no more data

        all_data.extend(data["data"])
        start += limit  # Move to the next batch

        # Convert first and last timestamp to check if we reached 1 year
        timestamps = [int(entry["timestamp"]) for entry in data["data"]]
        first_date = datetime.utcfromtimestamp(timestamps[0])
        last_date = datetime.utcfromtimestamp(timestamps[-1])
        one_year_ago = datetime.utcnow() - timedelta(days=365)

        if last_date < one_year_ago:
            break  # Stop fetching if we have 1 year of data

    return all_data


def process_fear_greed_data(data):
    """ Converts JSON data into a Pandas DataFrame. """
    if data:
        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int), unit="s")
        df.rename(columns={"value": "fear_greed_index", "value_classification": "sentiment"}, inplace=True)
        df = df[df["timestamp"] >= datetime.utcnow() - timedelta(days=365)]  # Keep only last 1 year
        return df
    else:
        return pd.DataFrame()


def push_data_to_db(df, table_name="FE_FEAR_GREED_CMC"):
    """Pushes the Fear & Greed data to the database."""
    engine = create_db_engine()  # Connect to primary database
    with engine.connect() as connection:
        df.to_sql(table_name, connection, if_exists="replace", index=False)
    print(f"Data successfully pushed to {table_name}")


if __name__ == "__main__":
    api_key = API_KEY
    full_year_data = fetch_full_year_data(api_key)
    df = process_fear_greed_data(full_year_data)

    if not df.empty:
        push_data_to_db(df)
