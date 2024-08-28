
# @title LIBRARY
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

import time
start_time = time.time()

# @title  AWS/Cloud DB connect
import mysql.connector
import pandas as pd

# Establishing the connection
con = mysql.connector.connect(
    host="dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com",
    port=3306,
    user="yogass09",
    password="jaimaakamakhya",
    database="dbcp"
)

# @title SQL Query Connection to AWS for Data Listing

# Executing the query and fetching the results directly into a pandas DataFrame
query = "SELECT * FROM 1K_coins_ohlcv"
all_coins_ohlcv_filtered = pd.read_sql_query(query, con)

query = "SELECT * FROM crypto_listings_latest_1000"
top_1000_cmc_rank = pd.read_sql_query(query, con)


con.close()

# @title  Enhancing Function Definition Through Grouping and Indexing Techniques
df=all_coins_ohlcv_filtered
df.set_index('symbol', inplace=True)
# Ensure the timestamp column is in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Sort the DataFrame by 'slug' and 'timestamp' columns
df.sort_values(by=['slug', 'timestamp'], inplace=True)

# Perform time-series calculations within each group (each cryptocurrency)
grouped = df.groupby('slug')



"""# METRICS"""

# @title  Enhancing Function Definition Through Grouping and Indexing Techniques

df=all_coins_ohlcv_filtered
# Ensure the timestamp column is in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Sort the DataFrame by 'slug' and 'timestamp' columns
df.sort_values(by=['slug', 'timestamp'], inplace=True)

# Perform time-series calculations within each group (each cryptocurrency)
grouped = df.groupby('slug')

# @title Calculate Daily Percentage Change & Calculate Cumulative Returns

# Calculate percentage change for each cryptocurrency
df['m_pct_1d'] = grouped['close'].pct_change()

# Calculate cumulative returns for each cryptocurrency
df['d_pct_cum_ret'] = (1 + df['m_pct_1d']).groupby(df['slug']).cumprod() - 1

# @title Calculate All-Time High (ATH) and All-Time Low (ATL)

# Step 1: Calculate all-time high and all-time low, and their corresponding dates
all_time_high = df.groupby('slug')['high'].max().reset_index()
all_time_high.columns = ['slug', 'v_met_ath']

all_time_low = df.groupby('slug')['low'].min().reset_index()
all_time_low.columns = ['slug', 'v_met_atl']

# Merge all-time high and low into the DataFrame
df = pd.merge(df, all_time_high, on='slug', how='left')
df = pd.merge(df, all_time_low, on='slug', how='left')

# Step 2: Find the dates for ATH and ATL for each cryptocurrency
ath_dates = df[df['high'] == df['v_met_ath']].groupby('slug')['timestamp'].max().reset_index()
ath_dates.columns = ['slug', 'ath_date']

atl_dates = df[df['low'] == df['v_met_atl']].groupby('slug')['timestamp'].max().reset_index()
atl_dates.columns = ['slug', 'atl_date']

# Merge the ATH and ATL dates back into the original DataFrame
df = pd.merge(df, ath_dates, on='slug', how='left')
df = pd.merge(df, atl_dates, on='slug', how='left')

# Step 3: Calculate the number of days since ATH and ATL
current_date = pd.Timestamp.now()
df['d_met_ath_days'] = (current_date - df['ath_date']).dt.days
df['d_met_atl_days'] = (current_date - df['atl_date']).dt.days

# Convert days to weeks and months
df['_met_ath_week'] = df['d_met_ath_days'] // 7
df['_met_ath_month'] = df['d_met_ath_days'] // 30

df['_met_atl_week'] = df['d_met_atl_days'] // 7
df['_met_atl_month'] = df['d_met_atl_days'] // 30

# @title Keeping Only the Latest Data for Each Cryptocurrency Slug
# prompt: in df .. keep only latest or max of timestamp for every slug #... by that i mean .. i need only all slugs one row data which is ;atest

# Group by 'slug' and get the row with the maximum timestamp
met_df1 = df.loc[df.groupby('slug')['timestamp'].idxmax()]
import pandas as pd
# Perform the inner join
met_df1 = pd.merge(met_df1, top_1000_cmc_rank[['slug', 'date_added', 'last_updated']], on='slug', how='inner')

# @title Analysis for CoinAge and Mcap  Metrics

df=met_df1
# Convert 'date_added' and 'last_updated' columns to datetime without time zone information
df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce').dt.tz_localize(None)
df['last_updated'] = pd.to_datetime(df['last_updated'], errors='coerce').dt.tz_localize(None)

# Ensure there are no NaT values in 'date_added' or 'last_updated'
df = df.dropna(subset=['date_added', 'last_updated'])

# Step 1: Calculate CoinAge in Days, Months, and Years
current_date = pd.Timestamp.now().normalize()  # Normalize to remove time part if present
df['d_met_coin_age_d'] = (current_date - df['date_added']).dt.days
df['d_met_coin_age_m'] = df['d_met_coin_age_d'] // 30  # Approximate months
df['d_met_coin_age_y'] = df['d_met_coin_age_d'] // 365  # Approximate years

# Step 2: Categorize Market Capitalization
def categorize_market_cap(market_cap):
    if market_cap >= 1e12:
        return '1T-100B'
    elif market_cap >= 1e11:
        return '100B-10B'
    elif market_cap >= 1e10:
        return '10B-1B'
    elif market_cap >= 1e9:
        return '1B-100M'
    elif market_cap >= 1e7:
        return '100M-1M'
    else:
        return 'Under1M'

df['m_cap_cat'] = df['market_cap'].apply(categorize_market_cap)

metrics=df

# prompt: drop col 4:9 in metrics

metrics = metrics.drop(metrics.columns[4:10], axis=1)
metrics.info()

# @title SQLalchemy to push data to aws db (mysql)

from sqlalchemy import create_engine

# Create a SQLAlchemy engine to connect to the MySQL database
engine = create_engine('mysql+mysqlconnector://yogass09:jaimaakamakhya@dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com:3306/dbcp')

# Write the DataFrame to a new table in the database
metrics.to_sql('FE_METRICS', con=engine, if_exists='replace', index=False)

print("Metrics DataFrame uploaded to AWS MySQL database successfully!")

# @title Metrics Signals
# m_pct_1d signal
metrics['m_pct_1d_signal'] = np.where(metrics['m_pct_1d'] > 0, 1, -1)

# d_pct_cum_ret signal
metrics['d_pct_cum_ret_signal'] = np.where(metrics['d_pct_cum_ret'] > 0, 1, -1)

# _met_ath_month signal
metrics['d_met_ath_month_signal'] = ((100 - metrics['_met_ath_month']) * 2)/100

# Define the mapping for market cap categories
market_cap_signal = {
    '100M-1M': 0.25,
    '1B-100M': .4,
    'Under1M': 0.1,
    '10B-1B': 0.5,
    '1T-100B': 1,
    '100B-10B': 0.75
}

# Create a new column 'd_market_cap_signal' based on the mapping
metrics['d_market_cap_signal'] = metrics['m_cap_cat'].map(market_cap_signal)


# d_met_coin_age_y signal
metrics['d_met_coin_age_y_signal'] = np.where(
    metrics['d_met_coin_age_y'] < 1, 0,
    np.where(metrics['d_met_coin_age_y'] >= 1, 1 - (1 / metrics['d_met_coin_age_y']), 0)
)

metrics.info()

metrics_signal = metrics.drop(metrics.columns[3:22], axis=1)

metrics_signal.info()

# @title SQLalchemy to push data to aws db (mysql)

from sqlalchemy import create_engine

# Create a SQLAlchemy engine to connect to the MySQL database
#engine = create_engine('mysql+mysqlconnector://yogass09:jaimaakamakhya@dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com:3306/dbcp')

# Write the DataFrame to a new table in the database
metrics_signal.to_sql('FE_METRICS_SIGNAL', con=engine, if_exists='replace', index=False)

print("FE_METRICS_SIGNAL DataFrame uploaded to AWS MySQL database successfully!")

metrics_signal.info()

end_time = time.time()
elapsed_time_seconds = end_time - start_time
elapsed_time_minutes = elapsed_time_seconds / 60

print(f"Cell execution time: {elapsed_time_minutes:.2f} minutes")



"""# PCT_CHANGE"""

# @title  Enhancing Function Definition Through Grouping and Indexing Techniques

df=all_coins_ohlcv_filtered
# Ensure the timestamp column is in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Sort the DataFrame by 'slug' and 'timestamp' columns
df.sort_values(by=['slug', 'timestamp'], inplace=True)

# Perform time-series calculations within each group (each cryptocurrency)
grouped = df.groupby('slug')

# @title  VaR & CVaR
import pandas as pd
# Calculate percentage change for each cryptocurrency
df['m_pct_1d'] = grouped['close'].pct_change()
# Calculate cumulative returns for each cryptocurrency
df['d_pct_cum_ret'] = (1 + df['m_pct_1d']).groupby(df['slug']).cumprod() - 1

# Define the confidence level, e.g., 95%
confidence_level = 0.95

# Calculate Historical VaR for each cryptocurrency
VaR_df = df.groupby('slug').apply(lambda x: x['m_pct_1d'].quantile(1 - confidence_level))
VaR_df = VaR_df.reset_index(name='d_pct_var')

# Calculate CVaR for each cryptocurrency
CVaR_df = df.groupby('slug').apply(lambda x: x['m_pct_1d'][x['m_pct_1d'] <= x['m_pct_1d'].quantile(1 - confidence_level)].mean())
CVaR_df = CVaR_df.reset_index(name='d_pct_cvar')

# Merge VaR and CVaR back into the original DataFrame
df = df.merge(VaR_df, on='slug', how='left')
df = df.merge(CVaR_df, on='slug', how='left')

# @title  Vol% change

# Calculate daily volume percentage (VolD%)
df['d_pct_vol_1d'] = df.groupby('slug')['volume'].pct_change()


import numpy as np
# Calculate the latest weekly volume percentage (VolW%)
# Ensure the DataFrame is sorted by 'timestamp' for each 'slug'
# Sort by 'timestamp' in descending order
df_sorted = df.sort_values(by='timestamp', ascending=True)


# Calculate latest weekly volume percentage
def latest_weekly_vol_percentage(group):
    if len(group) < 7:
        return np.nan
    latest_week = group.head(7)
    return latest_week['volume'].pct_change().iloc[-1] * 100

df['d_pct_vol_1w'] = df.groupby('slug').apply(lambda x: latest_weekly_vol_percentage(x)).reset_index(level=0, drop=True)

# Calculate the latest monthly volume percentage (VolM%)
def latest_monthly_vol_percentage(group):
    if len(group) < 30:
        return np.nan
    latest_month = group.head(30)
    return latest_month['volume'].pct_change().iloc[-1] * 100

df['d_pct_vol_1m'] = df.groupby('slug').apply(lambda x: latest_monthly_vol_percentage(x)).reset_index(level=0, drop=True)

# @title Keeping Only Latest Date for Each Slug
# Group by 'slug' and get the row with the maximum timestamp
pct_change = df.loc[df.groupby('slug')['timestamp'].idxmax()]

import numpy as np
# Drop columns with infinite values
pct_change = pct_change.replace([np.inf, -np.inf], np.nan).dropna(axis=1)

# Drop columns 4 to 10
pct_change = pct_change.drop(pct_change.columns[4:10], axis=1)

pct_change.info()

# @title SQLalchemy to push data to aws db (mysql)

from sqlalchemy import create_engine

# Create a SQLAlchemy engine to connect to the MySQL database
engine = create_engine('mysql+mysqlconnector://yogass09:jaimaakamakhya@dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com:3306/dbcp')

# Write the DataFrame to a new table in the database
pct_change.to_sql('FE_PCT_CHANGE', con=engine, if_exists='replace', index=False)

print("pct_change DataFrame uploaded to AWS MySQL database successfully!")



"""# TVV"""

# @title  Enhancing Function Definition Through Grouping and Indexing Techniques
df=all_coins_ohlcv_filtered
# Ensure the timestamp column is in datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Sort the DataFrame by 'slug' and 'timestamp' columns
df.sort_values(by=['slug', 'timestamp'], inplace=True)

# Perform time-series calculations within each group (each cryptocurrency)
grouped = df.groupby('slug')

# @title On-Balance Volume (OBV)

# Assuming df is your DataFrame and it is already sorted by 'slug' and 'timestamp'

def calculate_obv(group):
    # Initialize OBV list
    obv = [0]  # Start with zero for the first row
    for i in range(1, len(group)):
        if group['close'].iloc[i] > group['close'].iloc[i - 1]:
            obv.append(obv[-1] + group['volume'].iloc[i])
        elif group['close'].iloc[i] < group['close'].iloc[i - 1]:
            obv.append(obv[-1] - group['volume'].iloc[i])
        else:
            obv.append(obv[-1])
    return pd.Series(obv, index=group.index)

# Ensure the DataFrame has unique indices and reset if necessary
df = df.reset_index(drop=True)

# Group by 'slug' and apply OBV calculation
df['obv'] = df.groupby('slug').apply(calculate_obv).reset_index(level=0, drop=True)

# Recalculate the grouped DataFrame after adding the 'obv' column
grouped = df.groupby('slug')
df['m_tvv_obv_1d'] = grouped['obv'].pct_change()

# @title Moving Averages (SMA and EMA)

# Calculate the Simple Moving Average (SMA) for 9 and 18 periods
df['SMA9'] = grouped['close'].transform(lambda x: x.rolling(window=9).mean())
df['SMA18'] = grouped['close'].transform(lambda x: x.rolling(window=18).mean())

# Calculate the Exponential Moving Average (EMA) for 9 and 18 periods
df['EMA9'] = grouped['close'].transform(lambda x: x.ewm(span=9, adjust=False).mean())
df['EMA18'] = grouped['close'].transform(lambda x: x.ewm(span=18, adjust=False).mean())

# Calculate the Simple Moving Average (SMA) for 21 periods
df['SMA21'] = df.groupby('slug')['close'].transform(lambda x: x.rolling(window=21).mean())
df['SMA108'] = df.groupby('slug')['close'].transform(lambda x: x.rolling(window=108).mean())

# Calculate EMA (21-period)
df['EMA21'] = df.groupby('slug')['close'].transform(lambda x: x.ewm(span=21, adjust=False).mean())
# Calculate EMA (108-period)
df['EMA108'] = df.groupby('slug')['close'].transform(lambda x: x.ewm(span=108, adjust=False).mean())

# @title Average True Range (ATR)
def calculate_atr(group, window=14):
    # Calculate True Range
    group['prev_close'] = group['close'].shift(1)
    group['tr1'] = group['high'] - group['low']
    group['tr2'] = abs(group['high'] - group['prev_close'])
    group['tr3'] = abs(group['low'] - group['prev_close'])
    group['TR'] = group[['tr1', 'tr2', 'tr3']].max(axis=1)

    # Calculate ATR
    group['ATR'] = group['TR'].rolling(window=window).mean()

    return group

# Apply the function to each cryptocurrency
df = df.groupby('slug').apply(calculate_atr).reset_index(level=0, drop=True)

# @title Ketler and Donchain

def calculate_keltner_channels(group, window_ema=21, window_atr=14):
    # Calculate EMA
    group['EMA21'] = group['close'].ewm(span=window_ema, adjust=False).mean()

    # Calculate ATR
    group = calculate_atr(group, window=window_atr) # calculate_atr is now defined before being called

    # Calculate Keltner Channels
    group['Keltner_Upper'] = group['EMA21'] + (group['ATR'] * 1.5)
    group['Keltner_Lower'] = group['EMA21'] - (group['ATR'] * 1.5)

    return group

# Apply the function to each cryptocurrency
df = df.groupby('slug').apply(calculate_keltner_channels).reset_index(level=0, drop=True)

def calculate_donchian_channels(group, window=20):
    # Calculate Donchian Channels
    group['Donchian_Upper'] = group['high'].rolling(window=window).max()
    group['Donchian_Lower'] = group['low'].rolling(window=window).min()

    return group

# Reset the index before applying the function (if needed)
df = df.reset_index(drop=True) # drop=True to avoid old index being added as a column

# Apply the function to each cryptocurrency
df = df.groupby('slug').apply(calculate_donchian_channels).reset_index(level=0, drop=True)

# @title Vwap / ADL / CMF
def calculate_vwap(group):
    # Calculate typical price for each period
    group['typical_price'] = (group['high'] + group['low'] + group['close']) / 3

    # Calculate the cumulative sum of typical price * volume
    group['cum_price_volume'] = (group['typical_price'] * group['volume']).cumsum()

    # Calculate the cumulative sum of volume
    group['cum_volume'] = group['volume'].cumsum()

    # Calculate VWAP
    group['VWAP'] = group['cum_price_volume'] / group['cum_volume']

    return group

# Reset the index before applying the function (if needed)
df = df.reset_index(drop=True) # drop=True to avoid old index being added as a column


# Group by 'slug' to calculate VWAP for each cryptocurrency
df = df.groupby('slug').apply(calculate_vwap).reset_index(level=0, drop=True)

import pandas as pd

# Correct ADL Calculation
df['ADL'] = ((df['close'] - df['low'] - (df['high'] - df['close'])) / (df['high'] - df['low'])) * df['volume']

def calculate_cmf(group, period):
    # Ensure 'slug' is not an index
    group = group.reset_index(drop=True)

    # Correct ADL Calculation
    group['ADL'] = ((group['close'] - group['low'] - (group['high'] - group['close'])) / (group['high'] - group['low'])) * group['volume']

    # Calculate cumulative ADL and volume
    group['cum_adl'] = group['ADL'].cumsum()
    group['cum_volume'] = group['volume'].cumsum()

    # Calculate CMF, handling potential division by zero
    epsilon = 1e-10  # Small constant to avoid division by zero
    group['CMF'] = group['cum_adl'].rolling(window=period).sum() / (group['cum_volume'].rolling(window=period).sum() + epsilon)

    return group

# Define the period for CMF calculation
period = 21

# Reset the index before applying the function (if needed)
df = df.reset_index(drop=True)  # drop=True to avoid old index being added as a column

# Group by 'slug' to calculate CMF for each cryptocurrency
df = df.groupby('slug').apply(calculate_cmf, period=period).reset_index(level=0, drop=True)

# @title SQLalchemy to push (FE) data to aws db (mysql)

tvv=df
# Get the latest timestamp
latest_timestamp = tvv['timestamp'].max()

# Filter the DataFrame for rows where timestamp equals the latest timestamp
tvv = tvv[tvv['timestamp'] == latest_timestamp]

# Drop columns 4 to 10
tvv = tvv.drop(tvv.columns[4:10], axis=1)

# Replace infinite values with NaN
tvv = tvv.replace([np.inf, -np.inf], np.nan) # Replace inf values before pushing to SQL

# Create a SQLAlchemy engine to connect to the MySQL database
#engine = create_engine('mysql+mysqlconnector://yogass09:jaimaakamakhya@dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com:3306/dbcp')

# Write the DataFrame to a new table in the database
tvv.to_sql('FE_TVV', con=engine, if_exists='replace', index=False)

print("pct_change DataFrame uploaded to AWS MySQL database successfully!")

# @title TVV Binary Signals
columns_to_drop = ['name', 'ref_cur_id', 'ref_cur_name', 'time_open',
                   'time_close', 'time_high', 'time_low', 'open', 'high', 'low',
                   'close', 'volume', 'market_cap']

# Drop the specified columns
df_bin = df.drop(columns=columns_to_drop, errors='ignore')

df_bin['m_tvv_obv_1d_binary'] = df_bin['m_tvv_obv_1d'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))

# prompt: SMA9, SMA18, EMA9, EMA18, SMA21, SMA108, EMA21, EMA108
# mujhe crossover calculate karne hai so 9 ka 18 ke sath and 21 ka 108 ke sath hoga jab 9 18 se jyada hai toh 1 jab 9 18 se kaam hai tab -1 same jab 21 jyada hai 108 se tab 1 and jab 21 kaam hai 108 se tab -1
# mera naming conventing hai d_tvv_sma...

# Calculate crossovers for SMA9 and SMA18
df_bin['d_tvv_sma9_18'] = (df_bin['SMA9'] > df_bin['SMA18']).astype(int) * 2 - 1

# Calculate crossovers for EMA9 and EMA18
df_bin['d_tvv_ema9_18'] = (df_bin['EMA9'] > df_bin['EMA18']).astype(int) * 2 - 1

# Calculate crossovers for SMA21 and SMA108
df_bin['d_tvv_sma21_108'] = (df_bin['SMA21'] > df_bin['SMA108']).astype(int) * 2 - 1

# Calculate crossovers for EMA21 and EMA108
df_bin['d_tvv_ema21_108'] = (df_bin['EMA21'] > df_bin['EMA108']).astype(int) * 2 - 1

# Assuming 'CMF' column exists in df_bin
threshold = 0  # Adjust this threshold as needed
# Derive bullish/bearish signals based on CMF crossing the threshold
df_bin['m_tvv_cmf'] = 0  # Initialize the new column with zeros
df_bin.loc[df_bin['CMF'] > threshold, 'm_tvv_cmf'] = 1  # Bullish signal
df_bin.loc[df_bin['CMF'] < threshold, 'm_tvv_cmf'] = -1 # Bearish signal

df_bin.info()

# @title SQLalchemy to push (FE_SIGNALS) data to aws db (mysql)

# Drop columns by their index positions
df_bin.drop(df_bin.columns[3:31], axis=1, inplace=True)
tvv_signals=df_bin

# Get the latest timestamp
latest_timestamp = df['timestamp'].max()

# Filter the DataFrame for rows where timestamp equals the latest timestamp
tvv_signals = tvv_signals[tvv_signals['timestamp'] == latest_timestamp]

# Replace infinite values with NaN
tvv_signals = tvv_signals.replace([np.inf, -np.inf], np.nan) # Replace inf values before pushing to SQL

# Create a SQLAlchemy engine to connect to the MySQL database
#engine = create_engine('mysql+mysqlconnector://yogass09:jaimaakamakhya@dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com:3306/dbcp')

# Write the DataFrame to a new table in the database
tvv_signals.to_sql('FE_TVV_SIGNALS', con=engine, if_exists='replace', index=False)

print("tvv_signals DataFrame uploaded to AWS MySQL database successfully!")

tvv_signals.info()



"""# end of script

"""

# @title time cal and engine close

end_time = time.time()
elapsed_time_seconds = end_time - start_time
elapsed_time_minutes = elapsed_time_seconds / 60

print(f"Cell execution time: {elapsed_time_minutes:.2f} minutes")



engine.dispose()
con.close()
