# gcp_dmv_mom_backtest.py

## Overview
This script fetches all historical OHLCV data for the top 1000 coins from a PostgreSQL database for backtesting, calculates momentum indicators (such as RSI), and prepares the data for further analysis.

## Key Features
- Connects to PostgreSQL databases using SQLAlchemy.
- Fetches all historical OHLCV data for coins with `cmc_rank` <= 1000.
- Calculates RSI for multiple periods.
- Logs progress and errors.

## Usage
1. Set your database credentials as environment variables or in the script.
2. Run the script:
   ```powershell
   python gcp_dmv_mom_backtest.py
   ```

## Dependencies
- pandas
- numpy
- sqlalchemy
- logging
