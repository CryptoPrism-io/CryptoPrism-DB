# gcp_dmv_mom.py

## Overview
This script fetches recent OHLCV data for the top 1000 coins from a PostgreSQL database, calculates momentum indicators (such as RSI), and prepares the data for further analysis.

## Key Features
- Connects to PostgreSQL databases using SQLAlchemy.
- Fetches OHLCV data for coins with `cmc_rank` <= 1000 and within the last 110 days.
- Calculates RSI for multiple periods.
- Logs progress and errors.

## Usage
1. Set your database credentials in the script.
2. Run the script:
   ```powershell
   python gcp_dmv_mom.py
   ```

## Dependencies
- pandas
- numpy
- sqlalchemy
- logging
