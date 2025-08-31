# gcp_dmv_osc.py

## Overview
This script fetches OHLCV data for the top 1000 coins from a PostgreSQL database, calculates oscillator indicators, and prepares the data for further analysis.

## Key Features
- Connects to PostgreSQL databases using SQLAlchemy.
- Fetches OHLCV data for coins with `cmc_rank` <= 1000 and within the last 110 days.
- Calculates 1-day percentage change and other oscillator metrics.
- Logs progress and errors.

## Usage
1. Set your database credentials in the script.
2. Run the script:
   ```powershell
   python gcp_dmv_osc.py
   ```

## Dependencies
- pandas
- numpy
- sqlalchemy
- logging
