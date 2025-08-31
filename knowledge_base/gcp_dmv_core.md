# gcp_dmv_core.py

## Overview
This script aggregates and merges multiple signal tables (oscillators, momentum, metrics, TVV, ratios) from a PostgreSQL database, processes the data, and computes summary statistics for each row.

## Key Features
- Connects to a PostgreSQL database using SQLAlchemy.
- Merges multiple signal tables on `slug` and `timestamp`.
- Cleans and filters the resulting DataFrame.
- Computes bullish, bearish, and neutral counts for each row.
- Logs progress and errors.

## Usage
1. Set your database credentials in the script.
2. Run the script:
   ```powershell
   python gcp_dmv_core.py
   ```

## Dependencies
- pandas
- numpy
- sqlalchemy
- logging
