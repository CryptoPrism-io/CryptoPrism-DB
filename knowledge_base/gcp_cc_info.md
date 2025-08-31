# gcp_cc_info.py

## Overview
This script connects to a PostgreSQL database, retrieves the `slug` field from the `crypto_listings_latest_1000` table, and fetches detailed cryptocurrency info from the CoinMarketCap API in batches.

## Key Features
- Connects to a PostgreSQL database using psycopg2.
- Retrieves a list of crypto slugs.
- Fetches detailed info from CoinMarketCap API in batches of 199 slugs (to avoid API errors).
- Handles API rate limits with sleep intervals.

## Usage
1. Set your database credentials and CoinMarketCap API key in the script.
2. Run the script:
   ```powershell
   python gcp_cc_info.py
   ```

## Dependencies
- mysql-connector
- pandas
- psycopg2
- sqlalchemy
- requests
