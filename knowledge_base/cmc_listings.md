# cmc_listings.py

## Overview
This script fetches the latest cryptocurrency listings from the CoinMarketCap API, processes and cleans the data to match a 30-field schema, filters for the top 1000 coins by rank, and uploads the results to a database table.

## Key Features
- Fetches up to 5000 latest crypto listings from CoinMarketCap.
- Cleans and flattens the JSON data into a pandas DataFrame.
- Ensures all required columns exist and are properly formatted.
- Filters for the top 1000 coins by `cmc_rank`.
- Uploads the processed data to a specified database table using SQLAlchemy.

## Usage
1. Set the following environment variables:
   - `CMC_API_KEY`: Your CoinMarketCap API key.
   - `DB_URL`: SQLAlchemy-compatible database URL.
   - `DB_TABLE` (optional): Table name to upload data (default: `crypto_listings`).

2. Run the script:
   ```powershell
   python cmc_listings.py
   ```

## Dependencies
- requests
- pandas
- sqlalchemy

Install dependencies with:
```powershell
pip install requests pandas sqlalchemy
```

## Output
- Prints the number of records processed and uploaded.
- Writes the top 1000 coins' data to the specified database table.
