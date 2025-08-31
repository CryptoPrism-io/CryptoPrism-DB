# gcp_108k_1kcoins.R

## Overview
This R script connects to Google Cloud PostgreSQL databases, reads the latest crypto listings, filters for coins with a valid cmc_rank, and prepares the data for further analysis or historical data retrieval.

## Key Features
- Connects to two PostgreSQL databases (main and backtest) using RPostgres.
- Reads the `crypto_listings_latest_1000` table.
- Filters coins with `cmc_rank` between 1 and 1999.
- Prepares the data for downstream analysis.

## Usage
1. Install required R packages:
   ```R
   install.packages(c('RMySQL', 'crypto2', 'dplyr', 'DBI', 'RPostgres'))
   ```
2. Set your database credentials in the script.
3. Run the script in your R environment.

## Dependencies
- RMySQL
- crypto2
- dplyr
- DBI
- RPostgres
