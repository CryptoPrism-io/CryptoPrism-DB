# gcp_dmv_met.py

## Overview
This script connects to Google Cloud PostgreSQL databases, retrieves OHLCV and listing data for the top 1000 coins, and prepares the data for further analysis and visualization.

## Key Features
- Connects to two PostgreSQL databases (main and backtest) using SQLAlchemy.
- Retrieves OHLCV data and latest listings for the top 1000 coins.
- Sets up the DataFrame for further grouping and analysis.
- Suppresses warnings for cleaner output.

## Usage
1. Install required Python packages:
   ```powershell
   pip install pandas numpy matplotlib seaborn sqlalchemy
   ```
2. Set your database credentials in the script.
3. Run the script:
   ```powershell
   python gcp_dmv_met.py
   ```

## Dependencies
- pandas
- numpy
- matplotlib
- seaborn
- sqlalchemy
