library(RMySQL)
library(crypto2)
library(dplyr)
library(DBI)
library(RPostgres)

# Connection parameters
db_host <- "34.55.195.199"         # Public IP of your PostgreSQL instance on GCP
db_name <- "dbcp"                  # Database name
db_name_bt <- "cp_backtest"                  # Database name for bt
db_user <- "yogass09"              # Database username
db_password <- "jaimaakamakhya"    # Database password
db_port <- 5432                    # PostgreSQL port

# Establish database connections
con <- dbConnect(
  RPostgres::Postgres(),
  host = db_host,
  dbname = db_name,
  user = db_user,
  password = db_password,
  port = db_port
)

con_bt <- dbConnect(
  RPostgres::Postgres(),
  host = db_host,
  dbname = db_name_bt,
  user = db_user,
  password = db_password,
  port = db_port
)

# Check if the connection is valid
if (dbIsValid(con)) {
  print("Connection successful")
} else {
  print("Connection failed")
  stop("Database connection failed")
}

# Read crypto listings from database instead of API
print("Reading crypto listings from database...")
crypto.listings.latest <- dbReadTable(con, "crypto_listings_latest_1000")

# Alternative: Use SQL query for more control
# crypto.listings.latest <- dbGetQuery(con, "
#   SELECT * FROM crypto_listings_latest_1000 
#   WHERE cmc_rank > 0 AND cmc_rank < 2000
#   ORDER BY cmc_rank ASC
# ")

# Filter the data based on cmc_rank (if not already filtered in SQL)
crypto.listings.latest <- crypto.listings.latest %>%
  filter(cmc_rank > 0 & cmc_rank < 2000) %>%
  arrange(cmc_rank)

print(paste("Loaded", nrow(crypto.listings.latest), "coins from database"))

# Ensure the dataframe has the required columns for crypto_history()
# crypto_history() typically expects: id, slug, name, symbol
required_cols <- c("id", "slug", "name", "symbol")
missing_cols <- setdiff(required_cols, colnames(crypto.listings.latest))

if (length(missing_cols) > 0) {
  print(paste("Warning: Missing required columns:", paste(missing_cols, collapse = ", ")))
  print("Available columns:", paste(colnames(crypto.listings.latest), collapse = ", "))
}

# Get historical data using the database listings
print("Fetching historical OHLCV data...")
all_coins <- crypto_history(
  coin_list = crypto.listings.latest,
  convert = "USD",
  limit = 2000,
  start_date = Sys.Date()-1,
  end_date = Sys.Date()+1,
  sleep = 0
)

# Select required columns
all_coins <- all_coins[, c("id", "slug", "name", "symbol", "timestamp", "open","high", "low", "close", "volume", "market_cap")]

print(paste("Fetched OHLCV data for", nrow(all_coins), "records"))

# Get global crypto quotes (still from API as this is market-wide data)
print("Fetching global crypto quotes...")
crypto_global_quote <- crypto_global_quotes(
  which = "latest",
  convert = "USD",
  start_date = Sys.Date()-1,
  end_date = Sys.Date(),
  interval = "daily",
  quote = TRUE,
  requestLimit = 1,
  sleep = 0,
  wait = 60,
  finalWait = FALSE
)

# Write dataframes to database
print("Writing data to database...")

# Write global quotes
dbWriteTable(con, "crypto_global_latest", crypto_global_quote, overwrite = TRUE, row.names = FALSE)
print("✓ Written crypto_global_latest")

# Write OHLCV data to multiple tables
dbWriteTable(con, "108_1K_coins_ohlcv", all_coins, append = TRUE, row.names = FALSE)
print("✓ Appended to 108_1K_coins_ohlcv")

dbWriteTable(con, "1K_coins_ohlcv", all_coins, append = TRUE, row.names = FALSE)
print("✓ Appended to 1K_coins_ohlcv")

dbWriteTable(con_bt, "1K_coins_ohlcv", all_coins, append = TRUE, row.names = FALSE)
print("✓ Appended to 1K_coins_ohlcv (backtest db)")

# Optional: Update the listings table if needed
# dbWriteTable(con, "crypto_listings_latest_1000", crypto.listings.latest, overwrite = TRUE, row.names = FALSE)
# dbWriteTable(con_bt, "crypto_listings_latest_1000", crypto.listings.latest, overwrite = TRUE, row.names = FALSE)

print("All data processing completed successfully!")

# Close connections
dbDisconnect(con)
dbDisconnect(con_bt)
print("Database connections closed.")
