library(crypto2)
library(dplyr)
library(DBI)
library(RPostgres)
library(dotenv)

# Load environment variables from .env file (only for local development)
if (!Sys.getenv("GITHUB_ACTIONS") == "true") {
  # Find the project root by going up from the script's directory
  script_dir <- getwd()
  project_root <- normalizePath(file.path(script_dir, '..', '..'))
  env_path <- file.path(project_root, '.env')
  
  if (file.exists(env_path)) {
    dotenv::load_dot_env(file = env_path)
    print(paste("✅ Loaded .env from:", env_path))
  } else {
    print(paste("⚠️ .env file not found at:", env_path))
  }
} else {
  print("🔹 Running in GitHub Actions: Using GitHub Secrets.")
}

# Configuration (use environment variables for production)
CONFIG <- list(
  db_host = Sys.getenv("DB_HOST"),
  db_name = Sys.getenv("DB_NAME"),
  db_name_bt = Sys.getenv("DB_NAME_BT", "cp_backtest"),
  db_user = Sys.getenv("DB_USER"),
  db_password = Sys.getenv("DB_PASSWORD"),
  db_port = as.integer(Sys.getenv("DB_PORT", "5432"))
)

# Validate required environment variables
required_vars <- c("db_host", "db_user", "db_password", "db_name")
missing_vars <- c()

for (var in required_vars) {
  if (is.null(CONFIG[[var]]) || CONFIG[[var]] == "" || is.na(CONFIG[[var]])) {
    missing_vars <- c(missing_vars, var)
  }
}

if (length(missing_vars) > 0) {
  stop(paste("❌ Missing environment variables:", paste(missing_vars, collapse = ", ")))
}

print(paste("✅ Database Configuration Loaded: DB_HOST =", CONFIG$db_host, "DB_PORT =", CONFIG$db_port))

# Establish database connections
con <- dbConnect(
  RPostgres::Postgres(),
  host = CONFIG$db_host,
  dbname = CONFIG$db_name,
  user = CONFIG$db_user,
  password = CONFIG$db_password,
  port = CONFIG$db_port
)

con_bt <- dbConnect(
  RPostgres::Postgres(),
  host = CONFIG$db_host,
  dbname = CONFIG$db_name_bt,
  user = CONFIG$db_user,
  password = CONFIG$db_password,
  port = CONFIG$db_port
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

# Remove duplicates from fetched data using timestamp-based approach
print("Checking for existing data to prevent duplicates...")

# Get unique timestamps from fetched data
unique_timestamps <- unique(all_coins$timestamp)
print(paste("Fetched data contains", length(unique_timestamps), "unique timestamps"))

# Format timestamps for SQL IN clause
timestamp_list <- paste0("'", unique_timestamps, "'", collapse = ",")

# Query all tables for existing records with these timestamps
existing_records <- data.frame()

tryCatch({
  # Check main db - 108_1K_coins_ohlcv table
  existing_108k <- dbGetQuery(con, paste0("SELECT slug, timestamp FROM \"108_1K_coins_ohlcv\" WHERE timestamp IN (", timestamp_list, ")"))
  if (nrow(existing_108k) > 0) {
    existing_108k$source <- "108_1K_coins_ohlcv"
    existing_records <- rbind(existing_records, existing_108k)
  }

  # Check main db - 1K_coins_ohlcv table
  existing_1k_main <- dbGetQuery(con, paste0("SELECT slug, timestamp FROM \"1K_coins_ohlcv\" WHERE timestamp IN (", timestamp_list, ")"))
  if (nrow(existing_1k_main) > 0) {
    existing_1k_main$source <- "1K_coins_ohlcv_main"
    existing_records <- rbind(existing_records, existing_1k_main)
  }

  # Check backtest db - 1K_coins_ohlcv table
  existing_1k_bt <- dbGetQuery(con_bt, paste0("SELECT slug, timestamp FROM \"1K_coins_ohlcv\" WHERE timestamp IN (", timestamp_list, ")"))
  if (nrow(existing_1k_bt) > 0) {
    existing_1k_bt$source <- "1K_coins_ohlcv_backtest"
    existing_records <- rbind(existing_records, existing_1k_bt)
  }

  print(paste("Found", nrow(existing_records), "existing records across all tables"))

}, error = function(e) {
  print(paste("Warning: Error checking existing data:", e$message))
  print("Proceeding with all data (may cause duplicates)")
})

# Remove duplicates from all_coins
if (nrow(existing_records) > 0) {
  # Create composite keys for comparison
  existing_records$key <- paste(existing_records$slug, existing_records$timestamp, sep = "_")
  all_coins$key <- paste(all_coins$slug, all_coins$timestamp, sep = "_")

  # Filter out existing records
  clean_coins <- all_coins[!all_coins$key %in% existing_records$key, ]
  clean_coins$key <- NULL  # Remove temporary key column

  print(paste("Removed", nrow(all_coins) - nrow(clean_coins), "duplicate records"))
  print(paste("Will insert", nrow(clean_coins), "new records to all tables"))
} else {
  clean_coins <- all_coins
  print("No existing records found, will insert all fetched data")
}

# Insert clean data to all OHLCV tables using simple append
if (nrow(clean_coins) > 0) {
  # Insert to 108_1K_coins_ohlcv (main db)
  dbWriteTable(con, "108_1K_coins_ohlcv", clean_coins, append = TRUE, row.names = FALSE)
  print("✓ Inserted clean data to 108_1K_coins_ohlcv (main db)")

  # Insert to 1K_coins_ohlcv (main db)
  dbWriteTable(con, "1K_coins_ohlcv", clean_coins, append = TRUE, row.names = FALSE)
  print("✓ Inserted clean data to 1K_coins_ohlcv (main db)")

  # Insert to 1K_coins_ohlcv (backtest db)
  dbWriteTable(con_bt, "1K_coins_ohlcv", clean_coins, append = TRUE, row.names = FALSE)
  print("✓ Inserted clean data to 1K_coins_ohlcv (backtest db)")
} else {
  print("✓ No new data to insert - all records already exist")
}

# Optional: Update the listings table if needed
# dbWriteTable(con, "crypto_listings_latest_1000", crypto.listings.latest, overwrite = TRUE, row.names = FALSE)
# dbWriteTable(con_bt, "crypto_listings_latest_1000", crypto.listings.latest, overwrite = TRUE, row.names = FALSE)

print("All data processing completed successfully!")

# Close connections
dbDisconnect(con)
dbDisconnect(con_bt)
print("Database connections closed.")
