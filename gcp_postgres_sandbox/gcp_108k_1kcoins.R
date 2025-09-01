library(crypto2)
library(dplyr)
library(DBI)
library(RPostgres)
library(dotenv)

# Load environment variables from .env file (only for local development)
if (!Sys.getenv("GITHUB_ACTIONS") == "true") {
  # Find the project root by going up from the script's directory
  script_dir <- dirname(normalizePath(sys.frame(1)$ofile))
  project_root <- normalizePath(file.path(script_dir, '..'))
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
