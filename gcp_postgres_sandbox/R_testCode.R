library(RMySQL)
library(crypto2)
library(dplyr)
library(DBI)
library(RPostgres)
library(lubridate)

# Connection parameters (consider using environment variables for security)
db_host <- "34.55.195.199"
db_name <- "dbcp"
db_name_bt <- "cp_backtest"
db_user <- "yogass09"
db_password <- "jaimaakamakhya"
db_port <- 5432

# Function to get the latest timestamp from database
get_latest_timestamp <- function(connection, table_name) {
  query <- sprintf("SELECT MAX(timestamp) as max_timestamp FROM %s", table_name)
  
  tryCatch({
    result <- dbGetQuery(connection, query)
    if (!is.null(result$max_timestamp) && !is.na(result$max_timestamp)) {
      # Convert to POSIXct if it's not already
      latest_timestamp <- as.POSIXct(result$max_timestamp, tz = "UTC")
      return(latest_timestamp)
    } else {
      # If no data exists, return a default start date (e.g., 7 days ago)
      return(Sys.Date() - 7)
    }
  }, error = function(e) {
    print(paste("Error getting latest timestamp:", e$message))
    # Return default start date on error
    return(Sys.Date() - 7)
  })
}

# Establish connections
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

# Check if connections are valid
if (!dbIsValid(con) || !dbIsValid(con_bt)) {
  stop("Database connection failed")
}

print("Connections successful")

# Get the latest timestamp from the main OHLCV table
latest_timestamp <- get_latest_timestamp(con, "1K_coins_ohlcv")
print(paste("Latest timestamp in database:", latest_timestamp))

# Calculate start date (add a small buffer to avoid missing data)
# Add 1 second to avoid fetching the last record again
start_date <- as.Date(latest_timestamp + 1)

# If the latest timestamp is very recent (within last hour), skip the update
if (difftime(Sys.time(), latest_timestamp, units = "hours") < 1) {
  print("Data is already up to date (less than 1 hour old). Skipping update.")
  dbDisconnect(con)
  dbDisconnect(con_bt)
  stop("No update needed")
}

print(paste("Fetching data from:", start_date))

# Fetch cryptocurrency listings
tryCatch({
  crypto.listings.latest <- crypto_listings(
    which = "latest",
    convert = "USD",
    limit = 5000,
    start_date = start_date,
    end_date = Sys.Date(),
    interval = "day",
    quote = TRUE,
    sort = "cmc_rank",
    sort_dir = "asc",
    sleep = 0,
    wait = 0,
    finalWait = FALSE
  )
  
  # Filter the data based on cmc_rank
  crypto.listings.latest <- crypto.listings.latest %>%
    filter(cmc_rank > 0 & cmc_rank < 2000)
  
  print(paste("Fetched", nrow(crypto.listings.latest), "cryptocurrency listings"))
  
}, error = function(e) {
  print(paste("Error fetching crypto listings:", e$message))
  dbDisconnect(con)
  dbDisconnect(con_bt)
  stop("Failed to fetch crypto listings")
})

# Fetch historical data for all coins
tryCatch({
  all_coins <- crypto_history(
    coin_list = crypto.listings.latest,
    convert = "USD",
    limit = 2000,
    start_date = start_date,
    end_date = Sys.Date() + 1,
    sleep = 0
  )
  
  # Select relevant columns
  all_coins <- all_coins[, c("id", "slug", "name", "symbol", "timestamp", 
                              "open", "high", "low", "close", "volume", "market_cap")]
  
  # Remove any duplicates based on id and timestamp
  all_coins <- all_coins %>%
    distinct(id, timestamp, .keep_all = TRUE)
  
  print(paste("Fetched", nrow(all_coins), "OHLCV records"))
  
}, error = function(e) {
  print(paste("Error fetching crypto history:", e$message))
  dbDisconnect(con)
  dbDisconnect(con_bt)
  stop("Failed to fetch crypto history")
})

# Fetch global quotes
tryCatch({
  crypto_global_quote <- crypto_global_quotes(
    which = "latest",
    convert = "USD",
    start_date = start_date,
    end_date = Sys.Date(),
    interval = "daily",
    quote = TRUE,
    requestLimit = 1,
    sleep = 0,
    wait = 60,
    finalWait = FALSE
  )
  
  print(paste("Fetched", nrow(crypto_global_quote), "global quote records"))
  
}, error = function(e) {
  print(paste("Error fetching global quotes:", e$message))
  # Continue even if global quotes fail
})

# Write data to databases
# Function to safely write data
safe_db_write <- function(connection, table_name, data, append = FALSE, overwrite = FALSE) {
  if (nrow(data) > 0) {
    tryCatch({
      dbWriteTable(connection, table_name, data, 
                   append = append, 
                   overwrite = overwrite, 
                   row.names = FALSE)
      print(paste("Successfully wrote", nrow(data), "rows to", table_name))
      return(TRUE)
    }, error = function(e) {
      print(paste("Error writing to", table_name, ":", e$message))
      return(FALSE)
    })
  } else {
    print(paste("No data to write to", table_name))
    return(FALSE)
  }
}

# Write to main database
safe_db_write(con, "crypto_listings_latest_1000", crypto.listings.latest, overwrite = TRUE)
safe_db_write(con, "crypto_global_latest", crypto_global_quote, overwrite = TRUE)
safe_db_write(con, "108_1K_coins_ohlcv", all_coins, append = TRUE)
safe_db_write(con, "1K_coins_ohlcv", all_coins, append = TRUE)

# Write to backtest database
safe_db_write(con_bt, "1K_coins_ohlcv", all_coins, append = TRUE)
safe_db_write(con_bt, "crypto_listings_latest_1000", crypto.listings.latest, overwrite = TRUE)

# Optional: Clean up old data (keep only last 30 days)
# Uncomment if you want to implement data retention policy
# cleanup_query <- "DELETE FROM 1K_coins_ohlcv WHERE timestamp < NOW() - INTERVAL '30 days'"
# dbExecute(con, cleanup_query)
# dbExecute(con_bt, cleanup_query)

# Display summary
summary_query <- "SELECT COUNT(*) as total_records, 
                         MIN(timestamp) as oldest_record, 
                         MAX(timestamp) as newest_record 
                  FROM 1K_coins_ohlcv"

summary_main <- dbGetQuery(con, summary_query)
print("Main database summary:")
print(summary_main)

# Close connections
dbDisconnect(con)
dbDisconnect(con_bt)

print("Data pipeline completed successfully")
