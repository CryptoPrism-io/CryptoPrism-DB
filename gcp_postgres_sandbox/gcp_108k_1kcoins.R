# Load necessary libraries
library(RMySQL)
library(crypto2)
library(dplyr)
library(DBI)
library(RPostgres)
library(log4r)

# 🔹 Initialize Logger
log_file <- "crypto_data.log"
logger <- logger(threshold = "INFO", appenders = file_appender(log_file))

# 🔹 Log Start of Script
info(logger, "🔹 Script started")
script_start_time <- Sys.time()  # Store script start time

# 🔹 Exception Handler Function
safe_execute <- function(step_name, expr) {
  tryCatch({
    info(logger, paste("Starting:", step_name))
    result <- eval(expr)
    info(logger, paste("Completed:", step_name))
    return(result)
  }, error = function(e) {
    error(logger, paste("Error in", step_name, ":", e$message))
    return(NULL)  # Continue execution on failure
  })
}


# 🔹 Fetch Latest Crypto Listings
crypto.listings.latest <- safe_execute("Fetching latest crypto listings", {
  crypto_listings(
    which = "latest",
    convert = "USD",
    limit = NULL,
    start_date = Sys.Date()-1,
    end_date = Sys.Date(),
    interval = "day",
    quote = TRUE,
    sort = "cmc_rank",
    sort_dir = "asc",
    sleep = 0,
    wait = 0,
    finalWait = FALSE
  )
})

# 🔹 Filter Crypto Listings
crypto.listings.latest <- safe_execute("Filtering crypto listings", {
  crypto.listings.latest %>%
    filter(cmc_rank > 0 & cmc_rank < 2000)
})

# 🔹 Fetch Historical Data
all_coins <- safe_execute("Fetching historical data", {
  crypto_history(
    coin_list = crypto.listings.latest,
    convert = "USD",
    limit = 2000,
    start_date = Sys.Date()-1,
    end_date = Sys.Date()+1,
    sleep = 0
  )
})

# 🔹 Select Required Columns
all_coins <- safe_execute("Selecting required columns for historical data", {
  all_coins[, c("id", "slug", "name", "symbol", "timestamp", "open","high", "low", "close", "volume", "market_cap")]
})

# 🔹 Fetch Global Crypto Quotes
crypto_global_quote <- safe_execute("Fetching global crypto quotes", {
  crypto_global_quotes(
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
})

# 🔹 Database Connection Parameters
db_host <- "34.55.195.199"        
db_name <- "dbcp"                 
db_user <- "yogass09"             
db_password <- "jaimaakamakhya"   
db_port <- 5432                   

# 🔹 Establish Database Connection
con <- safe_execute("Connecting to database", {
  dbConnect(
    RPostgres::Postgres(),
    host = db_host,
    dbname = db_name,
    user = db_user,
    password = db_password,
    port = db_port
  )
})

if (!is.null(con) && dbIsValid(con)) {
  log.info(logger, "Database connection successful")
} else {
  log.error(logger, "Database connection failed, exiting script")
  quit(save = "no", status = 1)  # Exit script if DB connection fails
}

# 🔹 Write Data to Database
safe_execute("Writing crypto listings to database", {
  dbWriteTable(con, "crypto_listings_latest_1000", crypto.listings.latest, overwrite = TRUE, row.names = FALSE)
})

safe_execute("Writing global crypto quotes to database", {
  dbWriteTable(con, "crypto_global_latest", crypto_global_quote, overwrite = TRUE, row.names = FALSE)
})

safe_execute("Appending historical data to 108_1K_coins_ohlcv table", {
  dbWriteTable(con, "108_1K_coins_ohlcv", all_coins, append = TRUE, row.names = FALSE)
})

safe_execute("Appending historical data to 1K_coins_ohlcv table", {
  dbWriteTable(con, "1K_coins_ohlcv", as.data.frame(all_coins), append = TRUE, row.names = FALSE)
})

# 🔹 Close Database Connection
safe_execute("Closing database connection", {
  dbDisconnect(con)
})

# 🔹 Calculate Total Execution Time
script_end_time <- Sys.time()
total_execution_time <- script_end_time - script_start_time
log.info(logger, paste("🔹 Script execution completed in", round(total_execution_time, 2), "seconds"))

