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

info(logger, "🔹 Script started")
script_start_time <- Sys.time()  # Store script start time

# 🔹 Exception Handler Function
safe_execute <- function(step_name, expr, retry_attempts = 3, wait_time = 60) {
  attempt <- 1
  while (attempt <= retry_attempts) {
    tryCatch({
      info(logger, paste("Starting:", step_name, "- Attempt", attempt))
      result <- eval(expr)
      info(logger, paste("Completed:", step_name))
      return(result)
    }, error = function(e) {
      error(logger, paste("Error in", step_name, ":", e$message))
      if (attempt == retry_attempts) {
        warning(logger, paste("Max retries reached for", step_name, "- Skipping"))
        return(NULL)  # Skip on max retries
      }
      attempt <- attempt + 1
      info(logger, paste("Retrying", step_name, "in", wait_time, "seconds..."))
      Sys.sleep(wait_time)  # Wait before retry
    })
  }
}

# 🔹 Database Connection Parameters
db_host <- "34.55.195.199"       
db_name <- "dbcp"                 
db_user <- "yogass09"             
db_password <- "jaimaakamakhya"   
db_port <- 5432                   

# 🔹 Establish Database Connection
con <- safe_execute("Database Connection", {
  dbConnect(RPostgres::Postgres(),
            host = db_host,
            dbname = db_name,
            user = db_user,
            password = db_password,
            port = db_port)
})

if (!dbIsValid(con)) {
  stop(" Database connection failed! Check logs.")
} else {
  info(logger, " Database connection successful")
}

# 🔹 Fetch Latest Crypto Listings
crypto.listings.latest <- safe_execute("Fetch Crypto Listings", {
  crypto_listings(
    which = "latest",
    convert = "USD",
    start_date = Sys.Date() - 1,
    end_date = Sys.Date(),
    interval = "day",
    sort = "cmc_rank",
    sort_dir = "asc"
  ) %>% filter(cmc_rank > 0 & cmc_rank < 1500)
})

# 🔹 Fetch Historical Data for Top 2000 Coins (Retry on Failure)
all_coins <- safe_execute("Fetch Crypto Historical Data", {
  crypto_history(
    coin_list = crypto.listings.latest,
    convert = "USD",
    limit = 1500,
    start_date = Sys.Date() - 1,
    end_date = Sys.Date(),
    sleep = 0
  ) %>%
    select(id, slug, name, symbol, timestamp, open, high, low, close, volume, market_cap)
}, retry_attempts = 5, wait_time = 60)  # 🔥 Increased retry attempts for API stability

# 🔹 Fetch Global Crypto Market Data
crypto_global_quote <- safe_execute("Fetch Global Market Data", {
  crypto_global_quotes(
    which = "latest",
    convert = "USD",
    start_date = Sys.Date() - 1,
    end_date = Sys.Date(),
    interval = "daily",
    requestLimit = 1
  )
})

# 🔹 Push Data to Database
safe_execute("Push Crypto Listings to DB", {
  dbWriteTable(con, "crypto_listings_latest_1000", crypto.listings.latest, overwrite = TRUE, row.names = FALSE)
})

safe_execute("Push Global Crypto Data to DB", {
  dbWriteTable(con, "crypto_global_latest", crypto_global_quote, overwrite = TRUE, row.names = FALSE)
})

safe_execute("Push OHLCV Data to DB", {
  dbWriteTable(con, "108_1K_coins_ohlcv", all_coins, append = TRUE, row.names = FALSE)
  dbWriteTable(con, "1K_coins_ohlcv", as.data.frame(all_coins), append = TRUE, row.names = FALSE)
})

# 🔹 Close Database Connection
safe_execute("Close Database Connection", {
  dbDisconnect(con)
})

# 🔹 Log Script Completion
script_end_time <- Sys.time()
execution_time <- round(difftime(script_end_time, script_start_time, units = "secs"), 2)
info(logger, paste("script completed in", execution_time, "seconds"))
