# Enhanced Crypto Data Pipeline Script
# Load required libraries
library(RMySQL)
library(crypto2)
library(dplyr)
library(DBI)
library(RPostgres)
library(lubridate)

# Configuration (use environment variables for production)
CONFIG <- list(
  db_host = Sys.getenv("DB_HOST", "34.55.195.199"),
  db_name = Sys.getenv("DB_NAME", "dbcp"),
  db_name_bt = Sys.getenv("DB_NAME_BT", "cp_backtest"),
  db_user = Sys.getenv("DB_USER", "yogass09"),
  db_password = Sys.getenv("DB_PASSWORD", "jaimaakamakhya"),
  db_port = as.integer(Sys.getenv("DB_PORT", "5432")),
  
  # Data parameters
  coin_limit = 5000,
  cmc_rank_min = 1,
  cmc_rank_max = 2000,
  default_lookback_days = 7
)

# Function to establish database connection
connect_to_db <- function(dbname) {
  tryCatch({
    con <- dbConnect(
      RPostgres::Postgres(),
      host = CONFIG$db_host,
      dbname = dbname,
      user = CONFIG$db_user,
      password = CONFIG$db_password,
      port = CONFIG$db_port
    )
    
    if (dbIsValid(con)) {
      print(paste("Connection successful to", dbname))
      return(con)
    } else {
      stop("Connection invalid")
    }
  }, error = function(e) {
    stop(paste("Failed to connect to", dbname, ":", e$message))
  })
}

# Function to get latest timestamp from database
get_latest_timestamp <- function(con, table_name) {
  tryCatch({
    # Check if table exists first
    table_exists_query <- sprintf(
      "SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = '%s'
      )", 
      table_name
    )
    
    exists_result <- dbGetQuery(con, table_exists_query)
    
    if (!exists_result[1,1]) {
      print(paste("Table", table_name, "does not exist. Using default lookback."))
      return(Sys.Date() - CONFIG$default_lookback_days)
    }
    
    # Quote table names that start with numbers or contain special characters
    if (grepl("^[0-9]", table_name)) {
      query <- sprintf('SELECT MAX(timestamp) as max_timestamp FROM "%s"', table_name)
    } else {
      query <- sprintf('SELECT MAX(timestamp) as max_timestamp FROM %s', table_name)
    }
    
    result <- dbGetQuery(con, query)
    
    if (nrow(result) > 0 && !is.na(result$max_timestamp[1])) {
      latest_date <- as.Date(result$max_timestamp[1])
      print(paste("Latest timestamp in", table_name, ":", latest_date))
      return(latest_date)
    } else {
      print(paste("No data in", table_name, ". Using default lookback."))
      return(Sys.Date() - CONFIG$default_lookback_days)
    }
  }, error = function(e) {
    print(paste("Error getting latest timestamp from", table_name, ":", e$message))
    return(Sys.Date() - CONFIG$default_lookback_days)
  })
}

# Function to fetch cryptocurrency data
fetch_crypto_data <- function(start_date, end_date) {
  result <- list()
  
  # Fetch latest listings
  tryCatch({
    print("Fetching latest cryptocurrency listings...")
    crypto.listings.latest <- crypto_listings(
      which = "latest",
      convert = "USD",
      limit = CONFIG$coin_limit,
      start_date = start_date,
      end_date = end_date,
      interval = "day",
      quote = TRUE,
      sort = "cmc_rank",
      sort_dir = "asc",
      sleep = 0,
      wait = 0,
      finalWait = FALSE
    )
    
    # Filter based on rank
    crypto.listings.latest <- crypto.listings.latest %>%
      filter(cmc_rank >= CONFIG$cmc_rank_min & cmc_rank < CONFIG$cmc_rank_max)
    
    result$listings <- crypto.listings.latest
    print(paste("Fetched", nrow(crypto.listings.latest), "cryptocurrency listings"))
    
  }, error = function(e) {
    print(paste("Error fetching listings:", e$message))
    result$listings <- NULL
  })
  
  # Fetch historical data
  if (!is.null(result$listings) && nrow(result$listings) > 0) {
    tryCatch({
      print("Fetching historical OHLCV data...")
      all_coins <- crypto_history(
        coin_list = result$listings,
        convert = "USD",
        limit = 2000,
        start_date = start_date,
        end_date = end_date + 1,  # Add 1 day to ensure we get today's data
        sleep = 0
      )
      
      # Select required columns
      all_coins <- all_coins[, c("id", "slug", "name", "symbol", "timestamp", 
                                  "open", "high", "low", "close", "volume", "market_cap")]
      
      # Remove duplicates based on id and timestamp
      all_coins <- all_coins %>%
        distinct(id, timestamp, .keep_all = TRUE)
      
      result$ohlcv <- all_coins
      print(paste("Fetched", nrow(all_coins), "OHLCV records"))
      
    }, error = function(e) {
      print(paste("Error fetching OHLCV data:", e$message))
      result$ohlcv <- NULL
    })
  }
  
  # Fetch global quotes
  tryCatch({
    print("Fetching global cryptocurrency quotes...")
    crypto_global_quote <- crypto_global_quotes(
      which = "latest",
      convert = "USD",
      start_date = start_date,
      end_date = end_date,
      interval = "daily",
      quote = TRUE,
      requestLimit = 1,
      sleep = 0,
      wait = 60,
      finalWait = FALSE
    )
    
    result$global_quotes <- crypto_global_quote
    print(paste("Fetched", nrow(crypto_global_quote), "global quote records"))
    
  }, error = function(e) {
    print(paste("Error fetching global quotes:", e$message))
    result$global_quotes <- NULL
  })
  
  return(result)
}

# Function to write data to database with proper handling
write_to_db <- function(con, table_name, data, overwrite = FALSE) {
  if (is.null(data) || nrow(data) == 0) {
    print(paste("No data to write to", table_name))
    return(FALSE)
  }
  
  tryCatch({
    # For tables starting with numbers, use quoted identifiers
    if (grepl("^[0-9]", table_name)) {
      # First drop the table if overwrite is TRUE
      if (overwrite) {
        dbExecute(con, sprintf('DROP TABLE IF EXISTS "%s"', table_name))
      }
      
      # Use dbWriteTable with quoted name
      success <- dbWriteTable(con, 
                              name = SQL(sprintf('"%s"', table_name)), 
                              value = data, 
                              overwrite = overwrite, 
                              append = !overwrite, 
                              row.names = FALSE)
    } else {
      success <- dbWriteTable(con, table_name, data, 
                              overwrite = overwrite, 
                              append = !overwrite, 
                              row.names = FALSE)
    }
    
    if (success) {
      print(paste("Successfully wrote", nrow(data), "rows to", table_name))
    }
    return(success)
    
  }, error = function(e) {
    print(paste("Error writing to", table_name, ":", e$message))
    return(FALSE)
  })
}

# Main execution
main <- function() {
  # Establish connections
  con <- connect_to_db(CONFIG$db_name)
  con_bt <- connect_to_db(CONFIG$db_name_bt)
  
  tryCatch({
    # Get latest timestamp from main OHLCV table
    latest_timestamp <- get_latest_timestamp(con, "1K_coins_ohlcv")
    
    # Calculate date range
    start_date <- latest_timestamp + 1
    end_date <- Sys.Date()
    
    print(paste("Fetching data from:", start_date, "to:", end_date))
    
    # Check if we need to fetch data
    if (start_date > end_date) {
      print("Database is already up to date. No new data to fetch.")
      return()
    }
    
    # Fetch all data
    crypto_data <- fetch_crypto_data(start_date, end_date)
    
    # Write to databases
    if (!is.null(crypto_data$listings)) {
      write_to_db(con, "crypto_listings_latest_1000", crypto_data$listings, overwrite = TRUE)
      write_to_db(con_bt, "crypto_listings_latest_1000", crypto_data$listings, overwrite = TRUE)
    }
    
    if (!is.null(crypto_data$global_quotes)) {
      write_to_db(con, "crypto_global_latest", crypto_data$global_quotes, overwrite = TRUE)
    }
    
    if (!is.null(crypto_data$ohlcv)) {
      # Append to historical tables
      write_to_db(con, "108_1K_coins_ohlcv", crypto_data$ohlcv, overwrite = FALSE)
      write_to_db(con, "1K_coins_ohlcv", crypto_data$ohlcv, overwrite = FALSE)
      write_to_db(con_bt, "1K_coins_ohlcv", crypto_data$ohlcv, overwrite = FALSE)
    }
    
    print("Data pipeline completed successfully!")
    
  }, error = function(e) {
    print(paste("Error in main execution:", e$message))
  }, finally = {
    # Always close connections
    if (exists("con") && dbIsValid(con)) {
      dbDisconnect(con)
      print("Disconnected from main database")
    }
    if (exists("con_bt") && dbIsValid(con_bt)) {
      dbDisconnect(con_bt)
      print("Disconnected from backtest database")
    }
  })
}

# Run the main function
main()
