library(RMySQL)
library(crypto2)
library(dplyr)

crypto.listings.latest <- crypto_listings(
  which = "latest",
  convert = "USD",
  limit = 5000,
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

# Filter the data based on cmc_rank
crypto.listings.latest<- crypto.listings.latest %>%
  filter(cmc_rank > 0 & cmc_rank < 2000)


all_coins<-crypto_history(coin_list = crypto.listings.latest,convert = "USD",limit = 2000,
                          start_date = Sys.Date()-1,end_date = Sys.Date()+1,sleep = 0)

all_coins <- all_coins[, c("id", "slug", "name", "symbol", "timestamp", "open","high", "low", "close", "volume", "market_cap")]



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

# Load necessary libraries
library(DBI)
library(RPostgres)

# Connection parameters
db_host <- "34.55.195.199"         # Public IP of your PostgreSQL instance on GCP
db_name <- "dbcp"                  # Database name
db_name_bt <- "cp_backtest"                  # Database name for bt
db_user <- "yogass09"              # Database username
db_password <- "jaimaakamakhya"    # Database password
db_port <- 5432                    # PostgreSQL port

# Attempt to establish a connection
con <- dbConnect(
  RPostgres::Postgres(),
  host = db_host,
  dbname = db_name,
  user = db_user,
  password = db_password,
  port = db_port
)

# Attempt to establish a connection
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
}


# Write dataframes to database
dbWriteTable(con, "crypto_listings_latest_1000", crypto.listings.latest, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "crypto_global_latest", crypto_global_quote, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "108_1K_coins_ohlcv", all_coins, append = TRUE, row.names = FALSE)
dbWriteTable(con, "1K_coins_ohlcv", all_coins, append = TRUE, row.names = FALSE)
dbWriteTable(con_bt, "1K_coins_ohlcv", all_coins, append = TRUE, row.names = FALSE)
dbWriteTable(con_bt, "crypto_listings_latest_1000", crypto.listings.latest, overwrite = TRUE, row.names = FALSE)

# Close connection
dbDisconnect(con)
