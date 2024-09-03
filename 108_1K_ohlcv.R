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
  filter(cmc_rank > 0 & cmc_rank < 1200)


all_coins<-crypto_history(coin_list = crypto.listings.latest,convert = "USD",limit = 1200,
                          start_date = Sys.Date()-108,end_date = Sys.Date(),sleep = 0)

all_coins <- all_coins[, c("id", "slug", "name", "symbol", "timestamp", "open","high", "low", "close", "volume", "market_cap")]

# Database connection details


host <- "dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com"
port <- 3306
user <- "yogass09"  # replace with your MySQL username
password <- "jaimaakamakhya"  # replace with your MySQL password
dbname <- "dbcp"  # replace with your MySQL database name

# Establish connection
con <- dbConnect(
  MySQL(),
  host = host,
  port = port,
  user = user,
  password = password,
  dbname = dbname
)

dbWriteTable(con, "crypto_listings_latest_1000", as.data.frame(crypto.listings.latest), overwrite = TRUE, row.names = FALSE)

dbWriteTable(con, "108_1K_coins_ohlcv", as.data.frame(all_coins), overwrite = TRUE, row.names = FALSE)


dbDisconnect(con)

