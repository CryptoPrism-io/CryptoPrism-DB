library(RMySQL)
library(crypto2)
library(dplyr)


##  Crypto Global Latest
crypto.global.latest <- crypto_global_quotes(
  which = "latest",
  convert = "USD",
  start_date = NULL,
  end_date = NULL,
  interval = "daily",
  quote = TRUE,
  requestLimit = 1,
  sleep = 0,
  wait = 60,
  finalWait= FALSE
)


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

crypto.info <-crypto_info(
  coin_list = crypto.listings.latest,
  limit = NULL,
  requestLimit = 100,
  sleep = 0,
  finalWait = FALSE
)



