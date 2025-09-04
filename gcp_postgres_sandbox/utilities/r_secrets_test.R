# Test Script for Database Connection Using Environment Variables

# Load required libraries
library(DBI)
library(RPostgres)
library(dotenv)

# Load environment variables from .env file
dotenv::load_dot_env(file = ".env")

# Configuration (use environment variables for production)
CONFIG <- list(
  db_host = Sys.getenv("DB_HOST"),
  db_name = Sys.getenv("DB_NAME"),
  db_user = Sys.getenv("DB_USER"),
  db_password = Sys.getenv("DB_PASSWORD"),
  db_port = as.integer(Sys.getenv("DB_PORT"))
)

# Debugging: Print environment variables
print(paste("DB_HOST:", CONFIG$db_host))
print(paste("DB_NAME:", CONFIG$db_name))
print(paste("DB_USER:", CONFIG$db_user))
print(paste("DB_PASSWORD:", CONFIG$db_password))
print(paste("DB_PORT:", CONFIG$db_port))

# Function to establish database connection
connect_to_db <- function() {
  tryCatch({
    if (is.na(CONFIG$db_port)) {
      stop("DB_PORT is not a valid integer.")
    }

    con <- dbConnect(
      RPostgres::Postgres(),
      host = CONFIG$db_host,
      dbname = CONFIG$db_name,
      user = CONFIG$db_user,
      password = CONFIG$db_password,
      port = CONFIG$db_port
    )
    
    if (dbIsValid(con)) {
      print("Connection successful!")
      dbDisconnect(con)
    } else {
      print("Connection failed!")
    }
  }, error = function(e) {
    print(paste("Error establishing connection:", e$message))
  })
}

# Run the connection test
connect_to_db()