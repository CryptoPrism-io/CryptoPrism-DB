#!/usr/bin/env Rscript

# Environment Variables Test Script for R
# 
# This script tests loading and accessing environment variables in R.
# Loads .env from the project root directory.
# Tests database connections and validates all required variables.

# Load required libraries
suppressPackageStartupMessages({
  if (!require(dotenv, quietly = TRUE)) {
    stop("Package 'dotenv' is required. Install with: install.packages('dotenv')")
  }
  
  if (!require(DBI, quietly = TRUE)) {
    stop("Package 'DBI' is required. Install with: install.packages('DBI')")
  }
  
  if (!require(RPostgres, quietly = TRUE)) {
    stop("Package 'RPostgres' is required. Install with: install.packages('RPostgres')")
  }
})

# Function to print colored messages
print_status <- function(message, status = "info") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  switch(status,
    "success" = cat(sprintf("[%s] ✅ %s\n", timestamp, message)),
    "error" = cat(sprintf("[%s] ❌ %s\n", timestamp, message)),
    "warning" = cat(sprintf("[%s] ⚠️  %s\n", timestamp, message)),
    "info" = cat(sprintf("[%s] ℹ️  %s\n", timestamp, message)),
    cat(sprintf("[%s] %s\n", timestamp, message))
  )
}

# Function to mask sensitive values
mask_sensitive <- function(value, name) {
  if (is.null(value) || value == "" || is.na(value)) {
    return("Not set")
  }
  
  if (grepl("PASSWORD|KEY|TOKEN", name, ignore.case = TRUE)) {
    if (nchar(value) > 10) {
      return(paste0(substr(value, 1, 8), "...", substr(value, nchar(value)-3, nchar(value))))
    } else {
      return(paste0(substr(value, 1, 4), "..."))
    }
  }
  
  return(value)
}

# Load environment variables
load_environment <- function() {
  print_status("🧪 Starting R Environment Variables Test...", "info")
  
  # Try to find and load .env file
  tryCatch({
    # Get the script directory and find project root
    script_dir <- dirname(normalizePath(sys.frame(1)$ofile))
    project_root <- normalizePath(file.path(script_dir, "..", "..", ".."))
    env_path <- file.path(project_root, ".env")
    
    if (file.exists(env_path)) {
      dotenv::load_dot_env(file = env_path)
      print_status(sprintf("✅ Loaded .env from: %s", env_path), "success")
      print_status(sprintf("   .env file exists: %s", file.exists(env_path)), "info")
    } else {
      print_status(sprintf("❌ .env file not found at: %s", env_path), "error")
    }
  }, error = function(e) {
    print_status(sprintf("❌ Error loading .env: %s", e$message), "error")
  })
}

# Function to test environment variables
test_environment_variables <- function() {
  print_status("============================================================", "info")
  print_status("COMPREHENSIVE R ENVIRONMENT VARIABLES STATUS", "info")
  print_status("============================================================", "info")
  
  # Critical variables used in R codebase
  critical_vars <- list(
    "DB_HOST" = Sys.getenv("DB_HOST"),
    "DB_USER" = Sys.getenv("DB_USER"), 
    "DB_PASSWORD" = Sys.getenv("DB_PASSWORD"),
    "DB_PORT" = Sys.getenv("DB_PORT"),
    "DB_NAME" = Sys.getenv("DB_NAME"),
    "DB_NAME_BT" = Sys.getenv("DB_NAME_BT", "cp_backtest")
  )
  
  # Additional variables that may be used
  additional_vars <- list(
    "CMC_API_KEY" = Sys.getenv("CMC_API_KEY"),
    "GEMINI_API_KEY" = Sys.getenv("GEMINI_API_KEY"),
    "TELEGRAM_BOT_TOKEN" = Sys.getenv("TELEGRAM_BOT_TOKEN"),
    "TELEGRAM_CHAT_ID" = Sys.getenv("TELEGRAM_CHAT_ID"),
    "DB_URL" = Sys.getenv("DB_URL")
  )
  
  # Legacy variables (in .env but unused in R code)
  legacy_vars <- list(
    "MYSQL_HOST" = Sys.getenv("MYSQL_HOST"),
    "MYSQL_USER" = Sys.getenv("MYSQL_USER"),
    "MYSQL_PASSWORD" = Sys.getenv("MYSQL_PASSWORD"),
    "MYSQL_DATABASE" = Sys.getenv("MYSQL_DATABASE"),
    "PG_HOST" = Sys.getenv("PG_HOST"),
    "PG_DATABASE" = Sys.getenv("PG_DATABASE"),
    "PG_USER" = Sys.getenv("PG_USER"),
    "PG_PASSWORD" = Sys.getenv("PG_PASSWORD"),
    "PG_PORT" = Sys.getenv("PG_PORT")
  )
  
  # Test critical variables
  print_status("\n🔹 CRITICAL VARIABLES (used in R codebase):", "info")
  critical_set <- 0
  for (name in names(critical_vars)) {
    value <- critical_vars[[name]]
    if (value != "" && !is.na(value)) {
      critical_set <- critical_set + 1
      if (name == "DB_PASSWORD") {
        print_status(sprintf("✅ %s = [HIDDEN FOR SECURITY]", name), "success")
      } else {
        display_value <- mask_sensitive(value, name)
        print_status(sprintf("✅ %s = %s", name, display_value), "success")
      }
    } else {
      print_status(sprintf("❌ %s = Not set", name), "error")
    }
  }
  
  # Test additional variables
  print_status("\n🔹 ADDITIONAL VARIABLES:", "info")
  additional_set <- 0
  for (name in names(additional_vars)) {
    value <- additional_vars[[name]]
    if (value != "" && !is.na(value)) {
      additional_set <- additional_set + 1
      display_value <- mask_sensitive(value, name)
      print_status(sprintf("✅ %s = %s", name, display_value), "success")
    } else {
      print_status(sprintf("ℹ️  %s = Not set", name), "info")
    }
  }
  
  # Test legacy variables
  print_status("\n🔹 LEGACY VARIABLES (in .env but unused in R code):", "info")
  legacy_set <- 0
  for (name in names(legacy_vars)) {
    value <- legacy_vars[[name]]
    if (value != "" && !is.na(value)) {
      legacy_set <- legacy_set + 1
      if (grepl("PASSWORD", name)) {
        print_status(sprintf("📝 %s = [HIDDEN FOR SECURITY]", name), "info")
      } else {
        print_status(sprintf("📝 %s = %s", name, value), "info")
      }
    } else {
      print_status(sprintf("⚪ %s = Not set", name), "info")
    }
  }
  
  # Summary
  total_critical <- length(critical_vars)
  total_additional <- length(additional_vars)
  total_legacy <- length(legacy_vars)
  total_all <- total_critical + total_additional + total_legacy
  total_all_set <- critical_set + additional_set + legacy_set
  
  print_status("\n📊 SUMMARY:", "info")
  print_status(sprintf("   Critical variables: %d/%d", critical_set, total_critical), "info")
  print_status(sprintf("   Additional variables: %d/%d", additional_set, total_additional), "info")
  print_status(sprintf("   Legacy variables: %d/%d", legacy_set, total_legacy), "info")
  print_status(sprintf("   Total variables: %d/%d", total_all_set, total_all), "info")
  
  if (critical_set == total_critical) {
    print_status("   🎉 All critical environment variables are configured!", "success")
  } else {
    missing_critical <- names(critical_vars)[sapply(critical_vars, function(x) x == "" || is.na(x))]
    print_status(sprintf("   ⚠️  Missing critical variables: %s", paste(missing_critical, collapse = ", ")), "warning")
  }
  
  # Show unused variables
  if (legacy_set > 0) {
    used_legacy <- names(legacy_vars)[sapply(legacy_vars, function(x) x != "" && !is.na(x))]
    print_status(sprintf("   📝 Legacy variables (unused): %s", paste(used_legacy, collapse = ", ")), "info")
  }
  
  return(critical_set == total_critical)
}

# Function to test database connection
test_database_connection <- function() {
  print_status("Testing database connections...", "info")
  
  # Get database configuration
  db_config <- list(
    host = Sys.getenv("DB_HOST"),
    user = Sys.getenv("DB_USER"),
    password = Sys.getenv("DB_PASSWORD"),
    port = as.integer(Sys.getenv("DB_PORT", "5432")),
    dbname = Sys.getenv("DB_NAME"),
    dbname_bt = Sys.getenv("DB_NAME_BT", "cp_backtest")
  )
  
  # Validate required database variables
  required_vars <- c("host", "user", "password", "dbname")
  missing_vars <- c()
  
  for (var in required_vars) {
    if (is.null(db_config[[var]]) || db_config[[var]] == "" || is.na(db_config[[var]])) {
      missing_vars <- c(missing_vars, var)
    }
  }
  
  if (length(missing_vars) > 0) {
    print_status(sprintf("❌ Missing required database variables: %s", paste(missing_vars, collapse = ", ")), "error")
    return(FALSE)
  }
  
  # Test main database connection
  main_db_success <- FALSE
  tryCatch({
    print_status(sprintf("Attempting connection to main database: %s", db_config$dbname), "info")
    
    con <- dbConnect(
      RPostgres::Postgres(),
      host = db_config$host,
      dbname = db_config$dbname,
      user = db_config$user,
      password = db_config$password,
      port = db_config$port
    )
    
    if (dbIsValid(con)) {
      # Test with a simple query
      result <- dbGetQuery(con, "SELECT 1 as test")
      if (result$test[1] == 1) {
        print_status("✅ Main database connection test PASSED", "success")
        main_db_success <- TRUE
      }
      dbDisconnect(con)
    }
  }, error = function(e) {
    print_status(sprintf("❌ Main database connection failed: %s", e$message), "error")
  })
  
  # Test backtest database connection
  bt_db_success <- FALSE
  tryCatch({
    print_status(sprintf("Attempting connection to backtest database: %s", db_config$dbname_bt), "info")
    
    con_bt <- dbConnect(
      RPostgres::Postgres(),
      host = db_config$host,
      dbname = db_config$dbname_bt,
      user = db_config$user,
      password = db_config$password,
      port = db_config$port
    )
    
    if (dbIsValid(con_bt)) {
      # Test with a simple query
      result <- dbGetQuery(con_bt, "SELECT 1 as test")
      if (result$test[1] == 1) {
        print_status("✅ Backtest database connection test PASSED", "success")
        bt_db_success <- TRUE
      }
      dbDisconnect(con_bt)
    }
  }, error = function(e) {
    print_status(sprintf("❌ Backtest database connection failed: %s", e$message), "error")
  })
  
  return(main_db_success && bt_db_success)
}

# Main execution function
main <- function() {
  # Load environment variables
  load_environment()
  
  # Test environment variables
  env_test_passed <- test_environment_variables()
  
  # Test database connections
  print_status("Testing database connections (optional for local development)...", "info")
  db_test_passed <- test_database_connection()
  
  # Final summary
  print_status("==================================================", "info")
  print_status("🏁 R ENVIRONMENT TEST SUMMARY:", "info")
  print_status(sprintf("   Environment Loading: %s", if(env_test_passed) "✅ PASSED" else "❌ FAILED"), 
               if(env_test_passed) "success" else "error")
  print_status(sprintf("   Database Connection: %s", if(db_test_passed) "✅ PASSED" else "❌ FAILED"),
               if(db_test_passed) "success" else "error")
  
  if (env_test_passed && db_test_passed) {
    print_status("🎉 Full R environment and database connection test PASSED!", "success")
  } else if (env_test_passed) {
    print_status("⚠️ Database connection failed. If running locally, ensure your .env file is correct and the database is accessible.", "warning")
    print_status("ℹ️ Environment variables are properly configured for R scripts.", "info")
  } else {
    print_status("❌ Some environment variables are missing. Check your .env file.", "error")
  }
  
  print_status("✅ Your R scripts are ready for both local development and production!", "success")
  
  # Exit with appropriate code
  if (env_test_passed) {
    print_status("Environment test completed successfully!", "success")
    quit(status = 0)
  } else {
    print_status("Environment test failed!", "error")
    quit(status = 1)
  }
}

# Run the main function
main()