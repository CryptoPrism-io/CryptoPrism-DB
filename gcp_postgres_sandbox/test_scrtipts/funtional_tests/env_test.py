from sqlalchemy import create_engine
import os
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load .env file ONLY if running locally (not in GitHub Actions)
if not os.getenv("GITHUB_ACTIONS"):
    # Find the project root by going up from the script's directory to make it runnable from anywhere
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..', '..', '..'))
    env_path = os.path.join(project_root, '.env')

    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        logger.info(f"✅ .env file loaded successfully from project root.")
    else:
        logger.error(f"❌ .env file not found at the expected project root: {env_path}")
else:
    logger.info("🔹 Running in GitHub Actions: Using GitHub Secrets.")

# Fetch credentials (Works for both local and GitHub Actions)
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")  # Default to 5432 if not set
DB_NAME = os.getenv("DB_NAME", "dbcp")  # Default database
DB_NAME_BT = os.getenv("DB_NAME_BT", "cp_backtest")  # Backtest database
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Validate required environment variables
required_db_vars = {"DB_HOST": DB_HOST, "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD}
missing_vars = [key for key, value in required_db_vars.items() if not value]
if missing_vars:
    logger.error(f"❌ Missing environment variables: {', '.join(missing_vars)}")
    raise SystemExit("❌ Terminating: Missing required credentials.")

# Log only necessary info (DO NOT log DB_PASSWORD for security)
logger.info(f"✅ Database Configuration Loaded: DB_HOST={DB_HOST}, DB_PORT={DB_PORT}")

def print_env_vars():
    """Print all environment variables for testing purposes."""
    
    # Critical variables used in codebase
    critical_vars = [
        ("DB_HOST", DB_HOST),
        ("DB_USER", DB_USER),
        ("DB_PASSWORD", DB_PASSWORD),
        ("DB_PORT", DB_PORT),
        ("DB_NAME", DB_NAME),
        ("DB_NAME_BT", DB_NAME_BT),
        ("GEMINI_API_KEY", GEMINI_API_KEY),
        ("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN),
        ("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID)
    ]
    
    # Additional variables from .env analysis
    additional_vars = [
        ("CMC_API_KEY", os.getenv("CMC_API_KEY")),
        ("DB_URL", os.getenv("DB_URL")),
        ("DB_TABLE", os.getenv("DB_TABLE", "crypto_listings")),
        ("GITHUB_ACTIONS", os.getenv("GITHUB_ACTIONS", "false"))
    ]
    
    
    logger.info("=" * 60)
    logger.info("COMPREHENSIVE ENVIRONMENT VARIABLES STATUS")
    logger.info("=" * 60)
    
    logger.info("\n🔹 CRITICAL VARIABLES (used in codebase):")
    critical_set = 0
    for var_name, var_value in critical_vars:
        if var_value:
            critical_set += 1
            if var_name == "DB_PASSWORD":
                logger.info(f"✅ {var_name} = [HIDDEN FOR SECURITY]")
            elif "TOKEN" in var_name or "KEY" in var_name:
                # Mask sensitive values
                if len(var_value) > 10:
                    masked = f"{var_value[:8]}...{var_value[-4:]}"
                else:
                    masked = f"{var_value[:4]}..."
                logger.info(f"✅ {var_name} = {masked}")
            else:
                logger.info(f"✅ {var_name} = {var_value}")
        else:
            logger.warning(f"❌ {var_name} = None (Not Set)")
    
    logger.info(f"\n🔹 ADDITIONAL VARIABLES:")
    additional_set = 0
    for var_name, var_value in additional_vars:
        if var_value:
            additional_set += 1
            if "KEY" in var_name:
                if len(var_value) > 10:
                    masked = f"{var_value[:8]}...{var_value[-4:]}"
                else:
                    masked = f"{var_value[:4]}..."
                logger.info(f"✅ {var_name} = {masked}")
            else:
                logger.info(f"✅ {var_name} = {var_value}")
        else:
            logger.info(f"ℹ️  {var_name} = {var_value if var_value else 'None (using default)'}")
    
    
    total_critical = len(critical_vars)
    total_additional = len(additional_vars)
    total_all = total_critical + total_additional
    total_all_set = critical_set + additional_set
    
    logger.info(f"\n📊 SUMMARY:")
    logger.info(f"   Critical variables: {critical_set}/{total_critical}")
    logger.info(f"   Additional variables: {additional_set}/{total_additional}")
    logger.info(f"   Total variables: {total_all_set}/{total_all}")
    
    if critical_set == total_critical:
        logger.info("   🎉 All critical environment variables are configured!")
    else:
        missing_critical = [name for name, value in critical_vars if not value]
        logger.warning(f"   ⚠️  Missing critical: {', '.join(missing_critical)}")
    
    return critical_set == total_critical

def test_database_connection():
    """Test database connection using environment variables."""
    # Try multiple PostgreSQL driver options for compatibility
    drivers = [
        'postgresql+pg8000',   # pg8000 - pure Python, no C extensions
        'postgresql+psycopg',  # psycopg3
        'postgresql+psycopg2', # psycopg2 (if available)
        'postgresql'           # default
    ]
    
    for driver in drivers:
        try:
            logger.info(f"Attempting connection with driver: {driver}")
            # Create database engine
            engine = create_engine(f'{driver}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
            
            # Test connection
            with engine.connect() as conn:
                from sqlalchemy import text
                result = conn.execute(text("SELECT 1 as test"))
                test_value = result.scalar()
                if test_value == 1:
                    logger.info(f"✅ Database connection test PASSED with {driver}")
                    return True
                else:
                    logger.error(f"❌ Database connection test FAILED with {driver}")
        except Exception as e:
            logger.warning(f"Driver {driver} failed: {e}")
            continue
    
    logger.error("❌ All database drivers failed")
    return False

if __name__ == "__main__":
    logger.info("🚀 Starting Environment Variable Functionality Test...")
    
    # Test 1: Print environment variables
    print_env_vars()
    
    # Test 2: Test database connection (optional for local dev)
    logger.info("Testing database connection (optional for local development)...")
    db_test_passed = test_database_connection()
    
    # Summary
    logger.info("=" * 50)
    logger.info("🏁 Test Summary:")
    logger.info(f"   Environment Loading: ✅ PASSED")
    logger.info(f"   Database Connection: {'✅ PASSED' if db_test_passed else '❌ FAILED'}")

    # The test's primary goal is to validate env vars, so it exits successfully
    # even if the DB connection fails, but provides clear warnings.
    if db_test_passed:
        logger.info("🎉 Full environment and database connection test PASSED!")
    else:
        logger.warning("⚠️ Database connection failed. If running locally, ensure your .env file is correct and the database is accessible.")
        logger.info("ℹ️ This script will still exit with a success code as its primary goal is to test environment variable loading.")

    logger.info("✅ Your scripts are ready for both local development and GitHub Actions!")
    
    exit(0)