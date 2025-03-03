# -*- coding: utf-8 -*-
"""PROD_QA_CP_AI.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1_ITIzVn8ceb5AYWkD7Mb44lW6poqCCjL
"""

#@title pip

required_packages = [
    "pandas",
    "numpy",
    "sqlalchemy",
    "psycopg2-binary",
    "python-dotenv",
    "mysql-connector-python",
    "python-telegram-bot==13.15"
]

# Write to requirements.txt
with open("requirements.txt", "w") as f:
    f.write("\n".join(required_packages))

try:
    import google.colab  # Checks if running in Colab
    print("Running in Colab: Installing dependencies...")
    !pip install -r requirements.txt
except ImportError:
    print("Running outside Colab (e.g., GitHub Actions), skipping installation.")

#@title .env
# Create the .env file and write the variables to it
env_content = """
DB_HOST="34.55.195.199"
DB_USER="yogass09"
DB_PASSWORD="jaimaakamakhya"
DB_PORT="5432"
GEMINI_API_KEY="AIzaSyAuji6f62tQmNqmLRNOSt1pw5vg2AafzGY"
"""

# Write the content to a file called .env
with open('.env', 'w') as f:
    f.write(env_content)

#@title .env [load]
import os
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load .env file ONLY if running locally (not in GitHub Actions)
if not os.getenv("GITHUB_ACTIONS"):
    env_file = ".env"
    if os.path.exists(env_file):
        load_dotenv()
        logger.info("✅ .env file loaded successfully.")
    else:
        logger.error("❌ .env file is missing! Please create one for local testing.")
else:
    logger.info("🔹 Running in GitHub Actions: Using GitHub Secrets.")

# Fetch credentials (Works for both local and GitHub Actions)
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")  # Default to 5432 if not set
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Validate required environment variables
missing_vars = [var for var in ["DB_HOST", "DB_USER", "DB_PASSWORD", "GEMINI_API_KEY"] if not globals()[var]]
if missing_vars:
    logger.error(f"❌ Missing environment variables: {', '.join(missing_vars)}")
    raise SystemExit("❌ Terminating: Missing required credentials.")

# Log only necessary info (DO NOT log DB_PASSWORD for security)
logger.info(f"✅ Database Configuration Loaded: DB_HOST={DB_HOST}, DB_PORT={DB_PORT}")

# @title Delete duplicates
import os
from sqlalchemy import create_engine, text
from typing import Dict
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Fetch database credentials
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def delete_duplicates(db_names):
    results = {}

    for db_name in db_names:
        try:
            engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{db_name}',
                                   isolation_level="AUTOCOMMIT")
            logger.info(f"Processing database: {db_name}")
            results[db_name] = {}

            with engine.connect() as conn:
                # Fetch all table names
                table_names_query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
                table_names = [row[0] for row in conn.execute(table_names_query)]

                for table_name in table_names:
                    try:
                        # Check if 'slug' and 'timestamp' columns exist
                        column_check_query = text(f"""
                            SELECT column_name FROM information_schema.columns
                            WHERE table_name = '{table_name}'
                            AND column_name IN ('slug', 'timestamp');
                        """)
                        existing_columns = {row[0] for row in conn.execute(column_check_query)}

                        if 'slug' not in existing_columns or 'timestamp' not in existing_columns:
                            logger.info(f"Skipping table '{table_name}' in DB '{db_name}': Missing required columns.")
                            continue

                        # DELETE query based only on 'slug' and 'timestamp'
                        delete_query = text(f"""
                            DELETE FROM public."{table_name}" AS t1
                            USING (
                                SELECT ctid FROM (
                                    SELECT ctid, ROW_NUMBER() OVER (PARTITION BY slug, timestamp ORDER BY ctid) AS row_num
                                    FROM public."{table_name}"
                                ) AS subquery
                                WHERE row_num > 1
                            ) AS t2
                            WHERE t1.ctid = t2.ctid;
                        """)

                        # Execute delete query
                        result = conn.execute(delete_query)
                        deleted_rows = result.rowcount
                        results[db_name][table_name] = deleted_rows
                        logger.info(f"Deleted {deleted_rows} duplicate rows from '{table_name}' in DB '{db_name}'.")

                        # Run VACUUM ANALYZE
                        conn.execute(text(f"VACUUM ANALYZE public.\"{table_name}\";"))
                        logger.info(f"Vacuumed '{table_name}' in DB '{db_name}'.")

                    except Exception as e:
                        logger.error(f"Error processing table '{table_name}' in DB '{db_name}': {e}")
                        conn.execute(text("ROLLBACK;"))  # ROLLBACK transaction on error

                logger.info("=" * 50)

        except Exception as e:
            logger.error(f"Error connecting to database '{db_name}': {e}")

    return results

# List of databases to process
db_names = ['cp_ai']

# Run the function
delete_results = delete_duplicates(db_names)
identify_and_delete_duplicate_timestamps_multiple_dbs_results = delete_results
identify_and_delete_duplicate_timestamps_multiple_dbs_results

#@title cp_ai QA analysis for SQL DB
import os
import json
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
from dateutil.parser import parse

# Load environment variables
load_dotenv()

# Fetch database credentials
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")  # Default to 5432 if not set

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def classify_null_ratio(null_ratio):
    """Classifies the null ratio into risk levels."""
    if null_ratio is None:
        return "UNKNOWN"
    elif null_ratio < 0.2:
        return "LOW"
    elif 0.2 <= null_ratio < 0.49:
        return "MEDIUM"
    elif 0.49 <= null_ratio < 0.89:
        return "HIGH"
    else:
        return "CRITICAL"

def validate_timestamps(table_name, first_timestamp, last_timestamp):
    """Validates timestamps based on table name rules and checks for NULL values."""
    if not first_timestamp or not last_timestamp:
        return "WARNING: First and/or Last timestamp is NULL"

    try:
        first_dt = parse(first_timestamp)
        last_dt = parse(last_timestamp)

        if table_name.startswith("FE_"):
            if first_dt != last_dt:
                return "CRITICAL ERROR: FE Table timestamps must match"
        else:
            if (last_dt - first_dt).days < 4:
                return "CRITICAL ERROR: Non-FE Table timestamps must have at least a 4-day gap"

    except Exception as e:
        return f"ERROR: Invalid timestamp format ({e})"

    return "VALID"

def analyze_database(db_name):
    """Analyzes table schemas, row counts, null stats, timestamps, and duplicates in a PostgreSQL database."""
    logger.info(f"Processing database: {db_name}")

    try:
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{db_name}')
    except Exception as e:
        logger.error(f"Database connection failed for {db_name}: {e}")
        return {}

    results = {}

    try:
        with engine.connect() as conn:
            # Fetch all table names
            table_names_query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            table_names = [row[0] for row in conn.execute(table_names_query)]

            for table_name in table_names:
                try:
                    # Fetch column names and data types
                    columns_query = text(f"""
                        SELECT column_name, data_type FROM information_schema.columns
                        WHERE table_name = '{table_name}' AND table_schema = 'public';
                    """)
                    columns_df = pd.read_sql_query(columns_query, conn)

                    # Fetch total row count
                    row_count_query = text(f'SELECT COUNT(*) FROM public."{table_name}";')
                    row_count = conn.execute(row_count_query).scalar()

                    # Handle timestamps
                    timestamp_columns = columns_df[columns_df["data_type"].str.contains("timestamp", case=False)]["column_name"].tolist()
                    first_timestamp, last_timestamp, timestamp_status = None, None, "VALID"

                    if timestamp_columns:
                        timestamp_col = timestamp_columns[0]
                        first_query = text(f'SELECT "{timestamp_col}" FROM public."{table_name}" ORDER BY "{timestamp_col}" ASC LIMIT 1;')
                        last_query = text(f'SELECT "{timestamp_col}" FROM public."{table_name}" ORDER BY "{timestamp_col}" DESC LIMIT 1;')

                        first_timestamp = conn.execute(first_query).scalar()
                        last_timestamp = conn.execute(last_query).scalar()

                        first_timestamp = str(first_timestamp) if first_timestamp else None
                        last_timestamp = str(last_timestamp) if last_timestamp else None

                        timestamp_status = validate_timestamps(table_name, first_timestamp, last_timestamp)

                    # Fetch NULL statistics for each column
                    column_stats = {}
                    for _, row in columns_df.iterrows():
                        column_name = row['column_name']

                        null_count_query = text(f'SELECT COUNT(*) FROM public."{table_name}" WHERE "{column_name}" IS NULL;')
                        null_count = conn.execute(null_count_query).scalar()

                        non_null_count = row_count - null_count
                        null_ratio = round(null_count / row_count, 4) if row_count > 0 else None
                        risk_classification = classify_null_ratio(null_ratio)

                        column_stats[column_name] = {
                            "dataType": row['data_type'],
                            "null_count": null_count,
                            "non_null_count": non_null_count,
                            "total_rows": row_count,
                            "null_ratio": null_ratio,
                            "risk_classification": risk_classification
                        }

                    # Check for duplicate slug + timestamp
                    duplicate_count = None
                    duplicate_status = "VALID"

                    column_check_query = text(f"""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_name = '{table_name}' AND column_name IN ('slug', 'timestamp');
                    """)
                    existing_columns = {row[0] for row in conn.execute(column_check_query)}

                    if 'slug' in existing_columns and 'timestamp' in existing_columns:
                        duplicate_query = text(f"""
                            SELECT COUNT(*) FROM (
                                SELECT slug, timestamp, ROW_NUMBER() OVER (PARTITION BY slug, timestamp ORDER BY id DESC) as rn
                                FROM public."{table_name}"
                            ) t WHERE rn > 1;
                        """)
                        duplicate_count = conn.execute(duplicate_query).scalar()

                        if duplicate_count > 0:
                            duplicate_status = "CRITICAL ERROR: Duplicate rows found"

                    # Store results
                    results[table_name] = {
                        "columns": column_stats,
                        "column_count": len(column_stats),
                        "total_rows": row_count,
                        "first_timestamp": first_timestamp,
                        "last_timestamp": last_timestamp,
                        "timestamp_status": timestamp_status,
                        "duplicate_count": duplicate_count,
                        "duplicate_status": duplicate_status
                    }

                    logger.info(f"Processed table: {table_name} ({len(column_stats)} columns, {duplicate_count} duplicates)")

                except Exception as e:
                    logger.error(f"Error processing table '{table_name}' in DB '{db_name}': {e}")

    except Exception as e:
        logger.error(f"Error connecting to database '{db_name}': {e}")

    return results

# List of databases to analyze
db_names = ['cp_ai']

# Run analysis for each database
database_analysis_results = {db: analyze_database(db) for db in db_names}

# Save results to JSON
output_file = "database_analysis_results.json"
try:
    with open(output_file, "w") as f:
        json.dump(database_analysis_results, f, indent=4)
    logger.info(f"Analysis completed. Results saved to {output_file}")
except Exception as e:
    logger.error(f"Failed to save results to JSON: {e}")

# Print summary
print(json.dumps(database_analysis_results, indent=4))

# @title GenAI
import os
import json
import logging
from dotenv import load_dotenv
import google.generativeai as genai

# Load environment variables
load_dotenv()

# Securely fetch API Key from .env
API_KEY = os.getenv("GEMINI_API_KEY")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Ensure API key is available
if not API_KEY:
    logger.error("GEMINI_API_KEY is missing. Please set it in the .env file.")
    exit(1)

# Configure Gemini API
genai.configure(api_key=API_KEY)

# Model generation configuration
generation_config = {
    "temperature": 1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 8192,
    "response_mime_type": "text/plain",
}

# Load the database analysis results
try:
    with open("database_analysis_results.json", "r") as f:
        analysis_results = json.load(f)
    logger.info("Loaded database analysis results successfully.")
except FileNotFoundError:
    logger.error("Database analysis results file not found.")
    exit(1)
except json.JSONDecodeError as e:
    logger.error(f"Error decoding JSON file: {e}")
    exit(1)

def summarize_analysis(analysis_data: dict, model: genai.GenerativeModel) -> str:
    """Summarizes database analysis results using the Gemini API."""

    # Convert analysis data to JSON string (limit size to avoid API request issues)
    analysis_json_str = json.dumps(analysis_data, indent=4)[:128000]  # Trim if too large

    system_prompt = """
                      You are a super-efficient data quality analyst generating summaries for Telegram.
                      Analyze the provided JSON database analysis report. Deliver actionable insights
                      about any data quality issues. Highlighting and recommending fixes for any immediate needs.

                      ### **Knowledge Base (What Each Data Point Means)**
                      - **Null Ratio:** Indicates missing values in a column.
                        - Below **0.2** → Low risk ✅
                        - **0.2 to 0.49** → Medium risk ⚠️
                        - **0.49 to 0.89** → High risk 🔴
                        - **Above 0.89** → **CRITICAL - Requires immediate action**

                      - **Duplicate Count:** If greater than 0, it means there are duplicate values in a critical column.
                        - Duplicate timestamps can **cause incorrect aggregations & signal misfires**.
                        - Duplicate `slug + timestamp` values → **CRITICAL ERROR - Requires immediate action**
                        - Missing timestamps → **High-risk issue

                      - **Timestamp Validation:** Ensures time-based data consistency.
                        - FE_RATIOS, FE_MOMENTUM, FE_MOMENTUM_SIGNALS, FE_OSCILLATOR, FE_OSCILLATORS_SIGNALS, FE_RATIOS_SIGNALS, FE_TVV, FE_TVV_SIGNALS, FE_PCT_CHANGE should have the same first and last timestamp
                        - Other tables **must have at least a 4-day gap** between timestamps.
                        - Missing timestamps → **High-risk issue**

                      ### **Filtering & Summary Guidelines**
                      - ❌ **Do NOT include normal or low-risk findings**.
                      - ✅ **Report ONLY high-risk, critical, or failed checks**.
                      - 📌 **Highlight specific tables and columns with issues**.
                      - 🔍 **Provide clear explanations and recommendations**.

                      Format your response as a well formatted and spaced paragraph **technical summary for a Telegram notification**:
                     --------
                      QA Alert (1/n) (DatabaseName)

                      Issue: (Brief problem description)

                      Table: (Affected Table)

                      Details: (1-2 sentences explaining the issue and impact)

                      Action: (1 sentence recommendation)
                      """



    user_prompt = f"""
              Analyze the following database analysis results and extract **ONLY high-risk, failed, or critical issues**
              Make sure the output is presentation ready .. no header footer ... no nonsense text only what is important.
              ```json
              {analysis_json_str}

                  ```
                  """

    try:
        response = model.generate_content(system_prompt + user_prompt)
        return response.text if response else "No summary generated."

    except genai.exceptions.APIException as e:
        logger.error(f"Error generating summary: {e}")
        return "Error: Failed to generate summary."

# Create a Gemini model instance
model = genai.GenerativeModel(model_name="gemini-2.0-flash-exp", generation_config=generation_config)

# Summarize the analysis results
summary = summarize_analysis(analysis_results, model)

# Print or log summary
if summary:
    logger.info("Database analysis summary generated successfully.")
    print("Summary of Database Analysis:\n")
    print(summary)
else:
    logger.warning("No summary generated.")

# @title Telegram Runner
import requests

def send_telegram_message(bot_token, chat_id, message):
    """Sends a message to a Telegram chat using the provided bot token.

    Args:
        bot_token: Your Telegram bot token.
        chat_id: The ID of the Telegram chat.
        message: The message to send.
    """

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        params = {
            "chat_id": chat_id,
            "text": message
        }
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        print("Message sent successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Error sending message: {e}")

# Example usage:
bot_token = "7911188723:AAFB1_XTNd_1kDNhvqfhm6C0gL34HE8P8fU"  # Replace with your bot token
chat_id = "-4708531708"  # Replace with your chat ID
message = summary

send_telegram_message(bot_token, chat_id, message)