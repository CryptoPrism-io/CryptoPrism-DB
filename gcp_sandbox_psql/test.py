# Import necessary libraries

import os
from sqlalchemy import create_engine


# Retrieve database credentials from environment variables
db_host = os.getenv("SANDBOX_GCP_DB_HOST")
db_name = os.getenv("SANDBOX_GCP_NAME")
db_user = os.getenv("SANDBOX_GCP_DB_USER")
db_password = os.getenv("SANDBOX_GCP_DB_PASSWORD")
db_port = os.getenv("SANDBOX_GCP_DB_PORT")

# Create a SQLAlchemy engine for PostgreSQL
gcp_engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Test the connection
try:
    # Attempt to connect and execute a simple query
    with gcp_engine.connect() as connection:
        result = connection.execute("SELECT 1")
        print("Connection successful:", result.fetchone())
        
except Exception as e:
    print("Connection failed:", e)

finally:
    # Dispose the engine to close the connection
    gcp_engine.dispose()
    print("Connection closed.")
