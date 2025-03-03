import mysql.connector
import pandas as pd
import psycopg2

# Establishing the connection

con = psycopg2.connect(
        host="34.47.215.135",
        database="dbcp",
        user="yogass09",
        password="jaimaakamakhya",
        port=5432
    )
# @title SQL Query Connection to AWS for Data Listing

query = "SELECT slug FROM crypto_listings_latest_1000"
slug_column = pd.read_sql_query(query, con)


con.close()

# prompt: slug_column... convert this in a '"slug" must be a string'

slug_column['slug'] = slug_column['slug'].astype(str)

import requests
import pandas as pd

# Define the URL and API key
url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info"
api_key = "9d5aff71-7d4b-48f3-9afd-6532c5a1cd69"


# Define the query parameters
params = {
    'slug': slug_column['slug'],  # Pass the ID list as a string
    'skip_invalid': 'true',
    'aux': "logo,urls,description,tags,platform,date_added,notice"  #
}

# Set the headers including the API key
headers = {
    'X-CMC_PRO_API_KEY': api_key,
    'Accept': 'application/json'
}

# Make the API request
response = requests.get(url, params=params, headers=headers)

# prompt: {'status': {'timestamp': '2024-09-18T12:08:04.221Z', 'error_code': 400, 'error_message': '"slug" must be a string',... above code did not work , fetch 199 slugs and sleep for 2 secs
import requests
import time


# Split the slug_list into chunks of 199
chunks = [slug_column['slug'][i:i + 199] for i in range(0, len(slug_column['slug']), 199)]

# Initialize an empty list to store the results
results = []

# Iterate over the chunks
for chunk in chunks:
    # Define the query parameters for the current chunk
    params = {
        'slug': ','.join(chunk),  # Pass the slugs as a comma-separated string
        'skip_invalid': 'true',
        'aux': "logo,urls,description,tags,platform,date_added,notice"
    }

    # Make the API request
    response = requests.get(url, params=params, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Append the response to the results list
        results.append(response.json())
    else:
        print(f"Error: {response.status_code} - {response.text}")

    # Wait for 2 seconds
    time.sleep(2)

# prompt: display the above results
import pandas as pd
import json


# Combine the results into a single dictionary
combined_results = {}
for result in results:
  combined_results.update(result['data'])


# Convert the combined results to a DataFrame
df = pd.DataFrame.from_dict(combined_results, orient='index')

# prompt: in df there is a coloum called url which has list of urls with their names ... can i have a new df called url_df and split the urls into their respective name s... so slugs in cola a then increase the width of data set add col  for website , twitter etc ... no of rows should not increase ... make sure we have slug names ... thats our primary index .. we need the slug value as it is
import pandas as pd
# Create an empty dictionary to store the URLs
url_data = {}


# Iterate over the DataFrame
for index, row in df.iterrows():
    urls = row['urls']
    url_data[index] = {}  # Initialize a dictionary for the current slug
    for url_name, url_value in urls.items():
        url_data[index][url_name] = url_value

# Convert the dictionary to a DataFrame
url_df = pd.DataFrame.from_dict(url_data, orient='index')

# prompt: url_df can we get all the rows cleaned ... no [] or commas anywher e

# Apply a function to replace brackets and commas
def clean_values(value):
  if isinstance(value, list):
    return ' '.join(str(x) for x in value)
  elif isinstance(value, str):
    return value.replace('[', '').replace(']', '').replace(',', '')
  else:
    return value

# Apply the function to all columns in the DataFrame
url_df = url_df.applymap(clean_values)



# prompt: pd.concat df [slug, name , id , logo , description] and url df

import pandas as pd
# Select the desired columns from df
df_selected = df[['id', 'name', 'slug', 'logo', 'description']]

# Reset the index of both DataFrames
df_selected = df_selected.reset_index()
url_df = url_df.reset_index()

# Concatenate the DataFrames
final_df = pd.concat([df_selected, url_df], axis=1)

# prompt: final_df, drop index

final_df = final_df.drop(columns=['index'])

final_df.info()

"""from sqlalchemy import create_engine


# Create a SQLAlchemy engine to connect to the MySQL database
engine = create_engine('mysql+mysqlconnector://yogass09:jaimaakamakhya@dbcp.cry66wamma47.ap-south-1.rds.amazonaws.com:3306/dbcp')

"""


# Connection parameters
db_host = "34.47.215.135"         # Public IP of your PostgreSQL instance on GCP
db_name = "dbcp"                  # Database name
db_user = "yogass09"              # Database username
db_password = "jaimaakamakhya"     # Database password
db_port = 5432                    # PostgreSQL port

# Create a SQLAlchemy engine for PostgreSQL
gcp_engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')


# Write the DataFrame to a new table in the database
final_df.to_sql('FE_CC_INFO_URL', con=gcp_engine, if_exists='replace', index=False)


gcp_engine.dispose()

