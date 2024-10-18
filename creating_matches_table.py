import boto3
import os
import requests
import json
import os
from datetime import datetime, timedelta
import yaml
import pandas as pd
from io import StringIO
import snowflake.connector


# Function to read YAML file
def read_yaml(file_path):
    with open(file_path, 'r') as file:
        try:
            # Load the YAML content into a Python dictionary
            data = yaml.safe_load(file)
            return data
        except yaml.YAMLError as exc:
            print(f"Error reading YAML file: {exc}")
            return None



root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'
file_path = os.path.join(root_dir,'access_keys.yaml')
keys = read_yaml(file_path)

access_key = keys.get('AWS_creds')['access_key']
secret_access_key = keys.get('AWS_creds')['secret_access_key']
snowflake_role = keys.get('AWS_creds')['snowflake_role']

user_name = keys.get('snowflake_creds')['user_name']
password = keys.get('snowflake_creds')['password']
account_id = keys.get('snowflake_creds')['account_id']
warehouse = keys.get('snowflake_creds')['warehouse']

dbname = 'footballanalytics'
schema = 'footballanalytics_amark'
table_name = 'matches'
bucket_name = 'football-analytics-amark'

conn = snowflake.connector.connect(
    user = user_name,
    password = password,
    account = account_id,
    warehouse = warehouse,
    database = dbname,
    schema = schema
    )


## Creating teams table
cursor = conn.cursor()
create_table_query = f"""
CREATE OR REPLACE TABLE {table_name} (
    id INT PRIMARY KEY,
    utcDate DATE,
    Matchweek INT,
    stage VARCHAR(20),
    area_id INT,
    competition_id INT,
    homeTeam_id INT,
    awayTeam_id INT,
    score_winner VARCHAR(10),
    score_fullTime_home INT,
    score_fullTime_away INT
);       
"""
cursor.execute(create_table_query)