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

## read matchweek:
def get_match_week(**kwargs):
    root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'
    file_path = os.path.join(root_dir,'access_keys.yaml')
    keys = read_yaml(file_path)

    access_key = keys.get('AWS_creds')['access_key']
    secret_access_key = keys.get('AWS_creds')['secret_access_key']

    s3 = boto3.client('s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_access_key,
                    region_name='us-east-1')
    
    bucket_name = 'football-analytics-amark'
    s3_matchweek_tracker_key = 'matchweek_tracker/tracker.csv'

    # Get the current matchweek from S3
    try:
        response = s3.get_object(Bucket=bucket_name, Key=s3_matchweek_tracker_key)
        matchweek_tracker = pd.read_csv(response['Body'])
        successful_matchweek = matchweek_tracker[matchweek_tracker['status'] == 'success']

        matchweek = successful_matchweek.iloc[-1, 0]

    except:
        matchweek = 1
    return int(matchweek)

def loading_matches(**kwargs):

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
    cursor = conn.cursor()

    match_week = kwargs.get('match_week',None)

    # if match_week == 1:

    #     ## Creating teams table
    #     cursor = conn.cursor()
    #     create_table_query = f"""
    #     CREATE OR REPLACE TABLE {table_name} (
    #         id INT PRIMARY KEY,
    #         utcDate DATE,
    #         Matchweek INT,
    #         stage VARCHAR(20),
    #         area_id INT,
    #         competition_id INT,
    #         homeTeam_id INT,
    #         awayTeam_id INT,
    #         score_winner VARCHAR(10),
    #         score_fullTime_home INT,
    #         score_fullTime_away INT
    #     );       
    #     """
    #     cursor.execute(create_table_query)
    # else:
    #     pass
        


    ## loading tables

    copy_into_table_query_new = f"""
    COPY INTO {table_name}
    FROM 's3://{bucket_name}/{table_name}/Matchweek={match_week}/'
    credentials=(AWS_KEY_ID='{access_key}' AWS_SECRET_KEY='{secret_access_key}')
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    """


    cursor.execute(copy_into_table_query_new)
    cursor.close()
