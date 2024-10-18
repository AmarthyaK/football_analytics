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



def loading_competitions(**kwargs):

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
    table_name = 'competition'
    bucket_name = 'football-analytics-amark'

    conn = snowflake.connector.connect(
        user = user_name,
        password = password,
        account = account_id,
        warehouse = warehouse,
        database = dbname,
        schema = schema
        )

    
    try:

        ## Creating teams table
        cursor = conn.cursor()
        create_table_query = f"""
        CREATE OR REPLACE TABLE {table_name} (
            ID INT,
            name VARCHAR (60),
            code VARCHAR(5),
            type VARCHAR(10),
            area_id INT
        );       
        """
        cursor.execute(create_table_query)
        cursor.close()

    except Exception as e:
        print(e)



    ## loading tables
    cursor = conn.cursor()
    truncate_query = f"""TRUNCATE {table_name};"""
    cursor.execute(truncate_query)
    
    copy_into_table_query_new = f"""
    COPY INTO {table_name}
    FROM s3://{bucket_name}/{table_name}/{table_name}.csv credentials=(AWS_KEY_ID='{access_key}' AWS_SECRET_KEY='{secret_access_key}')
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
    """

    cursor.execute(copy_into_table_query_new)
    cursor.close()
