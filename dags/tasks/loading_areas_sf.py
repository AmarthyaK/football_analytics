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


# def read_s3_csv(bucket_name,s3_csv_key):

#     root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'

#     file_path = os.path.join(root_dir,'access_keys.yaml')
    
#     keys = read_yaml(file_path)

#     access_key = keys.get('AWS_creds')['access_key']
#     secret_access_key = keys.get('AWS_creds')['secret_access_key']

#     s3 = boto3.client('s3',
#                     aws_access_key_id=access_key,
#                     aws_secret_access_key=secret_access_key,
#                     region_name='us-east-1')

#     response = s3.get_object(Bucket = bucket_name,Key = s3_csv_key)
#     csv_content = response['Body'].read().decode('utf-8')
#     csv_buffer = StringIO(csv_content)

#     df = pd.read_csv(csv_buffer)
#     return df

def loading_areas(**kwargs):

    root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'
    file_path = os.path.join(root_dir,'access_keys.yaml')
    keys = read_yaml(file_path)

    access_key = keys.get('AWS_creds')['access_key']
    secret_access_key = keys.get('AWS_creds')['secret_access_key']
    snowflake_role = keys.get('AWS_creds')['snowflake_role']

    # s3 = boto3.client('s3',
    #                 aws_access_key_id=access_key,
    #                 aws_secret_access_key=secret_access_key,
    #                 region_name='us-east-1')

    ## creating areas table
    user_name = keys.get('snowflake_creds')['user_name']
    password = keys.get('snowflake_creds')['password']
    account_id = keys.get('snowflake_creds')['account_id']
    warehouse = keys.get('snowflake_creds')['warehouse']

    dbname = 'footballanalytics'
    schema = 'footballanalytics_amark'
    table_name = 'area'

    conn = snowflake.connector.connect(
        user = user_name,
        password = password,
        account = account_id,
        warehouse = warehouse,
        database = dbname,
        schema = schema
        )

    
    try:

        ## Creating areas table
        cursor = conn.cursor()
        create_table_query = f"""
        CREATE OR REPLACE TABLE {table_name} (
            id INT,
            name VARCHAR (20),
            countryCode VARCHAR(4),
            parentAreaId INT
        );       
        """
        cursor.execute(create_table_query)
        cursor.close()

    except Exception as e:
        print(e)

    ## loading dataframe into table time:
    bucket_name = 'football-analytics-amark'
    s3_csv_key = f'{table_name}/{table_name}.csv'
    s3_path = f"s3://{bucket_name}/{s3_csv_key}"

    # area_df = read_s3_csv(bucket_name = bucket_name,s3_csv_key = s3_csv_key)

    ## Using Copy command, trying to write the s3 bucket directly into the table


    cursor = conn.cursor()
    truncate_query = f"TRUNCATE TABLE {table_name};"
    cursor.execute(truncate_query)

    ##creating stage areas:
    stage_area_query = f"""CREATE OR REPLACE STAGE my_s3_{table_name}_stage
    URL = 's3://{bucket_name}/{table_name}/'
    CREDENTIALS = (AWS_ROLE = '{snowflake_role}');
    """
    cursor.execute(stage_area_query)


    ## copying to stage
    load_query = f"""
    COPY INTO {table_name}
    FROM @my_s3_{table_name}_stage/{table_name}.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
    """
    cursor.execute(load_query)

    print(f"Data loaded from {s3_path} into area successfully.")
    cursor.close()

