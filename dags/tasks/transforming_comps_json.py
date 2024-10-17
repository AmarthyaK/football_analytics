import boto3
import os
import requests
import json
import os
from datetime import datetime, timedelta
import yaml
import pandas as pd
from io import StringIO
from unidecode import unidecode

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


# function to read json file from s3:
def read_json_s3(bucket_name,key):

    root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'

    file_path = os.path.join(root_dir,'access_keys.yaml')
    keys = read_yaml(file_path)



    api_key = keys.get('football_source_API_key')['api_key']
    access_key = keys.get('AWS_creds')['access_key']
    secret_access_key = keys.get('AWS_creds')['secret_access_key']
        
    s3 = boto3.client('s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_access_key,
                    region_name='us-east-1')
    try:
        # Download the file from S3
        response = s3.get_object(Bucket=bucket_name, Key=key)
        
        # Read the file content (response['Body'] is a stream)
        content = response['Body'].read().decode('utf-8')
        
        # Parse the content as JSON and return it as a Python dictionary
        return json.loads(content)
    
    except Exception as e:
        print(f"Error reading JSON from S3: {e}")
        return None
    
##apply unidecode
def apply_unidecode(x):

    if pd.isna(x):
        return x
    return unidecode(x)

def transforming_competitions(**kwargs):

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
    s3_json_key = f'raw_data/competitions/competitions.json'

    competitions_json = read_json_s3(bucket_name=bucket_name, key = s3_json_key)['competitions']
    comp_df_filtered = pd.json_normalize(competitions_json,sep = '_')

    comp_df_filtered = comp_df_filtered.loc[:, ~comp_df_filtered.columns.str.startswith('currentSeason')]



    #selecting relvant columns only

    rel_cols = ['id', 'name', 'code','type','area_id','area_name']

    comp_df_filtered = comp_df_filtered[rel_cols]



    # Convert DataFrame to CSV in-memory (as a string)
    csv_buffer = StringIO()
    comp_df_filtered.to_csv(csv_buffer, index=False)

    s3_csv_key = 'competition/competition.csv'
    
    # Upload the CSV to S3
    try:
        s3.put_object(Bucket=bucket_name, Key=s3_csv_key, Body=csv_buffer.getvalue())
        print(f"DataFrame successfully uploaded to {bucket_name}/{s3_csv_key}")
    except Exception as e:
        print(f"Error uploading DataFrame to S3: {e}")