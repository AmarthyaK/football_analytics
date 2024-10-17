import boto3
import os
import requests
import json
import os
from datetime import datetime, timedelta
import yaml
import pandas as pd
from io import StringIO

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


def read_s3_csv(bucket_name,s3_csv_key):

    root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'

    file_path = os.path.join(root_dir,'access_keys.yaml')
    
    keys = read_yaml(file_path)

    access_key = keys.get('AWS_creds')['access_key']
    secret_access_key = keys.get('AWS_creds')['secret_access_key']

    s3 = boto3.client('s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_access_key,
                    region_name='us-east-1')

    response = s3.get_object(Bucket = bucket_name,Key = s3_csv_key)
    csv_content = response['Body'].read().decode('utf-8')
    csv_buffer = StringIO(csv_content)

    df = pd.read_csv(csv_buffer)
    return df

def loading_areas(**kwargs):

    root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'
    file_path = os.path.join(root_dir,'access_keys.yaml')
    keys = read_yaml(file_path)

    bucket_name = 'football-analytics-amark'
    s3_csv_key = f'area/area.csv'

    area_df = read_s3_csv(bucket_name = bucket_name,s3_csv_key = s3_csv_key)

