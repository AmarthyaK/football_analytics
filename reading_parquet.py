import boto3
import os
import requests
import json
import os
from datetime import datetime, timedelta
import yaml
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import StringIO
from pyarrow.fs import S3FileSystem

import awswrangler as wr

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

s3 = boto3.client('s3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_access_key,
                region_name='us-east-1')

# Specify the bucket and file path
bucket_name = 'football-analytics-amark'
file_path = f's3://{bucket_name}/matches/'

# # Read the Parquet file directly into a pandas DataFrame
# obj = s3.get_object(Bucket=bucket_name, Key=file_path)
# df = pd.read_parquet(obj['Body'])

session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_access_key,
    region_name='us-east-1'
)

df = wr.s3.read_parquet(
                    path = file_path,

                    boto3_session=session
)

print(df.info())
