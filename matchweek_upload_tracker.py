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

bucket_name = 'football-analytics-amark'
s3_matchweek_tracker_key = 'matchweek_tracker/tracker.csv'

root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'

file_path = os.path.join(root_dir,'access_keys.yaml')

keys = read_yaml(file_path)

access_key = keys.get('AWS_creds')['access_key']
secret_access_key = keys.get('AWS_creds')['secret_access_key']

s3 = boto3.client('s3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_access_key,
                region_name='us-east-1')


#creating matchweek_tracker_empty dataframe
matchweek_tracker_df = pd.DataFrame(columns=['matchweek', 'status'])


csv_buffer = StringIO()
matchweek_tracker_df.to_csv(csv_buffer, index=False)

s3.put_object(Bucket=bucket_name, Key=s3_matchweek_tracker_key, Body=csv_buffer.getvalue())
