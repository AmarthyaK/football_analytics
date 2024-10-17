import boto3
import os
import requests
import json
import os
from datetime import datetime, timedelta
import yaml
import pandas as pd

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

def extracting_areas(**kwargs):


    root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'

    file_path = os.path.join(root_dir,'access_keys.yaml')
    
    keys = read_yaml(file_path)



    api_key = keys.get('football_source_API_key')['api_key']
    access_key = keys.get('AWS_creds')['access_key']
    secret_access_key = keys.get('AWS_creds')['secret_access_key']

    headers = {
    "X-Auth-Token": api_key
    }


    area_url = 'http://api.football-data.org/v4/areas/'
    area_response = requests.get(area_url, headers=headers)

    if area_response.status_code == 200:

        area_data = area_response.json()
        area_data_json = json.dumps(area_data)



        ## boto3 stage of the code:

        s3 = boto3.client('s3',
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_access_key,
                        region_name='us-east-1')

        bucket_name = 'football-analytics-amark'
        s3_key = f'raw_data/areas/areas.json'

        # Upload the JSON string directly to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=area_data_json,  # The JSON string
            ContentType='application/json'  # Specify content type as JSON
        )

        print(f"Uploaded JSON to s3://{bucket_name}/{s3_key}")
