## This python script is to understand how apache airflow works
## First task is to upload match json file to raw_data folder in s3

import boto3
import os
import requests
import json
import os
from datetime import datetime, timedelta
import yaml
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


## function to read yaml:
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



## define the python function (or task to upload json to s3 folder):


def generate_json_upload_s3(**kwargs):

    current_dir = os.getcwd()
    file_path = os.path.join(current_dir,'access_keys.yaml')
    keys = read_yaml(file_path)

    matches_url = 'https://api.football-data.org/v4/matches'


    api_key = keys.get('api_key')
    access_key = keys.get('AWS_creds')['access_key']
    secret_access_key = keys.get('AWS_creds')['secret_access_key']

    headers = {
    "X-Auth-Token": api_key
    }

    ##match_week_information
    match_week_user_input_file = os.path.join(current_dir,'matchweek_user_input.yaml')
    match_week_user_input = read_yaml(match_week_user_input_file)
    match_week = int(match_week_user_input.get('Matchweek')['Matchweek'])


    ## reading matchweek_starting_date mapping file:

    matchweek_start_dates_file_path = os.path.join(current_dir,'matchweek_date_mapping.csv')
    matchweek_start_dates = pd.read_csv(matchweek_start_dates_file_path)
    matchweek_start_date = matchweek_start_dates[matchweek_start_dates['MatchWeek']==match_week]['Start_date'].values[0]


    print(matchweek_start_date)
    start_date = datetime.strptime(matchweek_start_date, "%m/%d/%Y")
    print(start_date)

    date_plus_one_week = start_date + timedelta(weeks=1)
    end_date_naming = start_date + timedelta(days=6)

    start_date = start_date.strftime("%Y-%m-%d")
    end_date = date_plus_one_week.strftime("%Y-%m-%d")
    end_date_naming = end_date_naming.strftime("%Y-%m-%d")

    match_params = {
        "dateFrom": f"{start_date}",
        "dateTo": f"{end_date}"
    }

    match_response = requests.get(matches_url, headers=headers, params = match_params)

    if match_response.status_code == 200:

        match_data = match_response.json()
        print('success!')
        match_data_json = json.dumps(match_data)


        ## boto3 stage of the code:

        s3 = boto3.client('s3',
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_access_key,
                        region_name='us-east-1')

        bucket_name = 'football-analytics-amark'
        s3_key = f'raw_data/matches/match_data{start_date}to{end_date_naming}.json'

        # Upload the JSON string directly to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=match_data_json,  # The JSON string
            ContentType='application/json'  # Specify content type as JSON
        )

        print(f"Uploaded JSON to s3://{bucket_name}/{s3_key}")


        

# generate_json_upload_s3()

## DAG code below:
default_args = {
    'owner': 'amar_k'
    'start_date': datetime.now() + timedelta(minutes = 3)
    'retries':1
}
## define the DAG
with DAG('generate_json_upload_s3_dag',
         default_args = default_args,
         schedule_interval = None,
         catchup=False) as dag:
    ## task 1
    generate_and_upload_task = PythonOperator(
        task_id = 'generate_upload_json_s3_task',
        python_callable = generate_json_upload_s3
    )


