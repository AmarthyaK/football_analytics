## This python script is to understand how apache airflow works
## First task is to upload match json file to raw_data folder in s3


import os
import requests
import json
import os
from datetime import datetime, timedelta
import yaml
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import sys
import os
# sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'tasks'))

root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'
sys.path.append(os.path.join(root_dir,'tasks'))


from tasks.loading_starting_match_json import loading_first_match_week
from tasks.loading_competitions_json import loading_competitions
from tasks.loading_areas_json import loading_areas
from tasks.loading_teams_json import loading_teams


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



## DAG code below:
default_args = {
    'owner': 'amar_k',
    'start_date': datetime.now() + timedelta(minutes = 1),
    'retries':0
}
## define the DAG
with DAG('matchweek1_upload_s3_dag',
         default_args = default_args,
         schedule_interval = None,
         catchup=False) as dag:
    
    ## task 1
    load_matches_task = PythonOperator(
        task_id = 'load_matches_task',
        python_callable = loading_first_match_week
    )

    ## task 2
    load_teams_task = PythonOperator(
        task_id = 'load_teams_task',
        python_callable = loading_teams
    )

    ## task 3
    load_areas_task = PythonOperator(
        task_id = 'load_areas_task',
        python_callable = loading_areas
    )

    ## task 4
    load_competitions_task = PythonOperator(
        task_id = 'load_competitions_task',
        python_callable = loading_competitions
    )



## Dependencies

load_matches_task >> load_teams_task >> load_areas_task >> load_competitions_task

