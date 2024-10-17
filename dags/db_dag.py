## This python script is to understand how apache airflow works
## First task is to upload match json file to raw_data folder in s3


import os
import requests
import json
import os
from datetime import datetime, timedelta, timezone
import yaml
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


import sys
import os
# sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'tasks'))

root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'
sys.path.append(os.path.join(root_dir,'tasks'))


# from tasks.loading_starting_match_json import loading_first_match_week
from tasks.creating_sf_db import creating_sf_db_schema


## function to read yaml:
# Function to read YAML file


## DAG code below:
default_args = {
    'owner': 'amar_k',
    # 'start_date': datetime.now() + timedelta(minutes = 2),
    'start_date': datetime(2024,10,16),
    'retries':0
}
## define the DAG
with DAG('db_dag',
         default_args = default_args,
         schedule_interval = '@once',
         catchup=False) as dag:

    ## task 1
    create_db_task = PythonOperator(
        task_id = 'creating_db_schema_task',
        python_callable = creating_sf_db_schema
    )    
