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


# from tasks.loading_starting_match_json import loading_first_match_week

#creating database task
from tasks.creating_sf_db import creating_sf_db_schema

##Extracting Tasks
from tasks.extracting_competitions_json import extracting_competitions
from tasks.extracting_areas_json import extracting_areas
from tasks.extracting_teams_json import extracting_teams

##Transformation Tasks
from tasks.transforming_comps_json import transforming_competitions
from tasks.transforming_areas_json import transforming_areas
from tasks.transforming_teams_json import transforming_teams

##Loading tasks
from tasks.loading_competitions_sf import loading_competitions
from tasks.loading_areas_sf import loading_areas
from tasks.loading_teams_sf import loading_teams

## function to read yaml:
# Function to read YAML file


## DAG code below:
default_args = {
    'owner': 'amar_k',
    'start_date': datetime.now() + timedelta(minutes = 2),
    'retries':0
}
## define the DAG
with DAG('ETL_area_team_comp_dag',
         default_args = default_args,
         schedule_interval = '@once',
         catchup=False) as dag:

    ## task 1
    load_competitions_task = PythonOperator(
        task_id = 'load_competitions_task',
        python_callable = loading_competitions
    )    

    ## task 2
    load_areas_task = PythonOperator(
        task_id = 'load_areas_task',
        python_callable = loading_areas
    )

    ## task 3
    load_teams_task = PythonOperator(
        task_id = 'load_teams_task',
        python_callable = loading_teams
    )

    ## task 4
    transform_competitions_task = PythonOperator(
        task_id = 'transform_competitions_task',
        python_callable = transforming_competitions
    )

    ## task 5
    transform_areas_task = PythonOperator(
        task_id = 'transform_areas_task',
        python_callable = transforming_areas
    )
    ## task 6
    transform_teams_task = PythonOperator(
        task_id = 'transform_teams_task',
        python_callable = transforming_teams
    ) 

    ## task 7
    extract_competitions_task = PythonOperator(
        task_id = 'extracting_competitions_task',
        python_callable = extracting_competitions
    )

    ## task 8
    extract_areas_task = PythonOperator(
        task_id = 'extracting_areas_task',
        python_callable = extracting_areas
    )
    ## task 9
    extract_teams_task = PythonOperator(
        task_id = 'extract_teams_task',
        python_callable = extracting_teams
    )
    ## task 10
    create_database_task = PythonOperator(
        task_id = 'create_database',
        python_callable = creating_sf_db_schema
    ) 

## Dependencies

# load_teams_task >> load_areas_task >> load_competitions_task

[extract_competitions_task >> transform_competitions_task,
extract_areas_task >> transform_areas_task,
extract_teams_task >> transform_teams_task] >> create_database_task >> [load_competitions_task,load_areas_task,load_teams_task]

