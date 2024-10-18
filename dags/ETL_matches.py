## This python script is to understand how apache airflow works
## First task is to upload match json file to raw_data folder in s3


from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import time
import sys
import os
# sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'tasks'))

root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'
sys.path.append(os.path.join(root_dir,'tasks'))


# from tasks.loading_starting_match_json import loading_first_match_week
from tasks.extracting_matches_json import extracting_matches
from tasks.transforming_matches_json import transforming_matches
from tasks.loading_matches_sf import loading_matches



## function to read yaml:
# Function to read YAML file


## DAG code below:
default_args = {
    'owner': 'amar_k',
    # 'start_date': datetime.now() + timedelta(minutes = 2),
    'start_date': datetime(2024,10,16),
    'retries':0
}
# ## define the DAG
# with DAG('ETL_match_dag',
#          default_args = default_args,
#          schedule_interval = '@once',
#          catchup=False) as dag:

#     ## task 1
#     extracting_matches_task = PythonOperator(
#         task_id = 'extract_matches_task',
#         python_callable = extracting_matches
#     )    

#     ## task 2
#     transform_matches_task = PythonOperator(
#         task_id = 'transform_matches_task',
#         python_callable = transforming_matches
#     )
#     ## task 3
#     loading_matches_task = PythonOperator(
#         task_id = 'loading_matches_task',
#         python_callable = loading_matches
#     )


# extracting_matches_task >> transform_matches_task >> loading_matches_task


###iterative dags:

# Define a simple sleep function to introduce delay
def sleep_task(**kwargs):
    time.sleep(2)  # 60 seconds = 1 minute delay
    print(f"Sleeping for 5 seconds between tasks")
def sleep_task2(**kwargs):
    time.sleep(2)  # 60 seconds = 1 minute delay
    print(f"Sleeping for 6 seconds between tasks")

# Define the DAG
with DAG('ETL_match_dag',
         default_args=default_args,
         schedule_interval='@once',
         concurrency=8,
         catchup=False) as dag:

# Initialize previous_task as None to set dependencies later

 

    for i in range(1, 11):  # Loop to create tasks for 10 iterations

        ## Sleep Task to introduce a 1-minute buffer between iterations
        sleep_between_iterations_task = PythonOperator(
            task_id=f'sleep_task_{i}',  # Unique task_id for each iteration
            python_callable=sleep_task  # Calls the function that sleeps for 60 seconds
        )    

        ## Sleep Task to introduce a 1-minute buffer between iterations
        sleep_between_iterations_task2 = PythonOperator(
            task_id=f'sleep_task2_{i}',  # Unique task_id for each iteration
            python_callable=sleep_task2  # Calls the function that sleeps for 60 seconds
        )  
        
        ## Task 1: Extracting matches
        extracting_matches_task = PythonOperator(
            task_id=f'extract_matches_task_{i}',  # Unique task_id for each iteration
            python_callable=extracting_matches,
            op_kwargs={'match_week': i}  # Pass iteration number as argument
        )

        ## Task 2: Transforming matches
        transform_matches_task = PythonOperator(
            task_id=f'transform_matches_task_{i}',  # Unique task_id for each iteration
            python_callable=transforming_matches,
            op_kwargs={'match_week': i}   # Pass iteration number as argument
        )

        ## Task 3: Loading matches
        loading_matches_task = PythonOperator(
            task_id=f'loading_matches_task_{i}',  # Unique task_id for each iteration
            python_callable=loading_matches,
            op_kwargs={'match_week': i}   # Pass iteration number as argument
        )

        extracting_matches_task >> sleep_between_iterations_task >> transform_matches_task >> sleep_between_iterations_task2 >> loading_matches_task


