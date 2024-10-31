## This python script is to understand how apache airflow works
## First task is to upload match json file to raw_data folder in s3


from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import time
import sys
import os
from airflow.utils.helpers import chain
# sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'tasks'))

root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'
sys.path.append(os.path.join(root_dir,'tasks'))


# from tasks.loading_starting_match_json import loading_first_match_week
from tasks.extracting_teams_json import extracting_teams
from tasks.transforming_teams_json import transforming_teams
from tasks.loading_teams_sf import loading_teams



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
#     extracting_teams_task = PythonOperator(
#         task_id = 'extract_teams_task',
#         python_callable = extracting_teams
#     )    

#     ## task 2
#     transform_teams_task = PythonOperator(
#         task_id = 'transform_teams_task',
#         python_callable = transforming_teams
#     )
#     ## task 3
#     loading_teams_task = PythonOperator(
#         task_id = 'loading_teams_task',
#         python_callable = loading_teams
#     )


# extracting_teams_task >> transform_teams_task >> loading_teams_task

###iterative dags:

# Define a simple sleep function to introduce delay
def sleep_task(**kwargs):
    time.sleep(1)  # 60 seconds = 1 minute delay
    print(f"Sleeping for 5 seconds between tasks")
def sleep_task2(**kwargs):
    time.sleep(1)  # 60 seconds = 1 minute delay
    print(f"Sleeping for 6 seconds between tasks")

# # Define the DAG
# with DAG('ETL_match_dag',
#          default_args=default_args,
#          schedule_interval='@once',
#          concurrency=8,
#          catchup=False) as dag:

# # Initialize previous_task as None to set dependencies later

#     for i in range(1, 11):  # Loop to create tasks for 10 iterations

#         ## Sleep Task to introduce a 1-minute buffer between iterations
#         sleep_between_iterations_task = PythonOperator(
#             task_id=f'sleep_task_{i}',  # Unique task_id for each iteration
#             python_callable=sleep_task  # Calls the function that sleeps for 60 seconds
#         )    

#         ## Sleep Task to introduce a 1-minute buffer between iterations
#         sleep_between_iterations_task2 = PythonOperator(
#             task_id=f'sleep_task2_{i}',  # Unique task_id for each iteration
#             python_callable=sleep_task2  # Calls the function that sleeps for 60 seconds
#         )  
        
#         ## Task 1: Extracting matches
#         extracting_teams_task = PythonOperator(
#             task_id=f'extract_teams_task_{i}',  # Unique task_id for each iteration
#             python_callable=extracting_teams,
#             op_kwargs={'match_week': i}  # Pass iteration number as argument
#         )

#         ## Task 2: Transforming matches
#         transform_teams_task = PythonOperator(
#             task_id=f'transform_teams_task_{i}',  # Unique task_id for each iteration
#             python_callable=transforming_teams,
#             op_kwargs={'match_week': i}   # Pass iteration number as argument
#         )

#         ## Task 3: Loading matches
#         loading_teams_task = PythonOperator(
#             task_id=f'loading_teams_task_{i}',  # Unique task_id for each iteration
#             python_callable=loading_teams,
#             op_kwargs={'match_week': i}   # Pass iteration number as argument
#         )

#         extracting_teams_task >> sleep_between_iterations_task >> transform_teams_task >> sleep_between_iterations_task2 >> loading_teams_task

### Acheiving parallelism:

with DAG('ETL_teams_parallel_check_dag',
         default_args=default_args,
         schedule_interval='@once',
         concurrency=8,
         catchup=False) as dag:
    
    # task 1
        extracting_teams_task_1 = PythonOperator(
            task_id=f'extract_teams_task_1',  # Unique task_id for each iteration
            python_callable=extracting_teams,
            op_kwargs={'match_week': 1}  # Pass iteration number as argument
        )

    # task 2
        extracting_teams_task_2 = PythonOperator(
            task_id=f'extract_teams_task_2',  # Unique task_id for each iteration
            python_callable=extracting_teams,
            op_kwargs={'match_week': 2}  # Pass iteration number as argument
        )

    # task 3
        extracting_teams_task_3 = PythonOperator(
            task_id=f'extract_teams_task_3',  # Unique task_id for each iteration
            python_callable=extracting_teams,
            op_kwargs={'match_week': 3}  # Pass iteration number as argument
        )

    # task 4
        extracting_teams_task_4 = PythonOperator(
            task_id=f'extract_teams_task_4',  # Unique task_id for each iteration
            python_callable=extracting_teams,
            op_kwargs={'match_week': 4}  # Pass iteration number as argument
        )

    # task 5
        extracting_teams_task_5 = PythonOperator(
            task_id=f'extract_teams_task_5',  # Unique task_id for each iteration
            python_callable=extracting_teams,
            op_kwargs={'match_week': 5}  # Pass iteration number as argument
        )

    # task 6
        extracting_teams_task_6 = PythonOperator(
            task_id=f'extract_teams_task_6',  # Unique task_id for each iteration
            python_callable=extracting_teams,
            op_kwargs={'match_week': 6}  # Pass iteration number as argument
        )

    # task 7
        extracting_teams_task_7 = PythonOperator(
            task_id=f'extract_teams_task_7',  # Unique task_id for each iteration
            python_callable=extracting_teams,
            op_kwargs={'match_week': 7}  # Pass iteration number as argument
        )

    # task 8
        extracting_teams_task_8 = PythonOperator(
            task_id=f'extract_teams_task_8',  # Unique task_id for each iteration
            python_callable=extracting_teams,
            op_kwargs={'match_week': 8}  # Pass iteration number as argument
        )

    # task 9
        extracting_teams_task_9 = PythonOperator(
            task_id=f'extract_teams_task_9',  # Unique task_id for each iteration
            python_callable=extracting_teams,
            op_kwargs={'match_week': 9}  # Pass iteration number as argument
        )
    
    # task 10
        extracting_teams_task_10 = PythonOperator(
            task_id=f'extract_teams_task_10',  # Unique task_id for each iteration
            python_callable=extracting_teams,
            op_kwargs={'match_week': 10}  # Pass iteration number as argument
        )

    # task 1
        transforming_teams_task_1 = PythonOperator(
            task_id=f'transform_teams_task_1',  # Unique task_id for each iteration
            python_callable=transforming_teams,
            op_kwargs={'match_week': 1}  # Pass iteration number as argument
        )

    # task 2
        transforming_teams_task_2 = PythonOperator(
            task_id=f'transform_teams_task_2',  # Unique task_id for each iteration
            python_callable=transforming_teams,
            op_kwargs={'match_week': 2}  # Pass iteration number as argument
        )

    # task 3
        transforming_teams_task_3 = PythonOperator(
            task_id=f'transform_teams_task_3',  # Unique task_id for each iteration
            python_callable=transforming_teams,
            op_kwargs={'match_week': 3}  # Pass iteration number as argument
        )

    # task 4
        transforming_teams_task_4 = PythonOperator(
            task_id=f'transform_teams_task_4',  # Unique task_id for each iteration
            python_callable=transforming_teams,
            op_kwargs={'match_week': 4}  # Pass iteration number as argument
        )

    # task 5
        transforming_teams_task_5 = PythonOperator(
            task_id=f'transform_teams_task_5',  # Unique task_id for each iteration
            python_callable=transforming_teams,
            op_kwargs={'match_week': 5}  # Pass iteration number as argument
        )

    # task 6
        transforming_teams_task_6 = PythonOperator(
            task_id=f'transform_teams_task_6',  # Unique task_id for each iteration
            python_callable=transforming_teams,
            op_kwargs={'match_week': 6}  # Pass iteration number as argument
        )

    # task 7
        transforming_teams_task_7 = PythonOperator(
            task_id=f'transform_teams_task_7',  # Unique task_id for each iteration
            python_callable=transforming_teams,
            op_kwargs={'match_week': 7}  # Pass iteration number as argument
        )

    # task 8
        transforming_teams_task_8 = PythonOperator(
            task_id=f'transform_teams_task_8',  # Unique task_id for each iteration
            python_callable=transforming_teams,
            op_kwargs={'match_week': 8}  # Pass iteration number as argument
        )

    # task 9
        transforming_teams_task_9 = PythonOperator(
            task_id=f'transform_teams_task_9',  # Unique task_id for each iteration
            python_callable=transforming_teams,
            op_kwargs={'match_week': 9}  # Pass iteration number as argument
        )
    
    # task 10
        transforming_teams_task_10 = PythonOperator(
            task_id=f'transform_teams_task_10',  # Unique task_id for each iteration
            python_callable=transforming_teams,
            op_kwargs={'match_week': 10}  # Pass iteration number as argument
        )

    # task 1
        loading_teams_task_1 = PythonOperator(
            task_id=f'load_teamses_task_1',  # Unique task_id for each iteration
            python_callable=loading_teams,
            op_kwargs={'match_week': 1}  # Pass iteration number as argument
        )

    # task 2
        loading_teams_task_2 = PythonOperator(
            task_id=f'load_teamses_task_2',  # Unique task_id for each iteration
            python_callable=loading_teams,
            op_kwargs={'match_week': 2}  # Pass iteration number as argument
        )

    # task 3
        loading_teams_task_3 = PythonOperator(
            task_id=f'load_teamses_task_3',  # Unique task_id for each iteration
            python_callable=loading_teams,
            op_kwargs={'match_week': 3}  # Pass iteration number as argument
        )

    # task 4
        loading_teams_task_4 = PythonOperator(
            task_id=f'load_teamses_task_4',  # Unique task_id for each iteration
            python_callable=loading_teams,
            op_kwargs={'match_week': 4}  # Pass iteration number as argument
        )

    # task 5
        loading_teams_task_5 = PythonOperator(
            task_id=f'load_teamses_task_5',  # Unique task_id for each iteration
            python_callable=loading_teams,
            op_kwargs={'match_week': 5}  # Pass iteration number as argument
        )

    # task 6
        loading_teams_task_6 = PythonOperator(
            task_id=f'load_teamses_task_6',  # Unique task_id for each iteration
            python_callable=loading_teams,
            op_kwargs={'match_week': 6}  # Pass iteration number as argument
        )

    # task 7
        loading_teams_task_7 = PythonOperator(
            task_id=f'load_teamses_task_7',  # Unique task_id for each iteration
            python_callable=loading_teams,
            op_kwargs={'match_week': 7}  # Pass iteration number as argument
        )

    # task 8
        loading_teams_task_8 = PythonOperator(
            task_id=f'load_teamses_task_8',  # Unique task_id for each iteration
            python_callable=loading_teams,
            op_kwargs={'match_week': 8}  # Pass iteration number as argument
        )

    # task 9
        loading_teams_task_9 = PythonOperator(
            task_id=f'load_teamses_task_9',  # Unique task_id for each iteration
            python_callable=loading_teams,
            op_kwargs={'match_week': 9}  # Pass iteration number as argument
        )
    
    # task 10
        loading_teams_task_10 = PythonOperator(
            task_id=f'load_teamses_task_10',  # Unique task_id for each iteration
            python_callable=loading_teams,
            op_kwargs={'match_week': 10}  # Pass iteration number as argument
        )

# DAG dependencies
## Setting up task dependencies using chain function

chain(

[extracting_teams_task_1,extracting_teams_task_2,extracting_teams_task_3,
 extracting_teams_task_4,extracting_teams_task_5,extracting_teams_task_6,
 extracting_teams_task_7,extracting_teams_task_8,extracting_teams_task_9,
 extracting_teams_task_10],

[transforming_teams_task_1,transforming_teams_task_2,transforming_teams_task_3,
 transforming_teams_task_4,transforming_teams_task_5,transforming_teams_task_6,
 transforming_teams_task_7,transforming_teams_task_8,transforming_teams_task_9,
 transforming_teams_task_10],

[loading_teams_task_1,loading_teams_task_2,loading_teams_task_3,
 loading_teams_task_4,loading_teams_task_5,loading_teams_task_6,
 loading_teams_task_7,loading_teams_task_8,loading_teams_task_9,
 loading_teams_task_10]
)