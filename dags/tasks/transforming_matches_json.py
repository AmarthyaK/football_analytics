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

## read matchweek:
def get_match_week(**kwargs):
    root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'
    file_path = os.path.join(root_dir,'access_keys.yaml')
    keys = read_yaml(file_path)

    access_key = keys.get('AWS_creds')['access_key']
    secret_access_key = keys.get('AWS_creds')['secret_access_key']

    s3 = boto3.client('s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_access_key,
                    region_name='us-east-1')
    
    bucket_name = 'football-analytics-amark'
    s3_matchweek_tracker_key = 'matchweek_tracker/tracker.csv'

    # Get the current matchweek from S3
    try:
        response = s3.get_object(Bucket=bucket_name, Key=s3_matchweek_tracker_key)
        matchweek_tracker = pd.read_csv(response['Body'])
        successful_matchweek = matchweek_tracker[matchweek_tracker['status'] == 'success']

        matchweek = successful_matchweek.iloc[-1, 0]

    except:
        matchweek = 1
    return int(matchweek)

## function to update tracker
def update_tracker(matchweek):
    root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'

    file_path = os.path.join(root_dir,'access_keys.yaml')
    keys = read_yaml(file_path)

    api_key = keys.get('football_source_API_key')['api_key']
    access_key = keys.get('AWS_creds')['access_key']
    secret_access_key = keys.get('AWS_creds')['secret_access_key']
        
    s3 = boto3.client('s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_access_key,
                    region_name='us-east-1')
    
    bucket_name = 'football-analytics-amark'
    s3_matchweek_tracker_key = 'matchweek_tracker/tracker.csv'

    response = s3.get_object(Bucket=bucket_name, Key=s3_matchweek_tracker_key)
    matchweek_tracker_df = pd.read_csv(response['Body'])

    # New row as a DataFrame (for compatibility with pandas 2.0+)
    new_row_df = pd.DataFrame({'matchweek': [matchweek], 'status': ['success']})

    # Append new row using concat
    matchweek_tracker_df = pd.concat([matchweek_tracker_df, new_row_df], ignore_index=True)

    csv_buffer = StringIO()
    matchweek_tracker_df.to_csv(csv_buffer, index=False)

    s3.put_object(Bucket=bucket_name, Key=s3_matchweek_tracker_key, Body=csv_buffer.getvalue())

    


# function to read json file from s3:
def read_json_s3(bucket_name,key):

    root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'

    file_path = os.path.join(root_dir,'access_keys.yaml')
    keys = read_yaml(file_path)

    api_key = keys.get('football_source_API_key')['api_key']
    access_key = keys.get('AWS_creds')['access_key']
    secret_access_key = keys.get('AWS_creds')['secret_access_key']
        
    s3 = boto3.client('s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_access_key,
                    region_name='us-east-1')
    try:
        # Download the file from S3
        response = s3.get_object(Bucket=bucket_name, Key=key)
        
        # Read the file content (response['Body'] is a stream)
        content = response['Body'].read().decode('utf-8')
        
        # Parse the content as JSON and return it as a Python dictionary
        return json.loads(content)
    
    except Exception as e:
        print(f"Error reading JSON from S3: {e}")
        return None
    

def transforming_matches(**kwargs):

    root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'

    file_path = os.path.join(root_dir,'access_keys.yaml')
    
    keys = read_yaml(file_path)

    access_key = keys.get('AWS_creds')['access_key']
    secret_access_key = keys.get('AWS_creds')['secret_access_key']

    s3 = boto3.client('s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_access_key,
                    region_name='us-east-1')
    
    ## info about match start week:

    # ##match_week_information
    # match_week_user_input_file = os.path.join(root_dir,'matchweek_user_input.yaml')
    # match_week_user_input = read_yaml(match_week_user_input_file)
    # match_week = int(match_week_user_input.get('Matchweek')['Matchweek'])

    match_week = kwargs.get('match_week',None)

    ## reading matchweek_starting_date mapping file:

    matchweek_start_dates_file_path = os.path.join(root_dir,'matchweek_date_mapping.csv')
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

    
    bucket_name = 'football-analytics-amark'
    s3_json_key = f'raw_data/matches/match_data{start_date}to{end_date_naming}.json'

    matches_json = read_json_s3(bucket_name=bucket_name, key = s3_json_key)['matches']
    matches_df = pd.json_normalize(matches_json,max_level = 2,sep = '_')

    matches_df['winner'] = matches_df.apply(lambda x: x['homeTeam_name'] if x['score_winner'] == 'HOME_TEAM' else (x['awayTeam_name'] if x['score_winner'] else 'DRAW'), axis = 1)
    matches_df['Matchweek'] = match_week

    rel_cols = ['id','utcDate','Matchweek','stage','area_id','area_name','competition_id','homeTeam_id','awayTeam_id','score_winner','score_fullTime_home','score_fullTime_away']
    matches_df = matches_df[rel_cols]

    matches_df['utcDate'] = pd.to_datetime(matches_df['utcDate']).dt.date
    matches_df = matches_df.drop(columns='area_name')

    ## Time to load matches_df to parquet files with partition: Matchweek = 1

    output_dir = f's3://{bucket_name}/matches/'


    #### using awswrangler

    # Create a session with your AWS credentials
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key,
        region_name='us-east-1'
)

    # Delete the existing partition before writing new data
    partition_path = f"{output_dir}Matchweek={match_week}/"

    # Delete all objects in the existing partition
    wr.s3.delete_objects(
        path=partition_path,
        boto3_session=session
    )


    wr.s3.to_parquet(matches_df,
                        path = output_dir,partition_cols = ['Matchweek'],
                        dataset = True,
                        boto3_session=session,
                        mode = 'append')
    update_tracker(match_week)