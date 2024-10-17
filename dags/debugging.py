import os
import yaml
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import boto3
# current_dir = os.getcwd()
# print(current_dir)

# root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'

# # parent_dir = os.path.abspath(os.path.join(current_dir,'..'))
# print(root_dir)

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


# file_path = os.path.join(root_dir,'access_keys.yaml')

# matches_url = 'https://api.football-data.org/v4/matches'

# keys = read_yaml(file_path)

# api_key = keys.get('football_source_API_key')['api_key']
# access_key = keys.get('AWS_creds')['access_key']
# secret_access_key = keys.get('AWS_creds')['secret_access_key']

# headers = {
# "X-Auth-Token": api_key
# }

# ##match_week_information
# match_week_user_input_file = os.path.join(root_dir,'matchweek_user_input.yaml')
# match_week_user_input = read_yaml(match_week_user_input_file)
# match_week = int(match_week_user_input.get('Matchweek')['Matchweek'])


# ## reading matchweek_starting_date mapping file:

# matchweek_start_dates_file_path = os.path.join(root_dir,'matchweek_date_mapping.csv')
# matchweek_start_dates = pd.read_csv(matchweek_start_dates_file_path)
# matchweek_start_date = matchweek_start_dates[matchweek_start_dates['MatchWeek']==match_week]['Start_date'].values[0]


# print(matchweek_start_date)
# start_date = datetime.strptime(matchweek_start_date, "%m/%d/%Y")
# print(start_date)

# date_plus_one_week = start_date + timedelta(weeks=1)
# end_date_naming = start_date + timedelta(days=6)

# start_date = start_date.strftime("%Y-%m-%d")
# end_date = date_plus_one_week.strftime("%Y-%m-%d")
# end_date_naming = end_date_naming.strftime("%Y-%m-%d")

# match_params = {
#     "dateFrom": f"{start_date}",
#     "dateTo": f"{end_date}"
# }

# match_response = requests.get(matches_url, headers=headers, params = match_params)

# if match_response.status_code == 200:

#     match_data = match_response.json()
#     print('success!')
#     # match_data_json = json.dumps(match_data)
#     print(match_data)

# print(api_key)

# import sys
# import os

# root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'
# print(os.path.join(root_dir,'tasks'))


# def read_json_s3(bucket_name,key):

#     root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'

#     file_path = os.path.join(root_dir,'access_keys.yaml')
#     keys = read_yaml(file_path)



#     api_key = keys.get('football_source_API_key')['api_key']
#     access_key = keys.get('AWS_creds')['access_key']
#     secret_access_key = keys.get('AWS_creds')['secret_access_key']
        
#     s3 = boto3.client('s3',
#                     aws_access_key_id=access_key,
#                     aws_secret_access_key=secret_access_key,
#                     region_name='us-east-1')
#     try:
#         # Download the file from S3
#         response = s3.get_object(Bucket=bucket_name, Key=key)
#         print(response)
        
#         # Read the file content (response['Body'] is a stream)
#         content = response['Body'].read().decode('utf-8')
        
#         # Parse the content as JSON and return it as a Python dictionary
#         return json.loads(content)['areas']
    
#     except Exception as e:
#         print(f"Error reading JSON from S3: {e}")
#         return None

# bucket_name = 'football-analytics-amark'
# s3_json_key = f'raw_data/areas/areas.json'

# area_json = read_json_s3(bucket_name=bucket_name, key = s3_json_key)
# print(area_json)
# area_df = pd.DataFrame(area_json)
# print(area_df.head(2))
# area_df = area_df.drop(columns = ['flag','parentArea'])



import pyarrow as pa
import pyarrow.parquet as pq
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

##match_week_information
match_week_user_input_file = os.path.join(root_dir,'matchweek_user_input.yaml')
match_week_user_input = read_yaml(match_week_user_input_file)
match_week = int(match_week_user_input.get('Matchweek')['Matchweek'])


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

print(matches_df.head())

## Time to load matches_df to parquet files with partition: Matchweek = 1

output_dir = f'{bucket_name}/matches'

# Create the path for partitioning (based on matchweek)
partitioned_path = os.path.join(output_dir, f'Matchweek={match_week}')
table = pa.Table.from_pandas(matches_df)

# print(table)
from pyarrow.fs import S3FileSystem
# Create S3 filesystem object
s3 = S3FileSystem()

try:
    pq.write_to_dataset(
        table,
        root_path=output_dir,
        filesystem = s3,
        partition_cols=['Matchweek']
    )

    print(f"Data written to Parquet format at {partitioned_path}")
except Exception as e:
    print(e)