import os
import yaml
from datetime import datetime, timedelta
import pandas as pd
import requests
import json

# current_dir = os.getcwd()
# print(current_dir)

# root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'

# # parent_dir = os.path.abspath(os.path.join(current_dir,'..'))
# print(root_dir)

# # Function to read YAML file
# def read_yaml(file_path):
#     with open(file_path, 'r') as file:
#         try:
#             # Load the YAML content into a Python dictionary
#             data = yaml.safe_load(file)
#             return data
#         except yaml.YAMLError as exc:
#             print(f"Error reading YAML file: {exc}")
#             return None


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

import sys
import os

root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'
print(os.path.join(root_dir,'tasks'))