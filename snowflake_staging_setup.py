import os
import yaml
import snowflake.connector

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

## staging

root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'
file_path = os.path.join(root_dir,'access_keys.yaml')
keys = read_yaml(file_path)

access_key = keys.get('AWS_creds')['access_key']
secret_access_key = keys.get('AWS_creds')['secret_access_key']
snowflake_role = keys.get('AWS_creds')['snowflake_role']


## conn config
user_name = keys.get('snowflake_creds')['user_name']
password = keys.get('snowflake_creds')['password']
account_id = keys.get('snowflake_creds')['account_id']
warehouse = keys.get('snowflake_creds')['warehouse']

dbname = 'footballanalytics'
schema = 'footballanalytics_amark'
bucket_name = 'football-analytics-amark'

conn = snowflake.connector.connect(
    user = user_name,
    password = password,
    account = account_id,
    warehouse = warehouse,
    database = dbname,
    schema = schema
    )
##integration step

cursor = conn.cursor()

# integration_query = f"""
# CREATE OR REPLACE STORAGE INTEGRATION s3_int
# TYPE = EXTERNAL_STAGE
# STORAGE_PROVIDER = 'S3'
# ENABLED = TRUE
# STORAGE_AWS_ROLE_ARN = '{snowflake_role}'
# STORAGE_ALLOWED_LOCATIONS = ('*')
# """
# cursor.execute(integration_query)

## creation of CSV_fileformat
create_csv_format_query = f"""
CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = CSV 
COMMENT = 'csv_format'
"""
cursor.execute(create_csv_format_query)



## staging step
cursor = conn.cursor()

use_db_schema_query = f"""USE {dbname}.{schema};"""
cursor.execute(use_db_schema_query)

# staging_query = f"""
# CREATE OR REPLACE STAGE my_s3_stage
#   URL='s3://{bucket_name}'
#   CREDENTIALS = (AWS_ROLE = '{snowflake_role}');      
# """

# staging_query_new = f"""
# CREATE OR REPLACE STAGE my_s3_stage_new
# STORAGE_INTEGRATION = s3_int
# URL = 's3://{bucket_name}/'
# FILE_FORMAT = my_csv_format;
# """
# cursor.execute(staging_query_new)

# describe_stage = """DESC STAGE my_S3_stage;"""
# cursor.execute(describe_stage)

# # Fetch all the results
# stage_description = cursor.fetchall()

# # Print the results
# for row in stage_description:
#     print(row)

## copy into table query
table_name = 'area'
cursor = conn.cursor()
create_table_query = f"""
CREATE OR REPLACE TABLE {table_name} (
    id INT,
    name VARCHAR (30),
    countryCode VARCHAR(4),
    parentAreaId INT
);       
"""
cursor.execute(create_table_query)

# copy_into_table_query = f"""
# COPY INTO area
# FROM @my_s3_stage_new
# """

copy_into_table_query_new = f"""
COPY INTO area
FROM s3://{bucket_name}/area/area.csv credentials=(AWS_KEY_ID='{access_key}' AWS_SECRET_KEY='{secret_access_key}')
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
"""

cursor.execute(copy_into_table_query_new)
cursor.close()
