
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

def create_general_db_schema(conn,dbname,schema):

    try:

        ## Creating a databse
        cursor = conn.cursor()
        create_db_query = f"CREATE OR REPLACE DATABASE {dbname}"
        cursor.execute(create_db_query)

        ##Using the database
        use_db_query = f"USE DATABASE {dbname}"
        cursor.execute(use_db_query)

        ##Creating Schema
        create_schema_query = f"CREATE OR REPLACE SCHEMA {schema}"
        cursor.execute(create_schema_query)

        cursor.close()
    except Exception as e:
        print(e)


def creating_sf_db_schema(**kwargs):

    root_dir = '/home/amarubuntu/football_analytics_project/football_analytics'
    file_path = os.path.join(root_dir,'access_keys.yaml')
    keys = read_yaml(file_path)

    user_name = keys.get('snowflake_creds')['user_name']
    password = keys.get('snowflake_creds')['password']
    account_id = keys.get('snowflake_creds')['account_id']
    warehouse = keys.get('snowflake_creds')['warehouse']

    conn = snowflake.connector.connect(
        user = user_name,
        password = password,
        account = account_id,
        warehouse = warehouse
        )
    dbname = 'footballanalytics'
    schema = 'footballanalytics_amark'
    create_general_db_schema(conn = conn,dbname=dbname,schema=schema)