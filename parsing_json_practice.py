## practicing json parsing:


import pandas as pd

data = [
    {'id':1,'name': {'first_name':'John', 'last_name':'lennon'}},
    {'name':{'given':'Xuan','english_name': 'Ben'}},
    {'id':2,'name': 'Nirvan','Pet':'Chiko'}
]

# df = pd.DataFrame(data)
# print(df)

# ##normalized_json

# df_normalized = pd.json_normalize(data)
# print(df_normalized)

## complicated data:

import pandas as pd

# Set display options for better output formatting in pandas
pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 150)
pd.set_option('display.max_colwidth', 150)
pd.set_option('display.width', 120)
pd.set_option('expand_frame_repr', True)

# Sample data structure as shown in your image
data = [
    {
        "state": "Florida",
        "shortname": "FL",
        "info": {"governor": "Rick Scott"},
        "counties": [
            {"name": "Dade", "population": 12345},
            {"name": "Broward", "population": 40000},
            {"name": "Palm Beach", "population": 60000}
        ],
    },
    {
        "state": "Ohio",
        "shortname": "OH",
        "info": {"governor": "John Kasich","area_code":447},
        "counties": [
            {"name": "Summit", "population": 1234},
            {"name": "Cuyahoga", "population": 1337}
        ]
    }
]

# Convert the data into a pandas DataFrame
df = pd.DataFrame(data)

# Display the resulting DataFrame
#print(df)


df2 = pd.json_normalize(data,record_path = 'counties',meta = ['state','shortname',['info','area_code'],['info','governor']],errors = 'ignore')
print(df2)
