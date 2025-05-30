import http.client
import pandas as pd
import json
from sqlalchemy import create_engine

# variable for api connection to collectapi
conn = http.client.HTTPSConnection("api.collectapi.com")

# api token dictionary for collectapi auth
headers = {
    'content-type': "application/json",
    'authorization': "apikey 0p51iJGo2yd7ALX7Q6VRHO:4mpH0rWGoseQMuTDF7SI1I"
    }

# get response from api
conn.request("GET", "/gasPrice/stateUsaPrice?state=WA", headers=headers)

res = conn.getresponse()
data = res.read()


print(data.decode("utf-8"))

parsed_data = json.loads(data.decode("utf-8"))

parsed_data

# extracting cities and result data from json script
cities_data = parsed_data['result']['cities']

# storing data in pandas df
df = pd.DataFrame(cities_data)

# dropping lowerName column from df
del df['lowerName']

# renaming column from name to city in df 
df = df.rename(columns={'name': 'city'})

# db credentials
user = 'postgres'
password = '1234'
host = '194.180.176.173'
port = '5432'
db_name = 'postgres'

# creating engine to establish connection to db
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}')


# Write to database using to_sql with the engine directly, not the connection
df.to_sql('austin_gas', engine, if_exists='replace', index=False)

print(pd.read_sql('SELECT * FROM gas', engine))
