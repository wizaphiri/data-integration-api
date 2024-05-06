#!/usr/bin/python3
from sqlalchemy import create_engine
from datetime import datetime
from io import BytesIO
import pandas as pd
import requests
import os

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': Data integration initialized')
os.environ['DATA_CLIENT_ID'] = "XXXXXXXXXXXX"
os.environ['DATA_CLIENT_SECRET'] = "XXXXXXXXXXXXXXXX"

AUTH_URL = (
    "https://........./token"
)
# Get ID Token
r = requests.post(
    AUTH_URL,
    headers={"content-type": "application/x-www-form-urlencoded"},
    data={
        "grant_type": "grant type",
        "client_id": os.environ["DATA_CLIENT_ID"],
        "client_secret": os.environ["DATA_CLIENT_SECRET"],
    },
)
headers = {"Authorization": "Bearer " + r.json()["access_token"]}

DATASET_URL = "https://..../data.csv"
r = requests.get(
    DATASET_URL,
    headers=headers,
    # allow_redirects=True
)

df = pd.read_csv(BytesIO(r.content))

# export df to csv
# print(df.info())

engine = create_engine('postgresql+psycopg2://user:password@localhost/db1')
with engine.begin() as conn:
    conn.exec_driver_sql("DROP TABLE IF EXISTS integration_res")
    #print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': table integration_res dropped')

df.to_sql('integration_res', engine, if_exists='append')
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': Data integration complete')

