#!/usr/bin/python3
from sqlalchemy import create_engine
from datetime import datetime
from io import BytesIO
import pandas as pd
import requests
import os

import warnings
warnings.filterwarnings('ignore')

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': Integration started......')
os.environ['client_id'] = "XXXXXXXXXXXX"
os.environ['client_key'] = "XXXXXXXXXXXXXXXX"

AUTH_URL = (
    "https://........./token"
)
# Get ID Token
r = requests.post(
    AUTH_URL,
    headers={"content-type": "application/x-www-form-urlencoded"},
    data={
        "grant_type": "grant type",
        "client_id": os.environ["client_id"],
        "client_secret": os.environ["client_key"],
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
df_records = df.shape[0]
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': '+str(df_records)+' records extracted')
# print(df.info())

engine = create_engine('postgresql+psycopg2://userx:passx@localhost/dbx')
with engine.begin() as conn:
    conn.exec_driver_sql("DROP TABLE IF EXISTS tablex")
    df.to_sql('tablex', engine, if_exists='append')

with engine.connect() as connection:
    countx = 'select count(*) from tablex'
    record_count = connection.execute(countx).scalar()

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': '+str(record_count)+' records loaded')
if df_records == record_count:
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': Scanform data integration completed successfully')
else:
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': etl mismatch - extracted '+str(df_records)+' records and loaded '+str(record_count)+' records')
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': Scanform data integration completed with errors')

