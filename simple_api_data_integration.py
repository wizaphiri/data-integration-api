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
df_records = df.shape[0]
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': '+str(df_records)+' records extracted')
# export df to csv
# print(df.info())

engine = create_engine('postgresql+psycopg2://userx:passx@localhost/dbx')
with engine.begin() as conn:
    conn.exec_driver_sql("DROP TABLE IF EXISTS art_scanform")
    #print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': table art_scanform dropped')

df.to_sql('art_scanform', engine, if_exists='append')

# table = 'art_scanform'

with engine.connect() as connection:
    count_statement = 'select count(*) from art_scanform'
    record_count = connection.execute(count_statement).scalar()

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': '+str(record_count)+' records loaded')
if df_records == record_count:
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': Scanform data integration completed successfully')
else:
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': etl mismatch - extracted '+str(df_records)+' records and loaded '+str(record_count)+' records')
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+': Scanform data integration completed with errors')

