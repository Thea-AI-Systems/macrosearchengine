import boto3
import duckdb
import os
#load config.keys.env
from dotenv import load_dotenv
import io
from datetime import datetime
import asyncio

#CHANGE THIS TO YOUR OWN S3 CONFIG
load_dotenv(os.path.join('config', 'keys.env'))

s3_access_key = os.getenv('s3_access_key')
s3_secret_key = os.getenv('s3_secret_key')
s3_region = os.getenv('s3_region', 'ap-southeast-1')
endpoint_url = os.getenv('endpoint_url')

pqdb = duckdb.connect()
pqdb.execute("INSTALL httpfs; LOAD httpfs;")
pqdb.execute(f"SET s3_region='{s3_region}'")
pqdb.execute("SET s3_endpoint='s3.ap-southeast-1.wasabisys.com'")
pqdb.execute("SET s3_access_key_id='" + s3_access_key + "'")
pqdb.execute("SET s3_secret_access_key='" + s3_secret_key + "'")
pqdb.execute("CREATE OR REPLACE TABLE temp_table AS SELECT * FROM read_parquet('s3://macrosearchengine/datasets/Banking/banking_statistics/*.parquet')")
#print table
df = pqdb.execute("SELECT * FROM temp_table").fetchdf()
#print the categories column... 
df.to_csv('temp.csv', index=False)
