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
pqdb.execute("SET s3_endpoint='" + endpoint_url + "'")
pqdb.execute("SET s3_access_key_id='" + s3_access_key + "'")
pqdb.execute("SET s3_secret_access_key='" + s3_secret_key + "'")

dataset = "BankingStatistics"

parquet_loc = f"s3://macrosearchengine/datasets/{dataset}/banking_statistics/*.parquet"

pqdb.execute("CREATE OR REPLACE TABLE databank_test AS SELECT * FROM read_parquet('" + parquet_loc + "')")

query = f"""
SELECT *
FROM databank_test
WHERE period_end = '2025-07-25'
"""

result = pqdb.execute(query).fetchdf()
print(result)


