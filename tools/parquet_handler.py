import boto3
import duckdb
import os
#load config.keys.env
from dotenv import load_dotenv
import io
from datetime import datetime
from time import time
from tempfile import NamedTemporaryFile
import asyncio

#CHANGE THIS TO YOUR OWN S3 CONFIG
load_dotenv(os.path.join('config', 'keys.env'))

s3_access_key = os.getenv('s3_access_key')
s3_secret_key = os.getenv('s3_secret_key')
s3_region = os.getenv('s3_region', 'ap-southeast-1')
endpoint_url = os.getenv('endpoint_url')
duckdb_format_endpoint_url = os.getenv('duckdb_format_endpoint_url')

pqdb = duckdb.connect()
pqdb.execute("INSTALL httpfs; LOAD httpfs;")
pqdb.execute(f"SET s3_region='{s3_region}'")
pqdb.execute("SET s3_endpoint='" + duckdb_format_endpoint_url + "'")
pqdb.execute("SET s3_access_key_id='" + s3_access_key + "'")
pqdb.execute("SET s3_secret_access_key='" + s3_secret_key + "'")

s3 = boto3.client(
        's3',
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        endpoint_url=endpoint_url
    )

bucket = "macrosearchengine"

async def save(df, parquet_loc):        
    parquets = s3.list_objects_v2(Bucket=bucket, Prefix=parquet_loc)
    file_keys = parquets.get('Contents', [])                
    
    if len(file_keys) == 0:
        #no existing parquet file, so create a new one
        today = datetime.now().strftime('%Y%m%d')
        filename = f"{parquet_loc}/data.parquet"
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3.upload_fileobj(buffer, bucket, filename)            
    else:            
        dataset_periodend_tuples = df[['dataset', 'period_end']].drop_duplicates().values.tolist()        
        
        today = datetime.now().strftime('%Y%m%d')
        pqdb.execute(f"""
            CREATE OR REPLACE TABLE temp_table AS 
            SELECT * FROM read_parquet('s3://{bucket}/{parquet_loc}/*.parquet')                
        """)

        #delete dataset_periodend_tuples from temp_table
        for dataset, period_end in dataset_periodend_tuples:
            pqdb.execute(f"""
                DELETE FROM temp_table 
                WHERE dataset = '{dataset}' AND period_end = '{period_end}'
            """)

        #print columsn in temp_table
        columns = pqdb.execute("DESCRIBE temp_table").fetchall()
        columns = [col[0] for col in columns]
        print (f"Columns in temp_table: {columns}")

        #print columns in df
        df_columns = df.columns.tolist()
        print (f"Columns in df: {df_columns}")

        #input("Press Enter to continue...")
        #save df to local csv
        #df.to_csv('temp_table.csv', index=False)
        #create a new table with the same schema as temp_table
        #append df to temp_table
        pqdb.execute("INSERT INTO temp_table BY NAME SELECT * FROM df")

        save_time_start = time()        

        #save to local parquet file        
        with NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
            temp_file_path = temp_file.name            
        
        pqdb.execute(f"""
            COPY temp_table TO '{temp_file_path}' 
            (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE TRUE)
        """)        
        
        #upload to s3
        with open(temp_file_path, 'rb') as f:
            s3.upload_fileobj(f, bucket, f"{parquet_loc}/data.parquet")

        #delete the local temp file
        os.remove(temp_file_path)
                
#prefix - 'datasets/Inflation/'
async def get_presigned_url(prefix, expiration=86400):
    # get presigned urls for all files hosted in this prefix
    paginator = s3.get_paginator('list_objects_v2')
    urls = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.parquet'):
                url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket, 'Key': key},
                    ExpiresIn=expiration
                )
                urls.append(url)
    return urls

async def get_all_parquets():
    # get all parquet files in the given prefix
    paginator = s3.get_paginator('list_objects_v2')
    parquets = []
    for page in paginator.paginate(Bucket=bucket, Prefix="datasets/"):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.parquet'):
                parquets.append(key)
    return parquets