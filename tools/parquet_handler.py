import boto3
import duckdb
import os
import io
from datetime import datetime
from tempfile import NamedTemporaryFile
import uuid
from tools.s3ops import s3, s3_access_key, s3_secret_key, s3_region, duckdb_format_endpoint_url

pqdb = duckdb.connect()
pqdb.execute("INSTALL httpfs; LOAD httpfs;")
pqdb.execute(f"SET s3_region='{s3_region}'")
pqdb.execute("SET s3_endpoint='" + duckdb_format_endpoint_url + "'")
pqdb.execute("SET s3_access_key_id='" + s3_access_key + "'")
pqdb.execute("SET s3_secret_access_key='" + s3_secret_key + "'")

bucket = "macrosearchengine"

async def save(df, parquet_loc):            
    parquets = s3.list_objects_v2(Bucket=bucket, Prefix=parquet_loc)
    file_keys = parquets.get('Contents', [])                
        
    df['period_span'] = df['period_span'].replace('None', None)
    df['period_span'] = df['period_span'].astype(str)
    df["inter_country_comparison"] = df["inter_country_comparison"].astype(bool)
    df["as_reported"] = df["as_reported"].astype(bool)

    if len(file_keys) == 0:
        #no existing parquet file, so create a new one
        today = datetime.now().strftime('%Y%m%d')
        filename = f"{parquet_loc}/data.parquet"
        buffer = io.BytesIO()
        #save df        
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3.upload_fileobj(buffer, bucket, filename)            
    else:            
        #metric is also important because constitutents can change
        deduplicate_tuples = df[['dataset', 'ticker', 'metric', 'period_end']].drop_duplicates().values.tolist()
        
        today = datetime.now().strftime('%Y%m%d')
        
        temp_table_name = f"temp_table_{uuid.uuid4().hex}"

        pqdb.execute(f"""
            CREATE OR REPLACE TABLE {temp_table_name} AS 
            SELECT * FROM read_parquet('s3://{bucket}/{parquet_loc}/*.parquet')                
        """)

        #delete dataset_periodend_tuples from temp_table
        for dataset, ticker, metric, period_end in deduplicate_tuples:
            pqdb.execute(f"""
                DELETE FROM {temp_table_name}
                WHERE dataset = '{dataset}' AND ticker = '{ticker}' AND metric = '{metric}' AND period_end = '{period_end}'
            """)            

        '''
        #print columns in temp_table
        columns = pqdb.execute(f"DESCRIBE {temp_table_name}").fetchall()
        columns = [col[0] for col in columns]
        print (f"Columns in temp_table: {columns}")

        #print columns in df
        df_columns = df.columns.tolist()
        print (f"Columns in df: {df_columns}")
        '''

        #input("Press Enter to continue...")
        #save df to local csv
        df.to_csv(f'{parquet_loc.replace("/", "_")}.csv', index=False)
        #create a new table with the same schema as temp_table
        #append df to temp_table
        pqdb.execute(f"INSERT INTO {temp_table_name} BY NAME SELECT * FROM df")

        #save to local parquet file        
        with NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
            temp_file_path = temp_file.name            
        
        pqdb.execute(f"""
            COPY {temp_table_name} TO '{temp_file_path}' 
            (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE TRUE)
        """)
        
        #upload to s3
        with open(temp_file_path, 'rb') as f:
            s3.upload_fileobj(f, bucket, f"{parquet_loc}/data.parquet")

        #delete the local temp file
        os.remove(temp_file_path)

        #Delete the temporary table
        pqdb.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                
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