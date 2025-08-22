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
import uuid
# use compression
import gzip

#CHANGE THIS TO YOUR OWN S3 CONFIG
load_dotenv(os.path.join('config', 'keys.env'))

s3_access_key = os.getenv('s3_access_key')
s3_secret_key = os.getenv('s3_secret_key')
s3_region = os.getenv('s3_region', 'ap-southeast-1')
endpoint_url = os.getenv('endpoint_url')
duckdb_format_endpoint_url = os.getenv('duckdb_format_endpoint_url')

bucket = "macrosearchengine"

s3 = boto3.client(
        's3',
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        endpoint_url=endpoint_url
    )

# There are various metadata and weights files that need to be accessed - These are stored in extras folcer
def save_extras(key, file_contents):
    key = f'extras/{key}'
    #compress the file
    with NamedTemporaryFile(mode='wb', delete=False) as f:
        # Dump JSON bytes and compress using gzip
        with gzip.GzipFile(fileobj=f, mode='wb') as gz:
            gz.write(file_contents.encode('utf-8'))
        temp_file_path = f.name

    # Upload the compressed file to S3
    s3.upload_file(temp_file_path, bucket, key)
    # Remove the temporary file
    os.remove(temp_file_path)

def load_extras(key):
    key = f'extras/{key}'
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        compressed_data = response['Body'].read()
        # Decompress the data using gzip
        with gzip.GzipFile(fileobj=io.BytesIO(compressed_data), mode='rb') as gz:
            data = gz.read().decode('utf-8')
        return data
    except s3.exceptions.NoSuchKey:
        return None
    

