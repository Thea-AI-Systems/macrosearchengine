import boto3
import os
#load config.keys.env
from dotenv import load_dotenv
import io

#CHANGE THIS TO YOUR OWN S3 CONFIG
load_dotenv(os.path.join('config', 'keys.env'))


s3_access_key = os.getenv('s3_access_key')
s3_secret_key = os.getenv('s3_secret_key')
endpoint_url = os.getenv('endpoint_url')


s3 = boto3.client(
        's3',
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        endpoint_url=endpoint_url
    )

bucket = "macrosearchengine"

def save(df, prefix):
    """
    Save a DataFrame to S3 as a Parquet file.
    """
    from_dt = df['period_end'].min().strftime('%Y%m%d')
    to_dt = df['period_end'].max().strftime('%Y%m%d')
    filename = f"{prefix}/{from_dt}_{to_dt}.parquet"
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.upload_fileobj(buffer, bucket, filename)
    
        
    
#prefix - 'datasets/Inflation/'
def get_presigned_url(prefix, expiration=86400):
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