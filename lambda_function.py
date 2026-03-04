import pandas as pd
import boto3
import os
from io import BytesIO

s3 = boto3.client('s3')

def silver_handler(event, context):
    try:
        record = event['Records'][0]
        bronze_bucket = record['s3']['bucket']['name']
        file_key = record['s3']['object']['key']
        silver_bucket = os.environ.get('SILVER_BUCKET', 'shadow-trader-silver-robin-2026')
        
        obj = s3.get_object(Bucket=bronze_bucket, Key=file_key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
        
        # Explicit mapping based on your 'X-Ray' output
        mapping = {
            '4. close': 'price',
            '5. volume': 'volume'
        }
        
        # Check if mapping keys exist before renaming
        df = df.rename(columns=mapping)
        
        if 'price' not in df.columns:
            return {"statusCode": 500, "error": f"No price column found. Available: {df.columns.tolist()}"}

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['price'] = pd.to_numeric(df['price'])
        
        silver_key = file_key.replace("raw/", "cleaned/")
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        s3.put_object(Bucket=silver_bucket, Key=silver_key, Body=buffer.getvalue())
        
        return {"statusCode": 200, "body": f"Silver saved: {silver_key}"}
    except Exception as e:
        return {"statusCode": 500, "error": str(e)}
