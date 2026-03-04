import pandas as pd
import boto3
from io import BytesIO

# Fetching from Silver
s3 = boto3.client('s3')
bucket = 'shadow-trader-silver-robin-2026'
# We'll grab the latest file we just fixed
key = 'cleaned/crypto_prices/BTC_20260304_022832.parquet' 

obj = s3.get_object(Bucket=bucket, Key=key)
df = pd.read_parquet(BytesIO(obj['Body'].read()))

# Feature Engineering
df['date'] = pd.to_datetime(df['timestamp'])
df = df.sort_values('date')
df['sma_7'] = df['price'].rolling(window=7).mean()

# PERSISTENCE: Save the Gold data locally for the Signal Engine
df.to_parquet('gold_features.parquet', index=False)
print(f"✅ Success! Gold features saved to gold_features.parquet ({len(df)} rows)")
