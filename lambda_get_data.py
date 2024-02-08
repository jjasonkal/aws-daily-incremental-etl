import urllib3
import json
import boto3
from datetime import datetime
import os

http = urllib3.PoolManager()

s3 = boto3.client('s3')


def lambda_handler(event, context):
    try:
        bucket_name = os.environ.get('S3_BUCKET_NAME')

        if not bucket_name:
            raise ValueError("S3_BUCKET_NAME environment variable is not set")

        r = http.request('GET', 'https://api.open-meteo.com/v1/forecast?latitude=40.64&longitude=22.94&hourly=temperature_2m&timezone=auto&forecast_days=1')
        data = json.loads(r.data)
        json_data = json.dumps(data)

        today_date = datetime.now().strftime('%Y-%m-%d')
        object_key = f'meteo-{today_date}.json'

        # Upload the JSON data to S3
        s3.put_object(Body=json_data, Bucket=bucket_name, Key=object_key)

        return {
            'statusCode': r.status,
            'body': f'Object {object_key} created in S3:{bucket_name}'
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': 'Error handling request'
        }
