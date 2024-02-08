import json
import csv
import boto3
from io import StringIO
from datetime import datetime
import re
import os

s3 = boto3.client('s3')


def invoke_glue_crawler(crawler_name):
    glue_client = boto3.client('glue')
    response = glue_client.start_crawler(Name=crawler_name)
    return response


def create_folders(bucket, folders):
    s3 = boto3.client('s3')
    for folder in folders:
        s3.put_object(Bucket=bucket, Key=f'{folder}/', Body='')


def lambda_handler(event, context):
    try:
        print("Lambda function started.")

        # Retrieve information about the uploaded file
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        # Extract date from the file name using RegEx
        match = re.search(r'meteo-(\d{4}-\d{2}-\d{2})\.json', object_key)
        if not match:
            raise ValueError("Invalid file name format")

        date_str = match.group(1)
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')

        print(f"Processing file: {object_key}")
        print(f"Date extracted from filename: {date_obj}")

        target_bucket_name = os.environ['TARGET_S3_BUCKET_NAME']

        # Create necessary partitions in the target S3 bucket
        folders_to_create = [
            f'year={date_obj.year}',
            f'year={date_obj.year}/month={date_obj.strftime("%b")}',
            f'year={date_obj.year}/month={date_obj.strftime("%b")}/day={date_obj.day}'
        ]

        create_folders(target_bucket_name, folders_to_create)

        print(f"Folders created in bucket: {target_bucket_name}")

        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        json_data = response['Body'].read().decode('utf-8')

        data = json.loads(json_data)
        csv_data = StringIO()
        csv_writer = csv.writer(csv_data)
        csv_writer.writerow(['datetime', 'temperature'])

        for i in range(len(data['hourly']['time'])):
            temperature = data['hourly']['temperature_2m'][i]
            datetime_str = data['hourly']['time'][i]
            datetime_iso = datetime.fromisoformat(datetime_str).strftime('%Y-%m-%d %H:%M:%S')
            csv_writer.writerow([datetime_iso, temperature])

        print("CSV data transformed")

        csv_object_key = f'year={date_obj.year}/month={date_obj.strftime("%b")}/day={date_obj.day}/{object_key.replace("meteo-", "").replace(".json", ".csv")}'
        s3.put_object(Body=csv_data.getvalue(), Bucket=target_bucket_name, Key=csv_object_key)

        print(f"CSV file uploaded to S3: s3://{target_bucket_name}/{csv_object_key}")

        # Invoke Glue Crawler
        crawler_name = os.environ['CRAWLER_NAME']
        response = invoke_glue_crawler(crawler_name)

        print(f"Glue Crawler Response: {json.dumps(response)}")
        print("Lambda function completed.")

        return {
            'statusCode': 200,
            'body': 'CSV conversion and upload completed'
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': 'Error handling request'
        }
