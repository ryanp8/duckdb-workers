from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import os
import time
import random
import requests
import base64

import pyarrow as pa
import pyarrow.flight
import json
import boto3
from dotenv import load_dotenv

def simple_query():
    # body = {
    #     'buckets': ['a','b'],
    #     'query': '''loop''',
    #     'args': []
    # }
    
    body = {
        'buckets': ['a','b'],
        'query': '''select ?''',
        'args': [10]
    }
    
    body = {
        'buckets': ['s3://aws-public-blockchain/v1.0/btc/blocks/date=2023-05-04/part-00000-833f1ffd-f3fb-4221-b2be-1a71c47c95c3-c000.snappy.parquet'],
        'query': '''SELECT * FROM
                    read_parquet('s3://aws-public-blockchain/v1.0/btc/blocks/date=2023-05-04/part-00000-833f1ffd-f3fb-4221-b2be-1a71c47c95c3-c000.snappy.parquet');''',
        'args': []
    }
    response = requests.post('http://127.0.0.1:5000/read', json=body)
    if response.ok:
        response_json = response.json()
        result_bytes = base64.b64decode(response_json['result'])
        buffer = pa.BufferReader(result_bytes).read_buffer()
        reader = pa.ipc.open_stream(buffer)
        print(reader.read_all())
    else:
        print(f'Error: {response.status_code} - {response.text}')
        
def clear_cache(worker_location=None):
    body = {}
    if worker_location:
        body['worker_location'] = worker_location
    response = requests.post('http://127.0.0.1:5000/clear_cache', json=body)
    if response.ok:
        print('Cache cleared successfully')
    else:
        print(f'Error: {response.status_code} - {response.text}')

def query(body):
    response = requests.post('http://127.0.0.1:5000/read', json=body)
    if not response.ok:
        print(f'Error: {response.status_code} - {response.text}')
        
if __name__ == '__main__':
    load_dotenv()

    AWS_KEY_ID = os.getenv("AWS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
    AWS_REGION = os.getenv("AWS_REGION")
    
    # s3_client = boto3.client('s3', aws_access_key_id=AWS_KEY_ID, aws_secret_access_key=AWS_SECRET_KEY, region_name=AWS_REGION)
    # files = []
    # total_size = 0
    
    # start_date = date(2025, 1, 1)
    # end_date = date(2026, 1, 1)

    # current = start_date
    # while current <= end_date:
    #     response = s3_client.list_objects_v2(Bucket='aws-public-blockchain', Prefix=f'v1.0/btc/blocks/date={current.strftime("%Y-%m-%d")}/')
    #     for obj in response.get('Contents', []):
    #         files.append(obj['Key'])
    #         total_size += obj['Size']
    #     current += timedelta(days=1)
    
    # print(files)
    # print(f'Total size: {total_size / (1024**3):.2f} GB')

    files = ['v1.0/btc/blocks/date=2026-01-01/part-00000-7678982d-5d67-4f87-817a-4bfaec1e3102-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-02/part-00000-a38fde5c-6e6c-48a0-835b-caabdb2990a9-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-03/part-00000-b4b2aa6d-facf-424e-8339-dd0d19ef7610-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-04/part-00000-38a65a28-d22c-486f-8bfc-f9e5392cb0c3-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-05/part-00000-ba406213-af79-4d22-9cfa-2c38af8935ab-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-06/part-00000-11e16c8d-f54c-42f4-9499-576c4b5cf49e-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-07/part-00000-2d1d0168-2c61-4a45-a113-db89bd745195-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-08/part-00000-17286b91-00f1-44ec-ba5e-faf67fcaef73-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-09/part-00000-a34820b1-9903-4b20-94a7-2b601c7c1f86-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-10/part-00000-cbb177ab-5fff-4329-81fa-f66b299f1ee1-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-11/part-00000-e16c603e-fed9-4a05-b657-aa4c90b02e75-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-12/part-00000-ee396475-478d-4a26-a0eb-ee46a18ff53d-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-13/part-00000-cc09e663-044b-4888-975a-8e54d4e8979e-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-14/part-00000-687de38e-73a4-48bb-b87e-36291ab71d00-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-15/part-00000-da585be5-7a3c-4d48-ad0b-49eb66598aad-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-16/part-00000-73b6f38f-03db-4e2a-9c0a-e585fed79742-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-17/part-00000-bedae8a0-6331-4097-a6e9-82ce7e34626a-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-18/part-00000-a7dd0343-1283-4bcc-a521-59857a3dd2ba-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-19/part-00000-39020c96-6a80-4668-9adb-5066ec8f8601-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-20/part-00000-0401827b-0e0c-4c81-9ff8-bf97f1404956-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-21/part-00000-ee4f27f6-41a8-4502-93d6-52a23361457f-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-22/part-00000-e2ccb13e-4209-4a83-99e3-8b537fd9fcc5-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-23/part-00000-29e7f027-cfa0-4da5-a40d-c921507fddd4-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-24/part-00000-51ccd3e7-a60c-4df7-80b2-a1161d41d582-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-25/part-00000-9cedb3fc-1baf-48c1-917f-b27a2d3ec4bd-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-26/part-00000-b6c4a156-e761-4437-8d3f-2dfceca4a277-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-27/part-00000-0c925f3a-e835-4f0e-8b3d-311bfd003fd1-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-28/part-00000-01893501-75eb-4f2d-91d1-45a5ea634273-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-29/part-00000-a9b161f8-9a2f-4f8a-9965-fe57f3676908-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-30/part-00000-95e8c635-ceb1-4be0-8c05-503eff98b1df-c000.snappy.parquet', 'v1.0/btc/blocks/date=2026-01-31/part-00000-3842c812-5471-4bf8-ba9b-f5a058b37a25-c000.snappy.parquet'] * 3
    
    start = time.time()
    futures = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        for file in files:
            body = {
                'buckets': [f's3://aws-public-blockchain/{file}'],
                'query': '''SELECT * FROM read_parquet(?);''',
                'args': [f's3://aws-public-blockchain/{file}']
            }
            future = executor.submit(query, body)
            futures.append(future)
            
    for future in futures:
        future.result()
        
    end = time.time()
    print(f'Total time: {end - start:.2f} seconds')
    print('done')
    
