import os
import grpc
import requests
import base64

import pyarrow as pa
import pyarrow.flight
import json

if __name__ == '__main__':

    # client = pa.flight.connect('grpc://localhost:5005')
    
    # ticket = {
    #     'buckets': ['a','b'],
    #     'query': '''
    #         SELECT ?
    #     ''',
    #     'args': [10]
    # }
    # # ticket = {
    # #     'buckets': ['a','b'],
    # #     'query': 'loop',
    # #     'args': [10]
    # # }
    # # client.do_action(pa.flight.Action('init', json.dumps(config).encode('utf-8')))
    # result = client.do_get(pa.flight.Ticket(json.dumps(ticket)))
    # reader = result.to_reader()
    # sink = pa.BufferOutputStream()
    # with pa.ipc.new_stream(sink, reader.schema) as writer:
    #     for batch in reader:
    #         writer.write_batch(batch)

    # print(reader)
    # buf = sink.getvalue()
    # pybytes = buf.to_pybytes()
    # decoded = pybytes.decode('base64')

    # buffer_reader = pa.BufferReader(pybytes)  
    # buffer = buffer_reader.read_buffer()
    # reader = pa.ipc.open_stream(buffer)
    # print(reader.read_all())
    # # client.do_action(pa.flight.Action('give_work', b'a'))    
    # print(result.read_all())
    
   # SELECT * FROM read_parquet('s3://aws-public-blockchain/v1.0/btc/blocks/date=2023-05-04/part-00000-833f1ffd-f3fb-4221-b2be-1a71c47c95c3-c000.snappy.parquet')
    
    # body = {
    #     'buckets': ['a','b'],
    #     'query': '''
    #         SELECT * FROM read_parquet('s3://aws-public-blockchain/v1.0/btc/blocks/date=2023-05-04/part-00000-833f1ffd-f3fb-4221-b2be-1a71c47c95c3-c000.snappy.parquet')
    #     ''',
    #     'args': []
    # }
    body = {
        'buckets': ['a','b'],
        'query': '''''',
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
    
    
    
    # body = {
    #     'buckets': ['a','b'],
    #     'query': '''loop''',
    #     'args': []
    # }
    # response = requests.post('http://127.0.0.1:5000/write', json=body)
    # if response.ok:
    #     response_json = response.json()
    #     result_bytes = base64.b64decode(response_json['result'])
    #     buffer = pa.BufferReader(result_bytes).read_buffer()
    #     reader = pa.ipc.open_stream(buffer)
    #     print(reader.read_all())