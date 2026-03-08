import os

import pyarrow as pa
import pyarrow.flight
import json
from dotenv import load_dotenv

if __name__ == '__main__':
    load_dotenv()
    config = {
        'AWS_KEY_ID': os.getenv("AWS_KEY_ID"),
        'AWS_SECRET_KEY': os.getenv("AWS_SECRET_KEY"),
        'AWS_REGION': os.getenv("AWS_REGION")
    }

    client = pa.flight.connect('grpc://localhost:5005')
    
    ticket = {
        'buckets': ['a','b'],
        'query': '''
            SELECT ?
        ''',
        'args': [10]
    }
    # ticket = {
    #     'buckets': ['a','b'],
    #     'query': 'hi',
    #     'args': [10]
    # }
    # client.do_action(pa.flight.Action('init', json.dumps(config).encode('utf-8')))
    result = client.do_get(pa.flight.Ticket(json.dumps(ticket)))
    # client.do_action(pa.flight.Action('give_work', b'a'))    
    print(result.read_all())