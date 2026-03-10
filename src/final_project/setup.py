import os
import json
import pyarrow as pa
import pyarrow.flight
from dotenv import load_dotenv


# load_dotenv()
# config = {
#     'AWS_KEY_ID': os.getenv("AWS_KEY_ID"),
#     'AWS_SECRET_KEY': os.getenv("AWS_SECRET_KEY"),
#     'AWS_REGION': os.getenv("AWS_REGION")
# }

hosts = ['grpc://localhost:5005', 'grpc://localhost:5006']
# hosts = ['grpc://localhost:5005']

clients = {}
for host in hosts:
    conn = pa.flight.connect(host)
    # conn.do_action(pa.flight.Action('init', json.dumps(config).encode('utf-8')))
    clients[host] = conn
    
    for other in hosts:
        if host != other:
            clients[host].do_action(pa.flight.Action('add_peer', other.encode('utf-8')))

