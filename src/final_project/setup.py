import os
import json
import pyarrow as pa
import pyarrow.flight
from dotenv import load_dotenv


hosts = [

]


clients = {}
for host in hosts:
    conn = pa.flight.connect(host)
    clients[host] = conn
    
    for other in hosts:
        if host != other:
            clients[host].do_action(pa.flight.Action('add_peer', other.encode('utf-8')))

