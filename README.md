# DuckDB Workers

This project introduces a experimental prototype to leverage DuckDB in-process behavior to build a distributed layer over it to facilitate using DuckDB as workers in a scalable data processing platform. A shared S3 object store is used as the storage layer to separate compute and storage logic, so focus can be put on scaling and implementing workers. Many other systems have scaled out embedded storage systems to provide fault tolerance and greater availability. For example, Dynamo was designed as a distributed hash table offering replication and high availability with each node running a local key-value store. This project aims to experiment with using a similar approach, using DuckDB and the `cache-httpfs` extension to improve availability.


## Architecture
<img width="297" height="259" alt="image" src="https://github.com/user-attachments/assets/5af57f47-3880-4187-a094-c8a582459d81" />


**Coordinator**: Entry point to the system. Uses rendezvous hashing to route requests to the appropriate worker.

**Workers**: Actually perform the query specified in the request. Steals work from other workers if idle.

## Running
Start the worker(s)
```
uv run src/worker.py --port=5005
```

Start the coordinator
```
uv run src/coordinator.py --workers grpc://localhost:5005
```

Use the client to submit requests
```python
from src.client import Client

file = 'v1.0/btc/blocks/date=2025-05-01/part-00000-4079d904-b105-4c8e-b885-4eface0507a1-c000.snappy.parquet'
body = {
    "buckets": [f"s3://aws-public-blockchain/{file}"],
    "query": """SELECT * FROM read_parquet(?);""",
    "args": [f"s3://aws-public-blockchain/{file}"],
}

client = Client('http://127.0.0.1:5000')
reader = client.read(body)
print(reader.read_all())
```

## Preliminary Evaluation
Running on t3.micro AWS EC2 instances using the AWS Public
Blockchain Dataset on 30 parquet files. Each file is read 3 times
to see the effects of caching.

| # Workers | Avg Time with Caching | Avg Time Without Caching |
|-|-|-|
|1|8.18|13.97|
|2|4.78|10.32|
|4|3.82|6.68|

