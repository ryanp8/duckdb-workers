# DuckDB Workers

This project introduces a experimental prototype to leverage DuckDB in-process behavior to build a distributed layer over it to facilitate using DuckDB as workers in a scalable data processing platform. A shared S3 object store is used as the storage layer to separate compute and storage logic, so focus can be put on scaling and implementing workers. Many other systems have scaled out embedded storage systems to provide fault tolerance and greater availability. For example, Dynamo was designed as a distributed hash table offering replication and high availability with each node running a local key-value store. This project aims to experiment with using a similar approach, using DuckDB and the `cache-httpfs` extension to improve availability.


## Architecture
<img width="297" height="259" alt="image" src="https://github.com/user-attachments/assets/5af57f47-3880-4187-a094-c8a582459d81" />

**Coordinator**: Entry point to the system. Uses rendezvous hashing to route requests to the appropriate worker. Workers cache previous results using the `cache-httpfs` DuckDB extension.

**Workers**: Actually perform the query specified in the request. Steals work from other workers if idle. The cache is deactivated for stolen requests.

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
table = reader.read_all()

df = table.to_pandas()
print(df)
```

Output (as pandas dataframe):
```
                                                  hash    version          mediantime       nonce  ... stripped_size           timestamp        date              last_modified
0    00000000000000000001f82196c8c357b84b75087c9b89...  587268096 2025-05-01 02:47:27   414521365  ...        677717 2025-05-01 03:31:20  2025-05-01 2026-02-12 02:02:36.800360
1    00000000000000000001e84dba1f8c005b0eca045632af...  570425344 2025-05-01 03:13:05  2872579434  ...        779205 2025-05-01 04:01:52  2025-05-01 2026-02-12 02:03:23.233488
2    00000000000000000001bcfdeb4a28dcdbb416c1c15cd2...  667680768 2025-05-01 20:52:23  2413021058  ...        794109 2025-05-01 21:46:58  2025-05-01 2026-02-12 02:01:02.471755
3    0000000000000000000109698361df2f0af20b19444342...  560406528 2025-05-01 22:55:58   403769182  ...        830181 2025-05-01 23:35:43  2025-05-01 2026-02-12 02:03:03.888925
4    000000000000000000019ca67e84028ceecf3a520e5724...  537681920 2025-05-01 06:02:14  1501445461  ...        247240 2025-05-01 06:34:00  2025-05-01 2026-02-12 02:02:44.267845
..                                                 ...        ...                 ...         ...  ...           ...                 ...         ...                        ...
130  0000000000000000000116a8fc05dee0f4596e8a52dbd4...  544538624 2025-05-01 02:29:23   389586506  ...        782250 2025-05-01 03:13:05  2025-05-01 2026-02-12 01:47:09.884046
131  0000000000000000000138b45f14826b3cf9958f58b40c...  625360896 2025-05-01 16:42:38  3402543620  ...        778326 2025-05-01 17:56:29  2025-05-01 2026-02-12 02:08:36.936514
132  000000000000000000014e936ba1ad84f3fb9f48eaab39...  536928256 2025-05-01 10:40:47   833227684  ...        789529 2025-05-01 11:48:35  2025-05-01 2026-02-12 02:02:04.850272
133  00000000000000000000d358c1fd8eec8ba0edaa23920c...  595255296 2025-05-01 04:13:29  3896549421  ...        890274 2025-05-01 05:02:35  2025-05-01 2026-02-12 02:08:27.278707
134  0000000000000000000247ce34ee564bde25650fec1c8c...  552853504 2025-05-01 06:48:40  3122640133  ...        659041 2025-05-01 07:48:28  2025-05-01 2026-02-12 02:03:12.396691
```

Because Apache Arrow Flight is used to communicate, the results of a request can be easily converted into a dataframe for further processing.

## Preliminary Evaluation
Running on t3.micro AWS EC2 instances using the AWS Public
Blockchain Dataset on 30 parquet files. Each file is read 3 times
to see the effects of caching.

| # Workers | Avg Time with Caching | Avg Time Without Caching |
|-|-|-|
|1|8.18|13.97|
|2|4.78|10.32|
|4|3.82|6.68|

