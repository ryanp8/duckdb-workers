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