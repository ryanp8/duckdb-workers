from concurrent.futures import ThreadPoolExecutor
from argparse import ArgumentParser
from dotenv import load_dotenv
from datetime import timedelta, datetime
import time
import os
import boto3

from src.client import Client

def get_files(start_date, end_date, output_path):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )
    files = []
    total_size = 0

    current = start_date
    while current <= end_date:
        response = s3_client.list_objects_v2(
            Bucket="aws-public-blockchain",
            Prefix=f"v1.0/btc/blocks/date={current.strftime('%Y-%m-%d')}/",
        )
        for obj in response.get("Contents", []):
            files.append(obj["Key"])
            total_size += obj["Size"]
        current += timedelta(days=1)

    with open(output_path, "w+") as f:
        for filename in files:
            f.writelines(filename + "\n")

    print(f"Total size: {total_size / (1024**3):.2f} GB")


if __name__ == "__main__":
    # Setup
    load_dotenv()

    AWS_KEY_ID = os.getenv("AWS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
    AWS_REGION = os.getenv("AWS_REGION")

    argparser = ArgumentParser()
    argparser.add_argument(
        "-dp", "--datapath", type=str, default="tests/data/crypto_files.txt"
    )
    argparser.add_argument(
        "-s", "--start", type=lambda s: datetime.strptime(s, "%Y-%m-%d")
    )
    argparser.add_argument(
        "-e", "--end", type=lambda s: datetime.strptime(s, "%Y-%m-%d")
    )
    argparser.add_argument(
        "-c", "--coordinator", type=str
    )
    args = argparser.parse_args()

    if not (os.path.exists(args.datapath) and os.stat(args.datapath).st_size):
        get_files(args.start, args.end, args.datapath)

    files = []
    with open(args.datapath, "r") as f:
        for line in f.readlines():
            files.append(line)

    # Pad files to test under load
    files *= 3

    client = Client(args.coordinator)
    start = time.time()
    futures = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        for file in files:
            file = file.strip()
            body = {
                "buckets": [f"s3://aws-public-blockchain/{file}"],
                "query": """SELECT * FROM read_parquet(?);""",
                "args": [f"s3://aws-public-blockchain/{file}"],
            }
            future = executor.submit(client.read, body)
            futures.append(future)

    for future in futures:
        future.result()

    end = time.time()
    print(f"Total time: {end - start:.2f} seconds")
    print("done")
