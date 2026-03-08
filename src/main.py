import os

from dotenv import load_dotenv
import duckdb

from final_project.flight_server import FlightServer


if __name__ == "__main__":
    
    
    load_dotenv()

    AWS_KEY_ID = os.getenv("AWS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
    AWS_REGION = os.getenv("AWS_REGION")
    con = duckdb.connect()

    con.execute('''
    INSTALL httpfs;
    LOAD httpfs;
    CREATE SECRET secret (
        TYPE s3,
        KEY_ID ?,
        SECRET ?,
        REGION ?
    );
    ''', (AWS_KEY_ID, AWS_SECRET_KEY, AWS_REGION))

    result = con.execute('''
    SELECT * FROM read_parquet('s3://aws-public-blockchain/v1.0/btc/blocks/date=2023-05-04/part-00000-833f1ffd-f3fb-4221-b2be-1a71c47c95c3-c000.snappy.parquet');
    ''')
    arrow_table = result.fetch_arrow_table()
    df = arrow_table.to_pandas()
    print(df)
    
    
    # result = con.execute('''
    #             SELECT * FROM glob("s3://aws-public-blockchain/v1.0/btc/blocks/date=2023-05-04/*")''')
    
    # print(result.df())
    