import pandas as pd
import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user = params.User
    host = params.Host
    port = params.Port
    password = params.Password
    db = params.DbName
    table = params.TableName
    url = params.Url
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    input_file = "data/Input.parquet"
    os.system(f'wget {url} -O {input_file}')

    file = pq.ParquetFile(input_file)

    t_start = time()
    count = 0
    for batch in file.iter_batches(batch_size=500000):
        count += 1
        batch_df = batch.to_pandas()
        print(f'inserting batch {count}...')
        print(f'Total number of record in batch {batch_df.count()}')
        b_start = time()

        batch_df.to_sql(name=table, con=engine, if_exists='append')
        b_end = time()
        print(f'inserted! time taken {b_end-b_start:10.3f} seconds.\n')

    t_end = time()
    print(
        f'Completed! Total time taken was {t_end-t_start:10.3f} seconds for {count} batches.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Data pipeline')
    # host port password dbname tablename url
    parser.add_argument('--User', help='User for postgres')
    parser.add_argument('--Password', help='Password for postgres')
    parser.add_argument('--Host', help='Host for postgres')
    parser.add_argument('--Port', help='Port no. for postgres')
    parser.add_argument('--DbName', help='DbName for postgres')
    parser.add_argument('--TableName', help='Table name for postgres')
    parser.add_argument('--Url', help='User for postgres')

    args = parser.parse_args()
    main(args)
