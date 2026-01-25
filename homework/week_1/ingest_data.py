import pandas as pd
from sqlalchemy import create_engine


# user: postgres, password: postgres, host: postgres, port: 5432, db: ny_taxi
db_url = 'postgresql://postgres:postgres@postgres:5432/ny_taxi'
engine = create_engine(db_url)

def upload_to_postgres(file_path, table_name, file_type='parquet'):
    print(f"Loarding from {file_path} to {table_name}...")

    if file_type == 'parquet':
        df = pd.read_parquet(file_path)
    else:
        # CSV
        df = pd.read_csv(file_path)

    # To Database
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    print(f"Loarded Qty: {len(df)}")

if __name__ == '__main__':
    # 1. Green Taxi
    upload_to_postgres(
        file_path='https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet', 
        table_name='green_taxi_trips', 
        file_type='parquet'
    )

    # 2. Zones
    upload_to_postgres(
        file_path='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv', 
        table_name='zones', 
        file_type='csv'
    )
