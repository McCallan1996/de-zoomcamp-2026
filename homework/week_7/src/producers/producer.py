import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer

# Q2: Download NYC green taxi trip data for October 2025
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount',
]
df = pd.read_parquet(url, columns=columns)

# Convert datetime columns to strings for JSON serialization
df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Fill NaN values to avoid JSON parse errors in Flink
df['passenger_count'] = df['passenger_count'].fillna(0)

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

topic_name = 'green-trips'

t0 = time.time()

for _, row in df.iterrows():
    message = row.to_dict()
    producer.send(topic_name, value=message)

producer.flush()

t1 = time.time()
print(f'Sent {len(df)} records')
print(f'took {(t1 - t0):.2f} seconds')
