import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer

server = 'localhost:9092'
topic_name = 'green-trips'

# Q3: Consumer to count trips with trip_distance > 5.0 km
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-hw3',
    consumer_timeout_ms=5000,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Listening to {topic_name}...")

count = 0
long_trips = 0

for message in consumer:
    trip = message.value
    count += 1
    if trip.get('trip_distance', 0) > 5.0:
        long_trips += 1

    if count % 10000 == 0:
        print(f"Processed {count} messages, long trips (>5km): {long_trips}")

consumer.close()
print(f"\nTotal messages: {count}")
print(f"Trips with distance > 5.0 km: {long_trips}")
