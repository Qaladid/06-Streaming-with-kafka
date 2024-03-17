import json
import time
import pandas as pd
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'
topic_name = 'test-topic'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

connected = producer.bootstrap_connected()
print("Connected to Kafka server:", connected)

# Sending test messages
t0 = time.time()
for i in range(10):
    message = {'number': i}
    producer.send(topic_name, value=message)
    print(f"Sent test message: {message}")
producer.flush()
t1 = time.time()
print(f'Test messages took {(t1 - t0):.2f} seconds')

# Load the green taxi data with error handling
try:
    df_green = pd.read_csv('C:\\Users\\LENOVO\\downloads\\green_tripdata_2019-10.csv\\green_tripdata_2019-10.csv')
except FileNotFoundError:
    print("Error: CSV file not found.")
    exit(1)

# Filter columns
df_green = df_green[['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount']]

# Sending data from CSV to Kafka
t2 = time.time()
for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    producer.send(topic_name, value=row_dict)
    print(f"Sent data row: {row_dict}")
producer.flush()
t3 = time.time()
print(f'Sending data from CSV took {(t3 - t2):.2f} seconds')
