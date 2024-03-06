import requests
import time
from kafka import KafkaProducer
from datetime import datetime
import os
import json

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'coinbase-topic')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                         api_version=(0,11,5),
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def fetch_coinbase_data():
    response = requests.get("https://api.coinbase.com/v2/prices/spot")
    if response.status_code == 200:
        response = response.json()
        return response
    else:
        print(f"Failed to fetch data: {response.text}")
        return None


def produce_data():
    while True:
        data = fetch_coinbase_data()
        if data:
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            print(f"Data sent to Kafka: {data} at : {dt_string}")
            producer.send(KAFKA_TOPIC, data)
        time.sleep(20)  # Fetch data every 10 seconds


if __name__ == "__main__":
    print("This is the main")
    produce_data()
