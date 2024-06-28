import requests
import time
from kafka import KafkaProducer
from datetime import datetime
import json
import logging
import os

# COINBASE
COINBASE_API_URL=os.getenv('COINBASE_API_URL','https://api.coinbase.com/v2/prices/spot?currency=USD')

# KAFKA
KAFKA_TOPIC=os.getenv('KAFKA_TOPIC','coinbase-topic')
KAFKA_BOOTSTRAP_SERVERS=os.getenv('KAFKA_BOOTSTRAP_SERVERS','kafka:9092')

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                         api_version=(0,11,5),
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def fetch_coinbase_data():
    response = requests.get(COINBASE_API_URL)
    if response.status_code == 200:
        response = response.json()
        return response
    else:
        logger.error(f"Failed to fetch data: {response.text}")
        return None


def produce_data():
    while True:
        data = fetch_coinbase_data()
        if data:
            producer.send(KAFKA_TOPIC, data)
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            logger.info(f"Data sent to Kafka: {data} at : {dt_string}")
        time.sleep(10)  # Fetch data every 10 seconds


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.INFO)
    logger.info("This is the main")
    produce_data()
