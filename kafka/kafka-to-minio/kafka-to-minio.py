from kafka import KafkaConsumer
import time
import json
import logging
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from minio import Minio
from minio.error import S3Error

# KAFKA
KAFKA_TOPIC=os.getenv('KAFKA_TOPIC','coinbase-topic')
KAFKA_BOOTSTRAP_SERVERS=os.getenv('KAFKA_BOOTSTRAP_SERVERS','kafka:9092')
# MINIO
MINIO_ENDPOINT=os.getenv('MINIO_ENDPOINT','minio:9000')
MINIO_ACCESS_KEY =os.getenv('MINIO_ACCESS_KEY','minioadmin')
MINIO_SECRET_KEY=os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET=os.getenv('MINIO_BUCKET','coinbase-data')

consumer = KafkaConsumer(KAFKA_TOPIC,
                        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

minio_client= Minio(MINIO_ENDPOINT,
                    access_key=MINIO_ACCESS_KEY,
                    secret_key=MINIO_SECRET_KEY,
                    secure=False)

if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)

def upload_to_minio(df,filename):
    try:
        with BytesIO() as buffer:
            table=pa.Table.from_pandas(df)
            pq.write_table(table, buffer)
            buffer.seek(0)
            minio_client.put_object(
                bucket_name=MINIO_BUCKET,
                object_name=filename,
                data=buffer,
                length=len(buffer.getvalue())
            )
            logger.info(f"Uploaded {filename} to MinIO")
    except S3Error as exc:
        logger.error(f"Fialed to upload to MinIO: {exc}")


def consume_data():
    for message in consumer:
        data = message.value
        if data:
            logger.info("Recive data")
            df = pd.DataFrame([data])
            timestamp = pd.to_datetime('now').strftime('%Y-%m-%d_%H-%M-%S')
            filename=f"coinbase_{timestamp}.parquet"
            upload_to_minio(df,filename)



if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.INFO)
    logger.info("This is the main")
    consume_data()
