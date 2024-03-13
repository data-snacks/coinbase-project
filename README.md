Kafka Data Pipeline Project
===========================

This project sets up a data pipeline to ingest cryptocurrency data from an API, processes it through Kafka, and print values in console. Below you'll find instructions on how to configure your environment to run the services.

Configuration
-------------

Before running the application, you need to set up environment variables that the application will use. Create a `.env` file in the root directory of your project and include the following configurations:

```bash
# COINBASE
COINBASE_API_URL = https://api.coinbase.com/v2/prices/spot?currency=USD

# KAFKA
KAFKA_TOPIC = coinbase-topic
KAFKA_BOOTSTRAP_SERVERS = kafka:9092
KAFKA_BROKER_ID = 1
KAFKA_ZOOKEEPER_CONNECT = zookeeper:2181
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP = PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_ADVERTISED_LISTENERS = PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR = 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR = 1

# ZOOKEEPER
ZOOKEEPER_CLIENT_PORT = 2181
ZOOKEEPER_TICK_TIME = 2000

# MINIO
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
MINIO_BUCKET = 'coinbase-data'
MINIO_ROOT_USER= minioadmin
MINIO_ROOT_PASSWORD= minioadmin
```

Please ensure you replace the placeholder values with your actual configurations where necessary.

Running the Application
-----------------------

After setting up the `.env` file, follow the steps below to start your services:

1.  **Build the Docker Images** (if applicable): If you are running your services in Docker containers, make sure to build your images with the Dockerfiles provided in the respective service directories.
    
2.  **Start the Services**: Use Docker Compose or your preferred orchestration tool to start the services defined in your `docker-compose.yml` file. Ensure the `.env` file is correctly referenced in your Docker Compose configuration for environment variables.
    
3.  **Verify Operations**: Once all services are up and running, you can verify their operations by checking logs and ensuring there are no errors during startup and runtime.
    

Additional Information
----------------------

*   **Kafka**: Used for streaming and processing cryptocurrency data in real-time.
*   **Zookeeper**: Required for managing the Kafka cluster.

For further details on configurations and operations, refer to the official documentation of each component.