#!/usr/bin/env python3
import json
import time
import logging
from kafka import KafkaConsumer
from hdfs import InsecureClient
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_TOPICS = ['dbserver1.inventory.customers', 'dbserver1.inventory.orders', 'dbserver1.inventory.order_items']
GROUP_ID = 'hdfs-sink-group'

# HDFS settings
HDFS_URL = 'http://namenode:9870'
HDFS_USER = 'root'
OUTPUT_FILE = '/data/cdc/output.json'

def connect_to_kafka():
    """Connect to Kafka and return a consumer."""
    retries = 5
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                *KAFKA_TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info(f"Connected to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
            return consumer
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            retries -= 1
            if retries > 0:
                logger.info(f"Retrying in 5 seconds... ({retries} attempts left)")
                time.sleep(5)
            else:
                logger.error("Maximum retries reached. Exiting.")
                raise
    return None

def connect_to_hdfs():
    """Connect to HDFS and return a client."""
    retries = 5
    while retries > 0:
        try:
            client = InsecureClient(HDFS_URL, user=HDFS_USER)
            logger.info(f"Connected to HDFS: {HDFS_URL}")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to HDFS: {e}")
            retries -= 1
            if retries > 0:
                logger.info(f"Retrying in 5 seconds... ({retries} attempts left)")
                time.sleep(5)
            else:
                logger.error("Maximum retries reached. Exiting.")
                raise
    return None

def append_to_hdfs_file(hdfs_client, file_path, data):
    """Append data to a file in HDFS."""
    # Check if file exists, create if it doesn't
    try:
        if not hdfs_client.status(file_path, strict=False):
            logger.info(f"File {file_path} doesn't exist. Creating new file.")
            with hdfs_client.write(file_path, encoding='utf-8') as writer:
                writer.write('[\n')
            logger.info(f"Created new file: {file_path}")
    except Exception as e:
        logger.info(f"Error checking file existence: {e}. Creating new file.")
        # Create the directory structure if it doesn't exist
        dir_path = '/'.join(file_path.split('/')[:-1])
        if dir_path:
            hdfs_client.makedirs(dir_path)
        with hdfs_client.write(file_path, encoding='utf-8') as writer:
            writer.write('[\n')
        logger.info(f"Created new file: {file_path}")

    # Read the current file content
    try:
        with hdfs_client.read(file_path, encoding='utf-8') as reader:
            content = reader.read()
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        content = '[\n'

    # Check if the file is properly formatted as a JSON array
    if not content.startswith('['):
        logger.warning("File is not properly formatted as a JSON array. Reformatting.")
        content = '[\n'

    # If file has more than just the opening bracket, we need to add a comma
    need_comma = content.strip() != '[' and not content.strip().endswith(',')

    # Append the new data
    with hdfs_client.write(file_path, encoding='utf-8', append=False) as writer:
        # Write everything except the closing bracket if it exists
        if content.rstrip().endswith(']'):
            writer.write(content.rstrip()[:-1])
        else:
            writer.write(content.rstrip())
        
        # Add comma if needed
        if need_comma:
            writer.write(',\n')
        
        # Add the new entry
        writer.write(json.dumps(data, indent=2))
        writer.write('\n]')

    logger.info(f"Appended data to {file_path}")

def process_messages(timeout_seconds=120):
    """Process messages from Kafka and write to HDFS."""
    try:
        # Connect to Kafka and HDFS
        consumer = connect_to_kafka()
        hdfs_client = connect_to_hdfs()
        
        # Set start time
        start_time = time.time()
        message_count = 0
        
        # Process messages until timeout
        while time.time() - start_time < timeout_seconds:
            # Poll for messages
            messages = consumer.poll(timeout_ms=1000, max_records=10)
            
            for topic_partition, records in messages.items():
                for record in records:
                    # Enrich the message with metadata
                    enriched_data = {
                        'topic': record.topic,
                        'partition': record.partition,
                        'offset': record.offset,
                        'timestamp': datetime.fromtimestamp(record.timestamp / 1000).isoformat(),
                        'data': record.value
                    }
                    
                    # Append to HDFS file
                    append_to_hdfs_file(hdfs_client, OUTPUT_FILE, enriched_data)
                    message_count += 1
                    logger.info(f"Processed message from {record.topic}: offset={record.offset}")
            
            # If no messages, wait a bit
            if not messages:
                time.sleep(1)
        
        consumer.close()
        logger.info(f"Consumer closed after processing {message_count} messages")
        return message_count

    except Exception as e:
        logger.error(f"Error processing messages: {e}")
        if 'consumer' in locals():
            consumer.close()
        return 0

if __name__ == "__main__":
    logger.info("Starting Kafka to HDFS consumer")
    process_messages(timeout_seconds=300)  # Run for 5 minutes
