#!/usr/bin/env python3

import json
import time
import os
from kafka import KafkaConsumer
from hdfs import InsecureClient
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_consumer')

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_TOPICS = os.getenv('KAFKA_TOPICS', 'dbserver1.inventory.customers,dbserver1.inventory.orders,dbserver1.inventory.order_items').split(',')
HDFS_URL = os.getenv('HDFS_URL', 'http://localhost:9870')
HDFS_USER = os.getenv('HDFS_USER', 'root')
HDFS_OUTPUT_PATH = os.getenv('HDFS_OUTPUT_PATH', '/data/cdc/output.json')

def connect_to_kafka():
    """Connect to Kafka and return consumer"""
    retry_count = 0
    max_retries = 5
    while retry_count < max_retries:
        try:
            logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
            consumer = KafkaConsumer(
                *KAFKA_TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='cdc-consumer-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
            )
            logger.info("Successfully connected to Kafka")
            return consumer
        except Exception as e:
            retry_count += 1
            wait_time = retry_count * 5
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            logger.info(f"Retrying in {wait_time} seconds (attempt {retry_count}/{max_retries})...")
            time.sleep(wait_time)
    
    logger.error("Failed to connect to Kafka after multiple attempts")
    raise Exception("Could not connect to Kafka")

def connect_to_hdfs():
    """Connect to HDFS and return client"""
    retry_count = 0
    max_retries = 5
    while retry_count < max_retries:
        try:
            logger.info(f"Connecting to HDFS at {HDFS_URL}...")
            client = InsecureClient(HDFS_URL, user=HDFS_USER)
            logger.info("Successfully connected to HDFS")
            return client
        except Exception as e:
            retry_count += 1
            wait_time = retry_count * 5
            logger.error(f"Failed to connect to HDFS: {str(e)}")
            logger.info(f"Retrying in {wait_time} seconds (attempt {retry_count}/{max_retries})...")
            time.sleep(wait_time)
    
    logger.error("Failed to connect to HDFS after multiple attempts")
    raise Exception("Could not connect to HDFS")

def append_to_hdfs(hdfs_client, data):
    """Append data to the HDFS file"""
    try:
        # Check if file exists
        try:
            file_exists = hdfs_client.status(HDFS_OUTPUT_PATH) is not None
        except:
            file_exists = False
        
        # Prepare data for appending (add newline)
        data_str = json.dumps(data) + '\n'
        
        if not file_exists:
            logger.info(f"Creating new file at {HDFS_OUTPUT_PATH}")
            with hdfs_client.write(HDFS_OUTPUT_PATH, encoding='utf-8') as writer:
                writer.write(data_str)
        else:
            logger.info(f"Appending to existing file at {HDFS_OUTPUT_PATH}")
            with hdfs_client.write(HDFS_OUTPUT_PATH, append=True, encoding='utf-8') as writer:
                writer.write(data_str)
        
        logger.info("Successfully wrote data to HDFS")
        return True
    except Exception as e:
        logger.error(f"Error writing to HDFS: {str(e)}")
        return False

def process_messages():
    """Process messages from Kafka and write to HDFS"""
    try:
        consumer = connect_to_kafka()
        hdfs_client = connect_to_hdfs()
        
        logger.info("Starting to consume messages...")
        for message in consumer:
            try:
                # Skip None messages
                if message.value is None:
                    continue
                
                # Add metadata to the message
                enriched_data = {
                    'topic': message.topic,
                    'partition': message.partition,
                    'offset': message.offset,
                    'timestamp': message.timestamp,
                    'data': message.value
                }
                
                # Log the received message (truncated for readability)
                value_str = str(message.value)
                log_str = value_str[:100] + '...' if len(value_str) > 100 else value_str
                logger.info(f"Received message: {log_str}")
                
                # Write to HDFS
                success = append_to_hdfs(hdfs_client, enriched_data)
                if not success:
                    logger.warning("Failed to write message to HDFS, will retry on next message")
            
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")
    
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        if 'consumer' in locals():
            consumer.close()
            logger.info("Consumer closed")

if __name__ == "__main__":
    logger.info("CDC Kafka Consumer starting up...")
    process_messages()
