#!/usr/bin/env python3

import json
import time
import os
import requests
from confluent_kafka import Consumer
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
HDFS_WEBHDFS_URL = os.getenv('HDFS_WEBHDFS_URL', 'http://namenode:9870/webhdfs/v1')
HDFS_USER = os.getenv('HDFS_USER', 'root')
HDFS_OUTPUT_PATH = os.getenv('HDFS_OUTPUT_PATH', '/data/cdc/output.json')

def connect_to_kafka():
    """Connect to Kafka and return consumer"""
    retry_count = 0
    max_retries = 5
    while retry_count < max_retries:
        try:
            logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
            
            # Configure consumer
            conf = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': 'cdc-consumer-group',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True
            }
            
            consumer = Consumer(conf)
            consumer.subscribe(KAFKA_TOPICS)
            
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

def check_file_exists(file_path):
    """Check if file exists in HDFS using WebHDFS API"""
    url = f"{HDFS_WEBHDFS_URL}{file_path}?op=GETFILESTATUS&user.name={HDFS_USER}"
    try:
        response = requests.get(url)
        return response.status_code == 200
    except:
        return False

def create_directory_if_not_exists(dir_path):
    """Create HDFS directory if it doesn't exist"""
    # Extract directory path from file path
    if '/' in dir_path:
        url = f"{HDFS_WEBHDFS_URL}{dir_path}?op=MKDIRS&user.name={HDFS_USER}"
        try:
            response = requests.put(url)
            return response.status_code in (200, 201)
        except Exception as e:
            logger.error(f"Error creating directory {dir_path}: {str(e)}")
            return False
    return True

def append_to_hdfs(data):
    """Append data to the HDFS file using WebHDFS API"""
    try:
        # Ensure the directory exists
        dir_path = os.path.dirname(HDFS_OUTPUT_PATH)
        if dir_path:
            create_directory_if_not_exists(dir_path)
        
        # Prepare data for appending (add newline)
        data_str = json.dumps(data) + '\n'
        
        # Check if file exists
        file_exists = check_file_exists(HDFS_OUTPUT_PATH)
        
        if not file_exists:
            # Create a new file
            url = f"{HDFS_WEBHDFS_URL}{HDFS_OUTPUT_PATH}?op=CREATE&user.name={HDFS_USER}&overwrite=true"
            logger.info(f"Creating new file at {HDFS_OUTPUT_PATH}")
            
            # First request gets a redirect
            response = requests.put(url, allow_redirects=False)
            
            if response.status_code == 307:
                # Follow the redirect with the data
                redirect_url = response.headers['Location']
                upload_response = requests.put(redirect_url, data=data_str.encode('utf-8'))
                
                if upload_response.status_code in (200, 201):
                    logger.info("Successfully created file in HDFS")
                    return True
                else:
                    logger.error(f"Error creating file: {upload_response.text}")
                    return False
            else:
                logger.error(f"Error initiating file creation: {response.text}")
                return False
        else:
            # Append to existing file
            url = f"{HDFS_WEBHDFS_URL}{HDFS_OUTPUT_PATH}?op=APPEND&user.name={HDFS_USER}"
            logger.info(f"Appending to existing file at {HDFS_OUTPUT_PATH}")
            
            # First request gets a redirect
            response = requests.post(url, allow_redirects=False)
            
            if response.status_code == 307:
                # Follow the redirect with the data
                redirect_url = response.headers['Location']
                append_response = requests.post(redirect_url, data=data_str.encode('utf-8'))
                
                if append_response.status_code == 200:
                    logger.info("Successfully appended to file in HDFS")
                    return True
                else:
                    logger.error(f"Error appending to file: {append_response.text}")
                    return False
            else:
                logger.error(f"Error initiating append operation: {response.text}")
                return False
    except Exception as e:
        logger.error(f"Error writing to HDFS: {str(e)}")
        return False

def process_messages():
    """Process messages from Kafka and write to HDFS"""
    try:
        consumer = connect_to_kafka()
        
        logger.info("Starting to consume messages...")
        
        while True:
            try:
                # Poll for messages
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                # Decode JSON value
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                except Exception as e:
                    logger.error(f"Error decoding message: {str(e)}")
                    continue
                
                # Add metadata to the message
                enriched_data = {
                    'topic': msg.topic(),
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'timestamp': msg.timestamp()[1] if msg.timestamp() else None,
                    'data': value
                }
                
                # Log the received message (truncated for readability)
                value_str = str(value)
                log_str = value_str[:100] + '...' if len(value_str) > 100 else value_str
                logger.info(f"Received message: {log_str}")
                
                # Write to HDFS
                success = append_to_hdfs(enriched_data)
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
