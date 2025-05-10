import requests
import json
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('kafka_to_hdfs')

HDFS_WEBHDFS_URL = "http://namenode:9870/webhdfs/v1"
HDFS_OUTPUT_PATH = "/data/cdc/output.json"
HDFS_USER = "root"

def check_kafka_to_hdfs_status():
    """
    Check if data is flowing from Kafka to HDFS properly
    """
    logger.info("Checking Kafka to HDFS data flow status")
    
    # Check if file exists
    try:
        file_status_url = f"{HDFS_WEBHDFS_URL}{HDFS_OUTPUT_PATH}?op=GETFILESTATUS&user.name={HDFS_USER}"
        response = requests.get(file_status_url, timeout=10)
        
        if response.status_code != 200:
            logger.error(f"Output file not found in HDFS: {response.status_code}")
            return False
        
        file_info = response.json()
        file_size = file_info['FileStatus']['length']
        modification_time = file_info['FileStatus']['modificationTime'] / 1000  # Convert to seconds
        
        logger.info(f"Output file exists in HDFS (Size: {file_size} bytes)")
        
        # Check if file was modified recently
        current_time = datetime.now().timestamp()
        last_modified = datetime.fromtimestamp(modification_time)
        last_modified_str = last_modified.strftime('%Y-%m-%d %H:%M:%S')
        
        if current_time - modification_time > 3600:  # Older than 1 hour
            logger.warning(f"Output file hasn't been updated recently (Last modified: {last_modified_str})")
            return False
        
        # Read sample data from file to verify content
        read_url = f"{HDFS_WEBHDFS_URL}{HDFS_OUTPUT_PATH}?op=OPEN&user.name={HDFS_USER}&offset=0&length=4096"
        response = requests.get(read_url, timeout=10, allow_redirects=True)
        
        if response.status_code != 200:
            logger.error(f"Failed to read output file: {response.status_code}")
            return False
        
        # Verify data has proper format
        try:
            lines = response.text.strip().split('\n')
            if not lines:
                logger.error("Output file is empty")
                return False
            
            # Validate JSON format for each line
            valid_records = 0
            for line in lines:
                record = json.loads(line)
                if "topic" in record and "data" in record:
                    valid_records += 1
            
            if valid_records == 0:
                logger.error("No valid CDC records found in file")
                return False
            
            logger.info(f"Found {valid_records} valid CDC records in sample data")
            return True
            
        except Exception as e:
            logger.error(f"Error validating data format: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Error checking Kafka to HDFS status: {e}")
        return False

if __name__ == "__main__":
    check_kafka_to_hdfs_status()