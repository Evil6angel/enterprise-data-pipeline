#!/bin/bash

# Stop any running Airflow containers
echo "Stopping Airflow containers..."
docker-compose down

# Ensure directory permissions
echo "Setting up directory permissions..."
mkdir -p ./logs ./dags ./plugins ./config
chmod -R 777 ./logs ./dags ./plugins ./config

# Create the DAG file if it doesn't exist
if [ ! -f ./dags/cdc_pipeline_dag.py ]; then
    echo "Creating DAG file..."
    cat > ./dags/cdc_pipeline_dag.py << 'DAGFILE'
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'cdc_pipeline',
    default_args=default_args,
    description='CDC Pipeline Orchestration',
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2025, 5, 10),
    catchup=False,
    tags=['cdc', 'kafka', 'hdfs'],
)

# Check services
check_services = BashOperator(
    task_id='check_services',
    bash_command='echo "Checking services..."',
    dag=dag,
)

# Check PostgreSQL
check_postgres = BashOperator(
    task_id='check_postgres',
    bash_command='docker exec postgres pg_isready -U postgres || exit 1',
    dag=dag,
)

# Generate test data
generate_data = BashOperator(
    task_id='generate_test_data',
    bash_command='docker exec postgres psql -U postgres -d inventory -c "INSERT INTO customers (first_name, last_name, email) VALUES (\'Test\', \'User\', CONCAT(\'test\', now()::text, \'@example.com\'));"',
    dag=dag,
)

# Verify HDFS data
verify_hdfs = BashOperator(
    task_id='verify_hdfs',
    bash_command='docker exec namenode hdfs dfs -ls /data/cdc/output.json || echo "HDFS file not found"',
    dag=dag,
)

# Define the workflow
check_services >> check_postgres >> generate_data >> verify_hdfs
DAGFILE
fi

# Create simple helper scripts if they don't exist
if [ ! -f ./config/data_generator.py ]; then
    echo "Creating helper scripts..."
    mkdir -p ./config
    
    cat > ./config/data_generator.py << 'PYFILE'
#!/usr/bin/env python3
import os
import subprocess
import random
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_generator")

def generate_test_data():
    """Generate test data in PostgreSQL"""
    logger.info("Generating test data")
    
    cmd = "docker exec postgres psql -U postgres -d inventory -c \"INSERT INTO customers (first_name, last_name, email) VALUES ('Test', 'User', 'test_' || now() || '@example.com');\""
    
    try:
        subprocess.run(cmd, shell=True, check=True)
        logger.info("Data generation successful")
        return True
    except Exception as e:
        logger.error(f"Error generating data: {e}")
        return False

if __name__ == "__main__":
    generate_test_data()
PYFILE

    cat > ./config/kafka_to_hdfs.py << 'PYFILE'
#!/usr/bin/env python3
import os
import subprocess
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka_to_hdfs")

def check_kafka_to_hdfs_status():
    """Check if data is flowing from Kafka to HDFS"""
    logger.info("Checking Kafka to HDFS data flow")
    
    cmd = "docker exec namenode hdfs dfs -ls /data/cdc/output.json"
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("HDFS file exists")
            return True
        else:
            logger.error("HDFS file does not exist")
            return False
    except Exception as e:
        logger.error(f"Error checking HDFS: {e}")
        return False

if __name__ == "__main__":
    check_kafka_to_hdfs_status()
PYFILE

    chmod +x ./config/data_generator.py
    chmod +x ./config/kafka_to_hdfs.py
fi

# Initialize Airflow
echo "Initializing Airflow..."
docker-compose up -d airflow-init

# Wait for initialization to complete
echo "Waiting for initialization to complete..."
sleep 30

# Start Airflow services
echo "Starting Airflow services..."
docker-compose up -d

echo "Airflow initialization completed!"
