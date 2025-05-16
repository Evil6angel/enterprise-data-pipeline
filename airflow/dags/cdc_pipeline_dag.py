from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor

import sys
import os
sys.path.append('/opt/airflow/config')
from data_generator import generate_test_data
from kafka_to_hdfs import check_kafka_to_hdfs_status

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

# Check that Postgres is ready using pg_isready
check_postgres = BashOperator(
    task_id='check_postgres',
    bash_command='pg_isready -h postgres -U postgres',
    dag=dag,
)

# Check if Kafka is reachable by listing topics (no docker exec)
# This assumes 'kafka-topics.sh' is available in the Airflow container; 
# if not, use a PythonOperator or install kafka-python and use it.
check_kafka = BashOperator(
    task_id='check_kafka',
    bash_command='nc -z kafka 9092',  # Simple port check; replace with kafka-python for a real health check
    dag=dag,
)

# Check if HDFS Namenode is up by checking the web UI port
check_namenode = BashOperator(
    task_id='check_namenode',
    bash_command='nc -z namenode 9870',  # Namenode HTTP UI port
    dag=dag,
)

# Check if the consumer service (container) is running using a network or process check
# Example: Check if the expected consumer port is open (replace with actual port if different)
check_consumer = BashOperator(
    task_id='check_consumer',
    bash_command='nc -z kafka-connect 8083 || (echo "Kafka Connect not running!" && exit 1)',
    dag=dag,
)

# Generate test data in PostgreSQL
run_test_generator = PythonOperator(
    task_id='generate_test_data',
    python_callable=generate_test_data,
    dag=dag,
)

# Wait for HDFS to have the output file using HdfsSensor (recommended) instead of bash polling with docker exec
wait_for_hdfs_data = HdfsSensor(
    task_id='wait_for_hdfs_data',
    filepath='/data/cdc/output.json',
    hdfs_conn_id='hdfs_default',
    timeout=60,
    poke_interval=2,
    dag=dag,
)

# Verify data quality 
verify_data_quality = PythonOperator(
    task_id='verify_data_quality',
    python_callable=check_kafka_to_hdfs_status,
    dag=dag,
)

# Flow definition
[check_postgres, check_kafka, check_namenode, check_consumer] >> run_test_generator >> wait_for_hdfs_data >> verify_data_quality