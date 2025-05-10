from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
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

# Check that all services are healthy
check_postgres = BashOperator(
    task_id='check_postgres',
    bash_command='docker exec postgres pg_isready -U postgres || exit 1',
    dag=dag,
)

check_kafka = BashOperator(
    task_id='check_kafka',
    bash_command='docker exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list > /dev/null || exit 1',
    dag=dag,
)

check_namenode = BashOperator(
    task_id='check_namenode',
    bash_command='docker exec namenode hdfs dfsadmin -report > /dev/null || exit 1',
    dag=dag,
)

check_consumer = BashOperator(
    task_id='check_consumer',
    bash_command='docker ps | grep kafka-consumer > /dev/null || echo "Restarting consumer" && docker start kafka-consumer || docker run -d --name kafka-consumer --network=data_pipeline_network -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 -e HDFS_WEBHDFS_URL=http://namenode:9870/webhdfs/v1 kafka-hdfs-consumer',
    dag=dag,
)

# Generate test data in PostgreSQL
run_test_generator = PythonOperator(
    task_id='generate_test_data',
    python_callable=generate_test_data,
    dag=dag,
)

# Check if data was written to HDFS within a reasonable timeframe
wait_for_hdfs_data = BashOperator(
    task_id='wait_for_hdfs_data',
    bash_command='timeout 60s bash -c \'until docker exec namenode hdfs dfs -ls /data/cdc/output.json; do sleep 2; done\'',
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