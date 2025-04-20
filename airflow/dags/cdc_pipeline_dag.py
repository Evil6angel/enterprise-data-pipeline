from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import os
import sys
import logging
import time
import json
import subprocess
from kafka import KafkaConsumer
from hdfs import InsecureClient

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'cdc_pipeline',
    default_args=default_args,
    description='CDC Pipeline from PostgreSQL to HDFS via Kafka',
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2025, 4, 20),
    catchup=False,
    tags=['cdc', 'pipeline', 'data-engineering'],
)

# Task: check if PostgreSQL is running
check_postgres = BashOperator(
    task_id='check_postgres',
    bash_command='docker ps | grep postgres || echo "PostgreSQL is not running!"',
    dag=dag,
)

# Task: check if Kafka is running
check_kafka = BashOperator(
    task_id='check_kafka',
    bash_command='docker ps | grep kafka || echo "Kafka is not running!"',
    dag=dag,
)

# Task: check if Hadoop is running
check_hadoop = BashOperator(
    task_id='check_hadoop',
    bash_command='docker ps | grep namenode || echo "Hadoop is not running!"',
    dag=dag,
)

# Function: check connector status
def check_connector_status():
    try:
        result = subprocess.run(
            ['curl', '-s', 'http://localhost:8083/connectors/inventory-connector/status'],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            return False

        status_data = json.loads(result.stdout)
        connector_state = status_data.get('connector', {}).get('state', '')

        if connector_state.upper() != 'RUNNING':
            logging.error(f"Connector state is {connector_state}, attempting to recreate...")
            return False

        return True
    except Exception as e:
        logging.error(f"Error checking connector status: {str(e)}")
        return False

# Function: ensure connector running
def ensure_connector_running():
    if not check_connector_status():
        try:
            connector_config = {
                "name": "inventory-connector",
                "config": {
                    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                    "plugin.name": "pgoutput",
                    "database.hostname": "postgres",
                    "database.port": "5432",
                    "database.user": "postgres",
                    "database.password": "postgres",
                    "database.dbname": "postgres",
                    "database.server.name": "dbserver1",
                    "topic.prefix": "dbserver1",
                    "table.include.list": "inventory.customers,inventory.orders,inventory.order_items",
                    "publication.name": "dbz_publication",
                    "transforms": "unwrap",
                    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                    "transforms.unwrap.drop.tombstones": "false",
                    "transforms.unwrap.delete.handling.mode": "rewrite",
                    "transforms.unwrap.add.fields": "op,table,lsn,source.ts_ms"
                }
            }

            # Delete existing connector if present
            subprocess.run(['curl', '-s', '-X', 'DELETE', 'http://localhost:8083/connectors/inventory-connector'])
            time.sleep(2)

            # Create connector
            result = subprocess.run(
                ['curl', '-s', '-X', 'POST', '-H', 'Content-Type: application/json',
                 '--data', json.dumps(connector_config),
                 'http://localhost:8083/connectors'],
                capture_output=True, text=True
            )

            if result.returncode != 0:
                logging.error("Failed to create connector")
                return False

            # Wait and verify
            time.sleep(5)
            return check_connector_status()

        except Exception as e:
            logging.error(f"Error ensuring connector running: {str(e)}")
            return False

    return True

# Task: ensure Debezium connector is running
check_connector = PythonOperator(
    task_id='check_connector',
    python_callable=ensure_connector_running,
    dag=dag,
)

# Task: copy data generator script into Airflow config
copy_data_generator = BashOperator(
    task_id='copy_data_generator',
    bash_command='cp /opt/airflow/../../test_generator/data_generator.py /opt/airflow/config/ || echo "Could not copy data_generator.py"',
    dag=dag,
)

# Task: generate test data
def generate_test_data():
    try:
        env = os.environ.copy()
        env['NUM_OPERATIONS'] = '20'
        env['OPERATION_DELAY'] = '0.5'
        subprocess.run(['python', '/opt/airflow/config/data_generator.py'], env=env)
        return True
    except Exception as e:
        logging.error(f"Error generating test data: {str(e)}")
        return False

generate_data = PythonOperator(
    task_id='generate_data',
    python_callable=generate_test_data,
    dag=dag,
)

# Task: copy Kafka consumer script into Airflow config
copy_kafka_consumer = BashOperator(
    task_id='copy_kafka_consumer',
    bash_command='cp /opt/airflow/../../kafka_consumer/kafka_to_hdfs.py /opt/airflow/config/ || echo "Could not copy kafka_to_hdfs.py"',
    dag=dag,
)

# Task: consume from Kafka and write to HDFS
def consume_kafka_to_hdfs():
    try:
        subprocess.run(['python', '/opt/airflow/config/kafka_to_hdfs.py'])
        return True
    except Exception as e:
        logging.error(f"Error consuming from Kafka to HDFS: {str(e)}")
        return False

kafka_to_hdfs = PythonOperator(
    task_id='kafka_to_hdfs',
    python_callable=consume_kafka_to_hdfs,
    dag=dag,
)

# Task: verify HDFS output
def verify_hdfs_output():
    try:
        client = InsecureClient('http://localhost:9870', user='root')
        status = client.status('/data/cdc/output.json')
        logging.info(f"HDFS output file status: {status}")
        with client.read('/data/cdc/output.json', encoding='utf-8') as reader:
            for i, line in enumerate(reader):
                if i >= 5:
                    break
                logging.info(f"Line {i+1}: {line.strip()}")
        return True
    except Exception as e:
        logging.error(f"Error verifying HDFS output: {str(e)}")
        return False

verify_output = PythonOperator(
    task_id='verify_output',
    python_callable=verify_hdfs_output,
    dag=dag,
)

# Define task dependencies
[check_postgres, check_kafka, check_hadoop] >> check_connector
check_connector >> copy_data_generator >> generate_data
check_connector >> copy_kafka_consumer >> kafka_to_hdfs
[generate_data, kafka_to_hdfs] >> verify_output
