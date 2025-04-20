#!/bin/bash

echo "Starting Enterprise CDC Pipeline..."

# Create the network if it doesn't exist
./create-network.sh

# Start PostgreSQL
echo "Starting PostgreSQL..."
cd postgres && ./start-postgres.sh
cd ..

# Wait for PostgreSQL to initialize
echo "Waiting for PostgreSQL to start..."
sleep 20

# Start Kafka
echo "Starting Kafka..."
cd kafka && ./start-kafka.sh
cd ..

# Wait for Kafka and Kafka Connect to initialize
echo "Waiting for Kafka to start..."
sleep 45

# Start Hadoop/HDFS
echo "Starting Hadoop/HDFS..."
cd hadoop && ./start-hadoop.sh
cd ..

# Wait for Hadoop to initialize
echo "Waiting for Hadoop/HDFS to start..."
sleep 45

# Initialize HDFS directories
echo "Initializing HDFS directories..."
cd hadoop && ./init-hdfs.sh
cd ..

# Set up Debezium connector
echo "Setting up Debezium connector..."
cd kafka && ./setup-connector.sh
cd ..

# Wait for connector to initialize
echo "Waiting for connector to start..."
sleep 10

# Start Airflow
echo "Starting Airflow..."
cd airflow && ./start-airflow.sh
cd ..

# Wait for Airflow to initialize
echo "Waiting for Airflow to start..."
sleep 45

echo "Pipeline components are running!"
echo "You can now trigger the DAG 'cdc_pipeline' in the Airflow UI at http://localhost:8088"
echo "Or you can run the test generator manually:"
echo "cd test_generator && ./setup-and-run.sh && ./data_generator.py"
echo "The Kafka consumer will write the data to HDFS at /data/cdc/output.json"
