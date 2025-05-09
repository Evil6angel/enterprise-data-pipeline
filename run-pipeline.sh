#!/bin/bash

# Function to check if a command was successful
check_status() {
    if [ $? -ne 0 ]; then
        echo "Error: $1 failed"
        exit 1
    fi
}

echo "Starting Enterprise CDC Pipeline..."

# Clean up any existing services first
./cleanup.sh
check_status "Cleanup"

# Create the network
./create-network.sh
check_status "Network creation"

# Start PostgreSQL
echo "Starting PostgreSQL..."
cd postgres && ./start-postgres.sh
check_status "PostgreSQL startup"
cd ..

# Wait for PostgreSQL to initialize
echo "Waiting for PostgreSQL to start..."
sleep 30

# Start Kafka
echo "Starting Kafka..."
cd kafka && ./start-kafka.sh
check_status "Kafka startup"
cd ..

# Wait for Kafka and Kafka Connect to initialize
echo "Waiting for Kafka to start..."
sleep 60

# Start Hadoop/HDFS
echo "Starting Hadoop/HDFS..."
cd hadoop && ./start-hadoop.sh
check_status "Hadoop startup"
cd ..

# Wait for Hadoop to initialize
echo "Waiting for Hadoop/HDFS to start..."
sleep 60

# Initialize HDFS directories
echo "Initializing HDFS directories..."
cd hadoop
./init-hdfs.sh
check_status "HDFS initialization"
cd ..

# Fix permissions for Debezium
echo "Setting up permissions for Debezium..."
docker exec -u root postgres chmod 755 /var/lib/postgresql/data/global
check_status "Permission setup"

# Set up Debezium connector
echo "Setting up Debezium connector..."
cd kafka && ./setup-connector.sh
check_status "Debezium connector setup"
cd ..

# Wait for connector to initialize
echo "Waiting for connector to start..."
sleep 20

# Start Airflow
echo "Starting Airflow..."
cd airflow && ./start-airflow.sh
check_status "Airflow startup"
cd ..

# Wait for Airflow to initialize
echo "Waiting for Airflow to start..."
sleep 60

echo "Pipeline components are running!"
echo "You can now trigger the DAG 'cdc_pipeline' in the Airflow UI at http://localhost:8088"
echo "Or you can run the test generator manually:"
echo "cd test_generator && ./setup-and-run.sh && ./data_generator.py"
echo "The Kafka consumer will write the data to HDFS at /data/cdc/output.json"
