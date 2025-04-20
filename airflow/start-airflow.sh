#!/bin/bash

# Check for existing containers and remove them if necessary
echo "Checking for existing Airflow containers..."
if docker ps -a | grep -q "airflow-postgres"; then
    echo "Removing existing airflow-postgres container..."
    docker stop airflow-postgres || true
    docker rm airflow-postgres || true
fi

# Start Airflow services
echo "Starting Airflow services..."
docker-compose up -d

# Wait for Airflow to initialize
echo "Waiting for Airflow to initialize..."
sleep 10

echo "Airflow is now running:"
echo "- Web UI: http://localhost:8088"
echo "- Username: airflow"
echo "- Password: airflow"
