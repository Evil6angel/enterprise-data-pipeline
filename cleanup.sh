#!/bin/bash

echo "Stopping all services..."

# Stop services in reverse order of dependency
echo "Stopping Airflow services..."
cd airflow && docker-compose down -v
cd ..

echo "Stopping Hadoop services..."
cd hadoop && docker-compose down -v
cd ..

echo "Stopping Kafka services..."
cd kafka && docker-compose down -v
cd ..

echo "Stopping Postgres services..."
cd postgres && docker-compose down -v
cd ..

# Force stop any remaining containers
echo "Force stopping any remaining containers..."
docker ps -q | xargs -r docker stop

# Force remove any remaining containers
echo "Force removing any remaining containers..."
docker ps -aq | xargs -r docker rm -f

# Remove the network
echo "Removing network..."
docker network rm data_pipeline_network

# Remove any dangling volumes
echo "Cleaning up volumes..."
docker volume prune -f

echo "Cleanup complete!"
