#!/bin/bash

# Create the network if it doesn't exist
../create-network.sh

# Create data directories if they don't exist
mkdir -p ./data/namenode ./data/datanode ./data/postgresql

# Start Hadoop and related services
docker-compose up -d

echo "Hadoop and related services are starting up..."
echo "This may take a few minutes for all services to be ready."
echo "You can check logs with: docker-compose logs -f"
echo "Hadoop UI will be available at: http://localhost:9870"
echo "Hive will be available at: http://localhost:10002"
