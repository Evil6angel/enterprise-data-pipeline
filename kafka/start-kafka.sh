#!/bin/bash

# Create the network if it doesn't exist
../create-network.sh

# Start Kafka and related services
docker-compose up -d

echo "Kafka and related services are starting up..."
echo "This may take a few moments."
echo "You can check logs with: docker-compose logs -f"
echo "Kafka UI will be available at: http://localhost:8080"
