#!/bin/bash

# Create the network if it doesn't exist
../create-network.sh

# Start PostgreSQL
docker-compose up -d

echo "PostgreSQL is starting up..."
echo "Wait a few seconds for it to be ready."
echo "You can check logs with: docker-compose logs -f"
