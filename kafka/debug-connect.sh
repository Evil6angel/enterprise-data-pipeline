#!/bin/bash

echo "Debugging Kafka Connect Service"
echo "==============================="

echo "1. Checking Kafka Connect container status:"
docker ps | grep kafka-connect

echo "2. Checking Kafka Connect logs for errors:"
docker logs kafka-connect | grep -i error

echo "3. Checking Kafka Connect REST API availability:"
docker exec -it kafka-connect curl -v http://localhost:8083/

echo "4. Listing available endpoints:"
docker exec -it kafka-connect curl -s http://localhost:8083/ | grep -o '"[^"]*"' | tr -d '"'

echo "5. Checking connector plugins:"
docker exec -it kafka-connect curl -s http://localhost:8083/connector-plugins | jq '.'

echo "6. Checking running connectors:"
docker exec -it kafka-connect curl -s http://localhost:8083/connectors | jq '.'

echo "Debugging complete. Check the output above for clues about the issue."
