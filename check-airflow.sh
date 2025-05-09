#!/bin/bash

echo "Checking Airflow services status..."

# Check if containers are running
echo "Container status:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep airflow

# Check webserver logs
echo -e "\nWebserver logs:"
docker logs airflow-webserver --tail 20

# Check port binding
echo -e "\nPort 8088 usage:"
netstat -tulpn | grep 8088

# Try to connect to the webserver
echo -e "\nTesting connection to webserver:"
curl -v http://localhost:8088
