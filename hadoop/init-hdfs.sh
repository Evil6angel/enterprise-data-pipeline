#!/bin/bash

echo "Initializing HDFS directories..."

# Create necessary directories in HDFS
docker exec -it namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec -it namenode hdfs dfs -mkdir -p /data/cdc/
docker exec -it namenode hdfs dfs -chmod -R 777 /user/hive/warehouse
docker exec -it namenode hdfs dfs -chmod -R 777 /data/cdc/

echo "HDFS directories created successfully!"
