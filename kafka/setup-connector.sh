#!/bin/bash

echo "Setting up Debezium connector..."

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
max_attempts=10
attempt=0
while [ $attempt -lt $max_attempts ]
do
  status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/connectors)
  if [ $status -eq 200 ]; then
    echo "Kafka Connect is ready"
    break
  fi
  attempt=$((attempt+1))
  echo "Attempt $attempt/$max_attempts: Kafka Connect not ready yet (status: $status), waiting..."
  sleep 5
done

# Create Debezium PostgreSQL connector
echo "Creating Debezium PostgreSQL connector..."
curl -X POST \
  -H "Content-Type: application/json" \
  --data '{
    "name": "inventory-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "plugin.name": "pgoutput",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "postgres",
      "database.server.name": "dbserver1",
      "topic.prefix": "dbserver1",
      "table.include.list": "inventory.customers,inventory.orders,inventory.order_items",
      "publication.name": "dbz_publication",
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "false",
      "transforms.unwrap.delete.handling.mode": "rewrite",
      "transforms.unwrap.add.fields": "op,table,lsn,source.ts_ms"
    }
  }' \
  http://localhost:8083/connectors

# Check connector status
echo "Checking connector status..."
curl -s http://localhost:8083/connectors/inventory-connector/status

echo "Connector created successfully!"
