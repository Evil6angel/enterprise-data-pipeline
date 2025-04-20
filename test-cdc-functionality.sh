#!/bin/bash

echo "Testing CDC functionality"
echo "========================="

echo "1. Inserting a new customer into PostgreSQL..."
docker exec -it postgres psql -U postgres -d postgres -c "INSERT INTO inventory.customers (first_name, last_name, email) VALUES ('Test', 'User', 'test.user@example.com');"

echo "2. Waiting a moment for the change to propagate..."
sleep 5

echo "3. Checking Kafka topics to see if the change was captured..."
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list | grep dbserver1

echo "4. Checking messages in the customers topic..."
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic dbserver1.inventory.customers --from-beginning --max-messages 1

echo "Testing complete!"
