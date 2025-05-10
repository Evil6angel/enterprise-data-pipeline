#!/bin/bash

# Colors for terminal output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=============================================${NC}"
echo -e "${BLUE}  Generating Test Data for CDC Pipeline      ${NC}"
echo -e "${BLUE}=============================================${NC}"

# Generate data in PostgreSQL
echo -e "\n${GREEN}Generating test data in PostgreSQL...${NC}"

# Insert new customers
for i in {1..3}; do
  first_name="User${i}"
  last_name="Test${i}"
  email="user${i}_$(date +%s)@example.com"
  
  docker exec -i postgres psql -U postgres -d inventory << SQL
INSERT INTO customers (first_name, last_name, email) 
VALUES ('${first_name}', '${last_name}', '${email}');
SQL
  
  if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Created customer: ${first_name} ${last_name}${NC}"
  else
    echo -e "${RED}✗ Failed to create customer${NC}"
  fi
  
  # Short delay between operations
  sleep 1
done

# Update a customer
echo -e "\n${GREEN}Updating a customer...${NC}"
docker exec -i postgres psql -U postgres -d inventory << SQL
UPDATE customers 
SET email = 'updated_$(date +%s)@example.com' 
WHERE id IN (SELECT id FROM customers ORDER BY id DESC LIMIT 1);
SQL

if [ $? -eq 0 ]; then
  echo -e "${GREEN}✓ Updated a customer${NC}"
else
  echo -e "${RED}✗ Failed to update customer${NC}"
fi

sleep 1

# Delete a customer (occasionally)
if [ $((RANDOM % 5)) -eq 0 ]; then
  echo -e "\n${GREEN}Deleting a customer...${NC}"
  docker exec -i postgres psql -U postgres -d inventory << SQL
DELETE FROM customers 
WHERE id IN (SELECT id FROM customers ORDER BY id ASC LIMIT 1);
SQL

  if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Deleted a customer${NC}"
  else
    echo -e "${RED}✗ Failed to delete customer${NC}"
  fi
fi

# Show current data
echo -e "\n${GREEN}Current data in customers table:${NC}"
docker exec -i postgres psql -U postgres -d inventory -c "SELECT * FROM customers ORDER BY id DESC LIMIT 5;"

# Wait for data to propagate to HDFS
echo -e "\n${BLUE}Waiting for data to propagate to HDFS...${NC}"
sleep 5

# Check HDFS data
echo -e "\n${GREEN}Checking HDFS data:${NC}"
docker exec namenode hdfs dfs -ls /data/cdc/output.json

# Show sample of HDFS data
echo -e "\n${GREEN}Sample of HDFS data:${NC}"
docker exec namenode hdfs dfs -cat /data/cdc/output.json | tail -n 2

echo -e "\n${BLUE}=============================================${NC}"
echo -e "${BLUE}  Test Data Generation Complete              ${NC}"
echo -e "${BLUE}=============================================${NC}"
