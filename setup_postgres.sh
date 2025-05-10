#!/bin/bash

# Create the customers table in PostgreSQL
echo "Creating customers table in PostgreSQL..."

docker exec -i postgres psql -U postgres -d inventory << 'SQL'
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data if table is empty
INSERT INTO customers (first_name, last_name, email)
SELECT 'John', 'Doe', 'john@example.com'
WHERE NOT EXISTS (SELECT 1 FROM customers LIMIT 1);

INSERT INTO customers (first_name, last_name, email)
SELECT 'Jane', 'Smith', 'jane@example.com'
WHERE NOT EXISTS (SELECT 1 FROM customers WHERE id = 2);

INSERT INTO customers (first_name, last_name, email)
SELECT 'Bob', 'Johnson', 'bob@example.com'
WHERE NOT EXISTS (SELECT 1 FROM customers WHERE id = 3);
SQL

if [ $? -eq 0 ]; then
    echo "✅ PostgreSQL setup completed successfully"
else
    echo "❌ Error setting up PostgreSQL"
    exit 1
fi

# Show current data
echo -e "\nCurrent data in customers table:"
docker exec -i postgres psql -U postgres -d inventory -c "SELECT * FROM customers;"
