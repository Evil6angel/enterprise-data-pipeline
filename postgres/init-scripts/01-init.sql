-- Create inventory database
CREATE SCHEMA inventory;
SET search_path TO inventory;

-- Create customers table
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

-- Create order_items table
CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(id)
);

-- Insert some sample data
INSERT INTO customers (first_name, last_name, email)
VALUES 
    ('John', 'Doe', 'john.doe@example.com'),
    ('Jane', 'Smith', 'jane.smith@example.com'),
    ('Bob', 'Johnson', 'bob.johnson@example.com');

INSERT INTO orders (customer_id, total_amount, status)
VALUES 
    (1, 100.00, 'COMPLETED'),
    (2, 150.00, 'PENDING'),
    (3, 200.00, 'PROCESSING');

INSERT INTO order_items (order_id, product_name, quantity, price)
VALUES 
    (1, 'Widget A', 2, 50.00),
    (2, 'Widget B', 3, 50.00),
    (3, 'Widget C', 4, 50.00);

-- Create publication for CDC
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- Alter tables to enable replication identity
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
