#!/usr/bin/env python3
import psycopg2
import random
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection settings
db_params = {
    'host': 'postgres',  # Use the docker container name
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'port': 5432
}

# Predefined sample data for generation
first_names = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'William', 'Elizabeth',
              'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica', 'Thomas', 'Sarah', 'Charles', 'Karen']
last_names = ['Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller', 'Wilson', 'Moore', 'Taylor',
             'Anderson', 'Thomas', 'Jackson', 'White', 'Harris', 'Martin', 'Thompson', 'Garcia', 'Martinez', 'Robinson']
products = ['Laptop', 'Smartphone', 'Tablet', 'Monitor', 'Keyboard', 'Mouse', 'Headphones', 'Speakers', 'Webcam',
           'Printer', 'Scanner', 'Router', 'External Hard Drive', 'USB Drive', 'SD Card', 'Power Bank', 'Charger',
           'Camera', 'Game Controller', 'Smart Watch']
order_statuses = ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED']

def get_db_connection():
    """Create a database connection."""
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(**db_params)
            conn.autocommit = True
            logger.info("Database connection established")
            return conn
        except psycopg2.OperationalError as e:
            logger.error(f"Connection failed: {e}")
            retries -= 1
            if retries > 0:
                logger.info(f"Retrying in 5 seconds... ({retries} attempts left)")
                time.sleep(5)
            else:
                logger.error("Maximum retries reached. Exiting.")
                raise
    return None

def insert_customer(cursor):
    """Insert a new customer and return its ID."""
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    email = f"{first_name.lower()}.{last_name.lower()}@example.com"
    
    cursor.execute(
        """
        INSERT INTO inventory.customers (first_name, last_name, email)
        VALUES (%s, %s, %s) RETURNING id
        """,
        (first_name, last_name, email)
    )
    customer_id = cursor.fetchone()[0]
    logger.info(f"Inserted customer ID: {customer_id}")
    return customer_id

def update_customer(cursor, customer_id):
    """Update an existing customer."""
    email = f"updated.{random.randint(1000, 9999)}@example.com"
    
    cursor.execute(
        """
        UPDATE inventory.customers
        SET email = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """,
        (email, customer_id)
    )
    logger.info(f"Updated customer ID: {customer_id}")

def delete_customer(cursor, customer_id):
    """Delete a customer."""
    # First check if there are any orders for this customer
    cursor.execute("SELECT id FROM inventory.orders WHERE customer_id = %s", (customer_id,))
    orders = cursor.fetchall()
    
    # Delete any order items and orders
    for order_id in orders:
        cursor.execute("DELETE FROM inventory.order_items WHERE order_id = %s", (order_id[0],))
        cursor.execute("DELETE FROM inventory.orders WHERE id = %s", (order_id[0],))
    
    # Now delete the customer
    cursor.execute("DELETE FROM inventory.customers WHERE id = %s", (customer_id,))
    logger.info(f"Deleted customer ID: {customer_id}")

def insert_order(cursor, customer_id):
    """Insert a new order and return its ID."""
    total_amount = round(random.uniform(10.0, 500.0), 2)
    status = random.choice(order_statuses)
    
    cursor.execute(
        """
        INSERT INTO inventory.orders (customer_id, total_amount, status)
        VALUES (%s, %s, %s) RETURNING id
        """,
        (customer_id, total_amount, status)
    )
    order_id = cursor.fetchone()[0]
    logger.info(f"Inserted order ID: {order_id}")
    return order_id

def update_order(cursor, order_id):
    """Update an existing order."""
    status = random.choice(order_statuses)
    
    cursor.execute(
        """
        UPDATE inventory.orders
        SET status = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """,
        (status, order_id)
    )
    logger.info(f"Updated order ID: {order_id}")

def delete_order(cursor, order_id):
    """Delete an order and its items."""
    cursor.execute("DELETE FROM inventory.order_items WHERE order_id = %s", (order_id,))
    cursor.execute("DELETE FROM inventory.orders WHERE id = %s", (order_id,))
    logger.info(f"Deleted order ID: {order_id}")

def insert_order_item(cursor, order_id):
    """Insert a new order item."""
    product_name = random.choice(products)
    quantity = random.randint(1, 5)
    price = round(random.uniform(10.0, 200.0), 2)
    
    cursor.execute(
        """
        INSERT INTO inventory.order_items (order_id, product_name, quantity, price)
        VALUES (%s, %s, %s, %s) RETURNING id
        """,
        (order_id, product_name, quantity, price)
    )
    item_id = cursor.fetchone()[0]
    logger.info(f"Inserted order item ID: {item_id}")
    return item_id

def update_order_item(cursor, item_id):
    """Update an existing order item."""
    quantity = random.randint(1, 10)
    
    cursor.execute(
        """
        UPDATE inventory.order_items
        SET quantity = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """,
        (quantity, item_id)
    )
    logger.info(f"Updated order item ID: {item_id}")

def delete_order_item(cursor, item_id):
    """Delete an order item."""
    cursor.execute("DELETE FROM inventory.order_items WHERE id = %s", (item_id,))
    logger.info(f"Deleted order item ID: {item_id}")

def get_existing_ids(cursor):
    """Get lists of existing IDs for customers, orders, and order items."""
    cursor.execute("SELECT id FROM inventory.customers")
    customer_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT id FROM inventory.orders")
    order_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT id FROM inventory.order_items")
    item_ids = [row[0] for row in cursor.fetchall()]
    
    return customer_ids, order_ids, item_ids

def generate_random_data(num_operations=10):
    """Generate random data operations."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get existing IDs
        customer_ids, order_ids, item_ids = get_existing_ids(cursor)
        
        for _ in range(num_operations):
            operation = random.choice(['insert', 'update', 'delete'])
            entity = random.choice(['customer', 'order', 'order_item'])
            
            if operation == 'insert':
                if entity == 'customer':
                    customer_id = insert_customer(cursor)
                    customer_ids.append(customer_id)
                    # Also add an order for this customer
                    if random.random() > 0.3:  # 70% chance to add an order
                        order_id = insert_order(cursor, customer_id)
                        order_ids.append(order_id)
                        # Add 1-3 items to this order
                        for _ in range(random.randint(1, 3)):
                            item_id = insert_order_item(cursor, order_id)
                            item_ids.append(item_id)
                elif entity == 'order' and customer_ids:
                    customer_id = random.choice(customer_ids)
                    order_id = insert_order(cursor, customer_id)
                    order_ids.append(order_id)
                    # Add 1-3 items to this order
                    for _ in range(random.randint(1, 3)):
                        item_id = insert_order_item(cursor, order_id)
                        item_ids.append(item_id)
                elif entity == 'order_item' and order_ids:
                    order_id = random.choice(order_ids)
                    item_id = insert_order_item(cursor, order_id)
                    item_ids.append(item_id)
            
            elif operation == 'update':
                if entity == 'customer' and customer_ids:
                    customer_id = random.choice(customer_ids)
                    update_customer(cursor, customer_id)
                elif entity == 'order' and order_ids:
                    order_id = random.choice(order_ids)
                    update_order(cursor, order_id)
                elif entity == 'order_item' and item_ids:
                    item_id = random.choice(item_ids)
                    update_order_item(cursor, item_id)
            
            elif operation == 'delete':
                if entity == 'customer' and customer_ids:
                    customer_id = random.choice(customer_ids)
                    delete_customer(cursor, customer_id)
                    customer_ids.remove(customer_id)
                elif entity == 'order' and order_ids:
                    order_id = random.choice(order_ids)
                    delete_order(cursor, order_id)
                    order_ids.remove(order_id)
                elif entity == 'order_item' and item_ids:
                    item_id = random.choice(item_ids)
                    delete_order_item(cursor, item_id)
                    item_ids.remove(item_id)
            
            # Add a small delay between operations
            time.sleep(0.5)
        
        cursor.close()
        conn.close()
        logger.info("Data generation completed successfully")
    
    except Exception as e:
        logger.error(f"Error generating data: {e}")
        if 'conn' in locals() and conn is not None:
            conn.close()

if __name__ == "__main__":
    logger.info("Starting data generation")
    # Generate a batch of random operations
    generate_random_data(num_operations=30)
