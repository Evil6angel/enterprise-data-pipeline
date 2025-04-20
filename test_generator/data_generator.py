#!/usr/bin/env python3

import psycopg2
import random
import time
import logging
from datetime import datetime
import os
import sys
from faker import Faker

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_generator')

# Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'postgres')

# Initialize faker for generating realistic data
fake = Faker()

def connect_to_db():
    """Connect to PostgreSQL and return connection"""
    try:
        logger.info(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        logger.info("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
        sys.exit(1)

def insert_customer(conn):
    """Insert a new customer record"""
    try:
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.email()
        
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO inventory.customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
            (first_name, last_name, email)
        )
        customer_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        
        logger.info(f"Inserted new customer: {first_name} {last_name} (ID: {customer_id})")
        return customer_id
    except Exception as e:
        logger.error(f"Error inserting customer: {str(e)}")
        conn.rollback()
        return None

def update_customer(conn, customer_id):
    """Update an existing customer record"""
    try:
        # Only update email to keep it simple
        new_email = fake.email()
        
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE inventory.customers SET email = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
            (new_email, customer_id)
        )
        conn.commit()
        cursor.close()
        
        logger.info(f"Updated customer ID {customer_id} with new email: {new_email}")
        return True
    except Exception as e:
        logger.error(f"Error updating customer: {str(e)}")
        conn.rollback()
        return False

def delete_customer(conn, customer_id):
    """Delete a customer record"""
    try:
        cursor = conn.cursor()
        
        # First, delete any related order items and orders (cascade delete isn't enabled)
        cursor.execute("DELETE FROM inventory.order_items WHERE order_id IN (SELECT id FROM inventory.orders WHERE customer_id = %s)", (customer_id,))
        cursor.execute("DELETE FROM inventory.orders WHERE customer_id = %s", (customer_id,))
        
        # Now delete the customer
        cursor.execute("DELETE FROM inventory.customers WHERE id = %s", (customer_id,))
        conn.commit()
        cursor.close()
        
        logger.info(f"Deleted customer ID {customer_id} and related records")
        return True
    except Exception as e:
        logger.error(f"Error deleting customer: {str(e)}")
        conn.rollback()
        return False

def insert_order(conn, customer_id):
    """Insert a new order with items"""
    try:
        # Create order
        total_amount = round(random.uniform(10.0, 500.0), 2)
        status = random.choice(['NEW', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'COMPLETED'])
        
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO inventory.orders (customer_id, total_amount, status) VALUES (%s, %s, %s) RETURNING id",
            (customer_id, total_amount, status)
        )
        order_id = cursor.fetchone()[0]
        
        # Create 1-5 order items
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            product_name = fake.word().capitalize() + ' ' + fake.word().capitalize()
            quantity = random.randint(1, 10)
            price = round(random.uniform(5.0, 100.0), 2)
            
            cursor.execute(
                "INSERT INTO inventory.order_items (order_id, product_name, quantity, price) VALUES (%s, %s, %s, %s)",
                (order_id, product_name, quantity, price)
            )
        
        conn.commit()
        cursor.close()
        
        logger.info(f"Inserted new order ID {order_id} for customer ID {customer_id} with {num_items} items")
        return order_id
    except Exception as e:
        logger.error(f"Error inserting order: {str(e)}")
        conn.rollback()
        return None

def update_order(conn, order_id):
    """Update an existing order"""
    try:
        # Update order status
        new_status = random.choice(['PROCESSING', 'SHIPPED', 'DELIVERED', 'COMPLETED'])
        
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE inventory.orders SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
            (new_status, order_id)
        )
        conn.commit()
        cursor.close()
        
        logger.info(f"Updated order ID {order_id} with new status: {new_status}")
        return True
    except Exception as e:
        logger.error(f"Error updating order: {str(e)}")
        conn.rollback()
        return False

def get_random_customer_id(conn):
    """Get a random customer ID from the database"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM inventory.customers ORDER BY random() LIMIT 1")
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting random customer ID: {str(e)}")
        return None

def get_random_order_id(conn):
    """Get a random order ID from the database"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM inventory.orders ORDER BY random() LIMIT 1")
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting random order ID: {str(e)}")
        return None

def generate_data(num_operations=50, delay=1):
    """Generate data with random operations"""
    conn = connect_to_db()
    
    try:
        # Generate initial data if needed
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM inventory.customers")
        customer_count = cursor.fetchone()[0]
        cursor.close()
        
        if customer_count < 5:
            logger.info("Adding some initial customers...")
            for _ in range(5):
                customer_id = insert_customer(conn)
                if customer_id:
                    insert_order(conn, customer_id)
        
        # Perform random operations
        logger.info(f"Starting to generate {num_operations} random operations...")
        for i in range(num_operations):
            operation = random.choice(['INSERT', 'UPDATE', 'DELETE'])
            
            if operation == 'INSERT':
                # 60% chance to insert a new customer
                if random.random() < 0.6:
                    customer_id = insert_customer(conn)
                    if customer_id and random.random() < 0.8:  # 80% chance to also create an order
                        insert_order(conn, customer_id)
                else:
                    # Insert an order for an existing customer
                    customer_id = get_random_customer_id(conn)
                    if customer_id:
                        insert_order(conn, customer_id)
            
            elif operation == 'UPDATE':
                # 50% chance to update a customer
                if random.random() < 0.5:
                    customer_id = get_random_customer_id(conn)
                    if customer_id:
                        update_customer(conn, customer_id)
                else:
                    # Update an order
                    order_id = get_random_order_id(conn)
                    if order_id:
                        update_order(conn, order_id)
            
            elif operation == 'DELETE':
                # Only delete customers occasionally (10% of operations)
                if random.random() < 0.1:
                    customer_id = get_random_customer_id(conn)
                    if customer_id:
                        delete_customer(conn, customer_id)
            
            logger.info(f"Completed operation {i+1}/{num_operations}")
            time.sleep(delay)
    
    except KeyboardInterrupt:
        logger.info("Data generation interrupted by user")
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    logger.info("Data Generator starting up...")
    num_ops = int(os.getenv('NUM_OPERATIONS', '50'))
    delay_sec = float(os.getenv('OPERATION_DELAY', '1'))
    generate_data(num_ops, delay_sec)
    logger.info("Data generation complete!")
