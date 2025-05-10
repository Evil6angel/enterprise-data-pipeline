import psycopg2
import random
import logging
from faker import Faker
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('data_generator')

fake = Faker()

def generate_test_data():
    """Generate test data in PostgreSQL for CDC pipeline testing"""
    logger.info("Starting test data generation")
    
    # Database connection parameters
    conn_params = {
        'dbname': 'inventory',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'postgres',
        'port': '5432'
    }
    
    # Try connecting with retries
    max_retries = 5
    retry_count = 0
    conn = None
    
    while retry_count < max_retries and conn is None:
        try:
            logger.info("Connecting to PostgreSQL...")
            conn = psycopg2.connect(**conn_params)
            logger.info("Connected to PostgreSQL")
        except Exception as e:
            retry_count += 1
            wait_time = 2 ** retry_count  # Exponential backoff
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            logger.info(f"Retrying in {wait_time} seconds (attempt {retry_count}/{max_retries})")
            time.sleep(wait_time)
    
    if conn is None:
        logger.error("Could not connect to PostgreSQL after multiple attempts")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Insert new customers
        for _ in range(3):
            first_name = fake.first_name()
            last_name = fake.last_name()
            email = fake.email()
            
            cursor.execute(
                "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
                (first_name, last_name, email)
            )
            customer_id = cursor.fetchone()[0]
            logger.info(f"Created customer: {first_name} {last_name} (ID: {customer_id})")
            conn.commit()
        
        # Update a customer
        cursor.execute("SELECT id FROM customers ORDER BY RANDOM() LIMIT 1")
        result = cursor.fetchone()
        
        if result:
            customer_id = result[0]
            new_email = fake.email()
            
            cursor.execute(
                "UPDATE customers SET email = %s WHERE id = %s", 
                (new_email, customer_id)
            )
            logger.info(f"Updated customer {customer_id} with new email: {new_email}")
            conn.commit()
        
        # Delete a customer (occasionally)
        if random.random() < 0.3:  # 30% chance to delete
            cursor.execute("SELECT id FROM customers ORDER BY RANDOM() LIMIT 1")
            result = cursor.fetchone()
            
            if result:
                customer_id = result[0]
                cursor.execute("DELETE FROM customers WHERE id = %s", (customer_id,))
                logger.info(f"Deleted customer {customer_id}")
                conn.commit()
        
        cursor.close()
        conn.close()
        logger.info("Test data generation completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error generating test data: {e}")
        if conn:
            conn.close()
        return False

if __name__ == "__main__":
    generate_test_data()