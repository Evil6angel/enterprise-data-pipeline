#!/usr/bin/env python3
import random
import subprocess
import sys
import time

# Names for random data generation
FIRST_NAMES = ["John", "Jane", "Michael", "Emily", "David", "Sarah", "Robert", "Laura"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson"]
EMAIL_DOMAINS = ["example.com", "test.com", "email.com", "company.org"]

def run_command(cmd):
    """Run a command and return its output"""
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return process.stdout, process.stderr, process.returncode

def generate_random_customer():
    """Generate a random customer data"""
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(EMAIL_DOMAINS)}"
    
    return first_name, last_name, email

def insert_customer(first_name, last_name, email):
    """Insert a customer into the database"""
    cmd = f"docker exec postgres psql -U postgres -d inventory -c \"INSERT INTO customers (first_name, last_name, email) VALUES ('{first_name}', '{last_name}', '{email}');\""
    stdout, stderr, returncode = run_command(cmd)
    
    if returncode != 0:
        print(f"Error inserting customer: {stderr}")
        return False
    
    print(f"✅ Inserted customer: {first_name} {last_name}")
    return True

def update_random_customer():
    """Update a random customer in the database"""
    # Get a random customer ID
    cmd = "docker exec postgres psql -U postgres -d inventory -c \"SELECT id FROM customers ORDER BY RANDOM() LIMIT 1;\""
    stdout, stderr, returncode = run_command(cmd)
    
    if returncode != 0 or "id" not in stdout:
        print(f"Error finding customer to update: {stderr}")
        return False
    
    # Extract ID from output like " id \n----\n  3\n(1 row)"
    try:
        id_line = stdout.strip().split('\n')[2]
        customer_id = id_line.strip()
        
        if not customer_id.isdigit():
            print(f"Invalid customer ID: {customer_id}")
            return False
        
        # Update the customer
        new_email = f"updated.{int(time.time())}@example.com"
        update_cmd = f"docker exec postgres psql -U postgres -d inventory -c \"UPDATE customers SET email='{new_email}' WHERE id={customer_id};\""
        update_stdout, update_stderr, update_returncode = run_command(update_cmd)
        
        if update_returncode != 0:
            print(f"Error updating customer: {update_stderr}")
            return False
        
        print(f"✅ Updated customer ID {customer_id} with new email: {new_email}")
        return True
        
    except Exception as e:
        print(f"Error parsing customer ID: {e}")
        return False

def delete_random_customer():
    """Delete a random customer from the database"""
    # Only delete if there are enough customers (>5)
    count_cmd = "docker exec postgres psql -U postgres -d inventory -c \"SELECT COUNT(*) FROM customers;\""
    count_stdout, count_stderr, count_returncode = run_command(count_cmd)
    
    if count_returncode != 0:
        print(f"Error counting customers: {count_stderr}")
        return False
    
    try:
        count_line = count_stdout.strip().split('\n')[2]
        count = int(count_line.strip())
        
        if count <= 5:
            print(f"Not deleting customer - only {count} customers left")
            return True
            
        # Get a random customer ID
        cmd = "docker exec postgres psql -U postgres -d inventory -c \"SELECT id FROM customers ORDER BY RANDOM() LIMIT 1;\""
        stdout, stderr, returncode = run_command(cmd)
        
        if returncode != 0 or "id" not in stdout:
            print(f"Error finding customer to delete: {stderr}")
            return False
        
        # Extract ID from output
        id_line = stdout.strip().split('\n')[2]
        customer_id = id_line.strip()
        
        if not customer_id.isdigit():
            print(f"Invalid customer ID: {customer_id}")
            return False
        
        # Delete the customer
        delete_cmd = f"docker exec postgres psql -U postgres -d inventory -c \"DELETE FROM customers WHERE id={customer_id};\""
        delete_stdout, delete_stderr, delete_returncode = run_command(delete_cmd)
        
        if delete_returncode != 0:
            print(f"Error deleting customer: {delete_stderr}")
            return False
        
        print(f"✅ Deleted customer ID {customer_id}")
        return True
        
    except Exception as e:
        print(f"Error in delete operation: {e}")
        return False

def main():
    """Main function to generate test data"""
    print("\n===== CRUD Test Data Generator =====\n")
    
    # Insert 3 new customers
    for _ in range(3):
        first_name, last_name, email = generate_random_customer()
        insert_customer(first_name, last_name, email)
    
    # Wait a moment
    time.sleep(2)
    
    # Update a random customer
    update_random_customer()
    
    # Wait a moment
    time.sleep(2)
    
    # Sometimes delete a customer
    if random.random() < 0.3:  # 30% chance
        delete_random_customer()
    
    # Show current data
    print("\nCurrent customers in database:")
    show_cmd = "docker exec postgres psql -U postgres -d inventory -c \"SELECT * FROM customers LIMIT 10;\""
    stdout, stderr, returncode = run_command(show_cmd)
    
    if returncode == 0:
        print(stdout)
    else:
        print(f"Error showing customers: {stderr}")
    
    print("\n✅ Test data generation complete!")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
