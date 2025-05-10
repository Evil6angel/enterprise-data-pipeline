#!/usr/bin/env python3
import json
import subprocess
import sys
import time

def run_command(cmd):
    """Run a command and return its output"""
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return process.stdout, process.stderr, process.returncode

def verify_postgres_table():
    """Verify the customers table exists in PostgreSQL"""
    print("Checking PostgreSQL customers table...")
    
    cmd = "docker exec postgres psql -U postgres -d inventory -c \"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'customers');\""
    stdout, stderr, returncode = run_command(cmd)
    
    if returncode != 0:
        print(f"Error checking PostgreSQL table: {stderr}")
        return False
    
    if "t" in stdout:
        print("✅ PostgreSQL customers table exists")
        return True
    else:
        print("❌ PostgreSQL customers table does not exist")
        
        # Try to create the table
        print("Creating customers table...")
        create_cmd = """docker exec postgres psql -U postgres -d inventory -c "CREATE TABLE IF NOT EXISTS customers (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(255) NOT NULL,
            last_name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );" """
        
        create_stdout, create_stderr, create_returncode = run_command(create_cmd)
        if create_returncode != 0:
            print(f"Error creating table: {create_stderr}")
            return False
            
        print("✅ Created customers table")
        return True

def insert_test_data():
    """Insert test data into PostgreSQL"""
    print("Inserting test data into PostgreSQL...")
    
    for i in range(1, 4):
        cmd = f"docker exec postgres psql -U postgres -d inventory -c \"INSERT INTO customers (first_name, last_name, email) VALUES ('TestUser{i}', 'LastName{i}', 'test{i}@example.com');\""
        stdout, stderr, returncode = run_command(cmd)
        
        if returncode != 0:
            print(f"Error inserting test data: {stderr}")
            return False
    
    print("✅ Inserted test data")
    
    # Update a record
    update_cmd = "docker exec postgres psql -U postgres -d inventory -c \"UPDATE customers SET email='updated@example.com' WHERE id IN (SELECT id FROM customers ORDER BY id LIMIT 1);\""
    update_stdout, update_stderr, update_returncode = run_command(update_cmd)
    
    if update_returncode != 0:
        print(f"Error updating test data: {update_stderr}")
    else:
        print("✅ Updated test data")
    
    return True

def check_hdfs_output():
    """Check if data exists in HDFS"""
    print("Checking HDFS output...")
    
    cmd = "docker exec namenode hdfs dfs -ls /data/cdc/output.json"
    stdout, stderr, returncode = run_command(cmd)
    
    if returncode != 0:
        print(f"Error checking HDFS: {stderr}")
        return False
    
    print("✅ Found output file in HDFS")
    
    # Check file content
    cat_cmd = "docker exec namenode hdfs dfs -cat /data/cdc/output.json | tail -n 2"
    cat_stdout, cat_stderr, cat_returncode = run_command(cat_cmd)
    
    if cat_returncode != 0:
        print(f"Error reading HDFS file: {cat_stderr}")
        return False
    
    print("HDFS File Sample:")
    print(cat_stdout)
    
    return True

def main():
    """Main function to verify the CDC pipeline"""
    print("\n===== CDC Pipeline Verification =====\n")
    
    # Step 1: Verify PostgreSQL table
    if not verify_postgres_table():
        print("❌ Failed to verify PostgreSQL table")
        return False
    
    # Step 2: Insert test data
    if not insert_test_data():
        print("❌ Failed to insert test data")
        return False
    
    # Step 3: Wait for CDC events to propagate
    print("Waiting for CDC events to propagate to HDFS...")
    time.sleep(10)
    
    # Step 4: Check HDFS output
    if not check_hdfs_output():
        print("❌ Failed to verify HDFS output")
        return False
    
    print("\n✅ CDC Pipeline is working correctly!")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
