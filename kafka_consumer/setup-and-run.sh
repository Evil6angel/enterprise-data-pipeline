#!/bin/bash

echo "Setting up Kafka consumer environment..."

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing required packages..."
pip install -r requirements.txt

# Run the Kafka consumer
echo "Starting Kafka consumer..."
python kafka_to_hdfs.py
