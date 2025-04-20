#!/bin/bash

echo "Setting up test generator environment..."

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

echo "Test generator setup complete!"
echo "Run the generator with: ./data_generator.py"
echo "You can set environment variables to control behavior:"
echo "  - NUM_OPERATIONS: Number of operations to perform (default: 50)"
echo "  - OPERATION_DELAY: Delay between operations in seconds (default: 1)"
