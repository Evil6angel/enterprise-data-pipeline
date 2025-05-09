#!/bin/bash

echo "Setting up test generator environment..."

# Remove existing venv if it exists
if [ -d "venv" ]; then
    echo "Removing existing virtual environment..."
    rm -rf venv
fi

# Create fresh virtual environment
echo "Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing required packages..."
pip install --no-cache-dir -r requirements.txt

# Make data_generator.py executable
chmod +x data_generator.py

# Create a wrapper script that uses the virtual environment
echo '#!/bin/bash' > run_generator.sh
echo 'source venv/bin/activate' >> run_generator.sh
echo 'python data_generator.py "$@"' >> run_generator.sh
chmod +x run_generator.sh

echo "Test generator setup complete!"
echo "Run the generator with: ./run_generator.sh"
