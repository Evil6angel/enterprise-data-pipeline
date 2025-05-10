#!/bin/bash

# Colors for terminal output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Clear terminal
clear

echo -e "${BLUE}===========================================${NC}"
echo -e "${BLUE}  Enterprise-Grade Data Pipeline Demo      ${NC}"
echo -e "${BLUE}===========================================${NC}"

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo -e "${RED}Docker is not running. Please start Docker first.${NC}"
        exit 1
    fi
}

# Create network if it doesn't exist
create_network() {
    if ! docker network ls | grep -q data_pipeline_network; then
        echo -e "${BLUE}Creating data_pipeline_network...${NC}"
        docker network create data_pipeline_network
    fi
}

# Ensure PostgreSQL has the customers table
ensure_postgres_table() {
    echo -e "${BLUE}Ensuring PostgreSQL schema is set up...${NC}"
    docker exec postgres psql -U postgres -d inventory -c "CREATE TABLE IF NOT EXISTS customers (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );" || echo "Could not create table - may already exist"
}

# Initialize core components
initialize_components() {
    echo -e "${BLUE}Starting core services...${NC}"
    docker-compose up -d
    
    # Wait for PostgreSQL to be ready
    echo -e "${BLUE}Waiting for PostgreSQL to be ready...${NC}"
    for i in {1..30}; do
        if docker exec postgres pg_isready -U postgres 2>/dev/null; then
            break
        fi
        echo -n "."
        sleep 2
    done
    echo ""
    
    ensure_postgres_table
}

# Generate test data
generate_test_data() {
    echo -e "${BLUE}Generating test data...${NC}"
    ./test_generator.py
}

# Check pipeline status
check_pipeline() {
    echo -e "${BLUE}Checking pipeline status...${NC}"
    ./verify_cdc.py
}

# Split-screen demo setup
setup_demo_view() {
    echo -e "${BLUE}Setting up demo view...${NC}"
    echo -e "${GREEN}Open two terminal windows to demonstrate the pipeline:${NC}"
    echo -e "${BLUE}Window 1: Monitor logs${NC}"
    echo -e "   Run: ${GREEN}docker-compose logs -f${NC}"
    echo -e "${BLUE}Window 2: Run commands${NC}"
    echo -e "   Run: ${GREEN}./test_generator.py${NC}"
    echo -e "   Run: ${GREEN}docker exec namenode hdfs dfs -cat /data/cdc/output.json | tail${NC}"
}

# Main execution
main() {
    check_docker
    create_network
    initialize_components
    generate_test_data
    check_pipeline
    setup_demo_view
    
    echo -e "\n${GREEN}Demo is ready!${NC}"
    echo -e "${BLUE}===========================================${NC}"
    echo -e "${BLUE}  Access your services at:               ${NC}"
    echo -e "${BLUE}  - PostgreSQL: localhost:5432           ${NC}"
    echo -e "${BLUE}  - Kafka:      localhost:9092           ${NC}"
    echo -e "${BLUE}  - HDFS UI:    http://localhost:9870    ${NC}"
    echo -e "${BLUE}===========================================${NC}"
}

# Run the main function
main
