#!/bin/bash
# Check if network exists
if ! docker network ls | grep -q data_pipeline_network; then
    echo "Creating data_pipeline_network..."
    docker network create data_pipeline_network
else
    echo "Network data_pipeline_network already exists."
fi
