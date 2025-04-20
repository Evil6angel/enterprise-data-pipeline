# Enterprise Data Pipeline

A comprehensive data pipeline that captures changes from PostgreSQL, streams them through Kafka, and stores them in HDFS, all orchestrated by Airflow.

## Project Overview

This project implements an enterprise-grade data pipeline with the following features:

1. **PostgreSQL with CDC**: Captures data changes using Debezium
2. **Kafka**: Streams data changes in real-time
3. **Hadoop/HDFS**: Stores data in a unified sink file
4. **Airflow**: Orchestrates the entire pipeline
5. **Test Generator**: Automatically generates CRUD operations

## Architecture

![Architecture Diagram](architecture.png)

The pipeline works as follows:

1. **Data Source**: PostgreSQL database with CDC (Change Data Capture) enabled.
2. **CDC Engine**: Debezium connects to PostgreSQL and captures changes.
3. **Message Broker**: Kafka receives the change events from Debezium.
4. **Stream Processing**: Kafka consumer reads the events and processes them.
5. **Data Storage**: Processed data is stored in HDFS in a single sink file.
6. **Orchestration**: Airflow orchestrates the pipeline components and workflow.

## Enterprise Features

- **Monitoring & Logging**: All components emit logs that can be centralized.
- **Error Handling**: Retry mechanisms and error handling throughout the pipeline.
- **Containerization**: All components run in separate Docker containers.
- **Scalability**: Components can be scaled independently.
- **Automation**: Pipeline execution is fully automated via Airflow.
- **Data Quality**: Validation of data through the pipeline.
- **Security**: Basic security measures in place (can be enhanced further).

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Git
- At least 8GB RAM
- At least 100GB free disk space

## Quick Start

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/enterprise-data-pipeline.git
   cd enterprise-data-pipeline
