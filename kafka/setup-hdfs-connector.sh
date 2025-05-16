#!/bin/bash

# Function to log steps
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Set paths
CONNECTOR_PATH="/kafka/connect/kafka-connect-hdfs"
DOWNLOAD_DIR="/tmp/kafka-connect-hdfs"
CONNECTOR_URL="https://github.com/confluentinc/kafka-connect-hdfs/releases/download/v10.0.9/confluentinc-kafka-connect-hdfs-10.0.9.zip"
ZIP_FILE="/tmp/hdfs-connector.zip"

# Create directories
log "Creating connector directories..."
mkdir -p "$CONNECTOR_PATH" "$DOWNLOAD_DIR"

# Download the connector zip
log "Downloading HDFS connector..."
if curl -sL --output "$ZIP_FILE" "$CONNECTOR_URL"; then
  log "Download successful!"
else
  log "ERROR: Download failed. Trying alternative URL..."
  # Try the Confluent archive URL
  CONNECTOR_URL="https://packages.confluent.io/archive/7.4/confluent-community-7.4.0.tar.gz"
  if curl -sL --output "$ZIP_FILE" "$CONNECTOR_URL"; then
    log "Alternative download successful!"
    log "Extracting Confluent archive (larger file)..."
    cd /tmp
    tar xzf "$ZIP_FILE"
    # Find and copy the HDFS connector from the extracted archive
    find /tmp/confluent* -name "kafka-connect-hdfs*" -type d -exec cp -r {}/* "$CONNECTOR_PATH" \;
    log "HDFS Connector installed from Confluent archive!"
    # Start the original entrypoint
    log "Starting Kafka Connect..."
    exec /docker-entrypoint.sh "$@"
  else
    log "ERROR: All download attempts failed. Will try to continue anyway..."
    # Start the original entrypoint even though download failed
    exec /docker-entrypoint.sh "$@"
  fi
fi

# If we got here, the direct download worked
if [ -f "$ZIP_FILE" ]; then
  log "Extracting connector..."
  unzip -o "$ZIP_FILE" -d "$DOWNLOAD_DIR"
  cp -r "$DOWNLOAD_DIR"/*/* "$CONNECTOR_PATH/"
  log "HDFS Connector installed!"
fi

# Start the original entrypoint
log "Starting Kafka Connect..."
exec /docker-entrypoint.sh "$@"
