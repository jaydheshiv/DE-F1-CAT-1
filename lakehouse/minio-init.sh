#!/bin/bash
# MinIO Initialization Script
# Creates required buckets for the lakehouse architecture
set -e

echo "Waiting for MinIO to be ready..."
sleep 5

# Configure MinIO client alias
mc alias set myminio http://minio:9000 minioadmin minioadmin

# Create buckets
mc mb --ignore-existing myminio/warehouse
mc mb --ignore-existing myminio/lakehouse

# Set bucket policies (allow read access for Trino/Spark)
mc anonymous set download myminio/warehouse
mc anonymous set download myminio/lakehouse

echo "MinIO buckets created:"
mc ls myminio/
echo "MinIO initialization complete!"
