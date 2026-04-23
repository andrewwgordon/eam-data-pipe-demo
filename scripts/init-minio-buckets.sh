#!/usr/bin/env bash
# Create the Iceberg warehouse bucket in MinIO.
# Runs as a one-shot init container after MinIO is healthy.
set -euo pipefail

MINIO_HOST="http://minio:9000"
MINIO_USER="minioadmin"
MINIO_PASS="minioadmin"
BUCKET="warehouse"

echo "Configuring MinIO client..."
mc alias set local "${MINIO_HOST}" "${MINIO_USER}" "${MINIO_PASS}"

echo "Creating bucket: ${BUCKET}"
mc mb --ignore-existing "local/${BUCKET}"

echo "Bucket ready:"
mc ls local/
