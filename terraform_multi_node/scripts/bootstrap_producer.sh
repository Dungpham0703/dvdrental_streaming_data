#!/bin/bash
# =============================================
# bootstrap_producer.sh
# One-time setup script for EC2 Producer Node.
# Run after connecting via SSM Session Manager or SSH to the t3.micro node.
# =============================================

set -e

echo "[1/4] Installing Python dependencies..."
sudo dnf install -y python3-pip

pip3 install \
    kafka-python \
    boto3

echo "[2/4] Fetching producer script from S3..."
S3_BUCKET=$(aws secretsmanager get-secret-value \
    --secret-id "dvdrental/s3_config" \
    --query SecretString --output text | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['bucket_name'])")

aws s3 cp "s3://${S3_BUCKET}/scripts/producer.py" ~/producer.py

echo "[3/4] Script ready! Execute with custom settings:"
echo "  WORKERS=8 TARGET_MESSAGES=100000000 python3 ~/producer.py"
echo ""
echo "[4/4] Or run immediately with defaults (4 workers, 100M messages):"
echo "  python3 ~/producer.py"
