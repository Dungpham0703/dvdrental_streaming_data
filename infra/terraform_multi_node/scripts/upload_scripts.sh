#!/bin/bash
set -e

cd "$(dirname "$0")"

cd ../terraform
BUCKET=$(terraform output -raw s3_bucket_name 2>/dev/null || true)
if [ -z "$BUCKET" ] || [ "$BUCKET" = "No outputs found" ]; then
    BUCKET=$(grep 'existing_s3_bucket_name' terraform.tfvars 2>/dev/null | cut -d'=' -f2 | tr -d ' "' || echo "dvdrental-hudi-lake-dzung-2026")
    BUCKET=${BUCKET:-"dvdrental-hudi-lake-dzung-2026"}
fi
cd - > /dev/null

echo "[*] Uploading scripts to s3://${BUCKET}/scripts/"
aws s3 cp ../producer/producer.py              "s3://${BUCKET}/scripts/producer.py"
aws s3 cp run_hudi_streamer_ec2.sh "s3://${BUCKET}/scripts/run_hudi_streamer_ec2.sh"

echo "[*] Uploading 15 Hudi configs and schemas to S3..."
if [ -d "../hudi_configs/" ]; then
    aws s3 sync ../hudi_configs/ "s3://${BUCKET}/hudi-configs/" --delete
fi
