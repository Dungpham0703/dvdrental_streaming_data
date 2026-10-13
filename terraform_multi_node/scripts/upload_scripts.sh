set -e

cd "$(dirname "$0")"

cd ../terraform
BUCKET=$(terraform output -raw s3_bucket_name)
cd - > /dev/null

echo "[*] Uploading scripts to s3://${BUCKET}/scripts/"
aws s3 cp ../producer/producer.py              "s3://${BUCKET}/scripts/producer.py"
aws s3 cp run_hudi_streamer_ec2.sh "s3://${BUCKET}/scripts/run_hudi_streamer_ec2.sh"

echo "[*] Uploading 15 Hudi configs and schemas to S3..."
if [ -d "../hudi_configs/" ]; then
    aws s3 sync ../hudi_configs/ "s3://${BUCKET}/hudi-configs/" --delete
fi