#!/bin/bash
# =============================================
# run_hudi_initial_load_ec2.sh
# AWS Standalone EC2 Edition (Spark Master + Workers).
# Performs full initial snapshot bootstrap for all 15 tables from Kafka to S3 Hudi Lake.
# Runs 2 passes without infinite loop to ensure 100% of historical data is ingested.
# =============================================

set -euo pipefail

echo "============================================================"
echo "[*] DVDRental HoodieMultiTableStreamer - EC2 Initial Load"
echo "============================================================"

export AWS_DEFAULT_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-ap-southeast-2}}"

# === Fetch secrets from AWS Secrets Manager ===
KAFKA_SECRET=$(aws secretsmanager get-secret-value \
    --region "${AWS_DEFAULT_REGION}" \
    --secret-id "dvdrental/kafka_bootstrap_servers" \
    --query SecretString --output text || echo '{}')

S3_SECRET=$(aws secretsmanager get-secret-value \
    --region "${AWS_DEFAULT_REGION}" \
    --secret-id "dvdrental/s3_config" \
    --query SecretString --output text)

# Safely extract
KAFKA_BROKERS=$(echo "$KAFKA_SECRET" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('bootstrap_servers', '172.31.16.100:9092,172.31.16.101:9092,172.31.16.102:9092'))")
S3_BASE_PATH=$(echo "$S3_SECRET"   | python3 -c "import sys,json; print(json.load(sys.stdin)['base_path'])")
S3_BUCKET=$(echo "$S3_SECRET"      | python3 -c "import sys,json; print(json.load(sys.stdin)['bucket_name'])")

echo "[*] S3 base path:  ${S3_BASE_PATH}"
echo "[*] Syncing latest Hudi configs from s3://${S3_BUCKET}/hudi-configs/ to /opt/hudi-configs/..."
aws s3 sync "s3://${S3_BUCKET}/hudi-configs/" /opt/hudi-configs/ --delete || true

# Get Spark Master IP
SPARK_MASTER_IP=$(hostname -I | awk '{print $1}')

# Find the hudi-utilities-bundle JAR
HUDI_JAR=$(ls /opt/hudi/hudi-utilities-bundle_2.12-*.jar \
              /opt/spark/jars/hudi-utilities-bundle_2.12-*.jar \
              /opt/hudi-jars/hudi-utilities-bundle_2.12-*.jar \
           2>/dev/null | head -n 1 || true)

HUDI_SPARK_JAR=$(ls /opt/hudi/hudi-spark3.5-bundle_2.12-*.jar \
                    /opt/spark/jars/hudi-spark3.5-bundle_2.12-*.jar \
                    /opt/hudi-jars/hudi-spark3.5-bundle_2.12-*.jar \
                 2>/dev/null | head -n 1 || true)

HUDI_VERSION="0.15.0"
SCALA_VERSION="2.12"
HUDI_JAR_DIR="/opt/hudi-jars"
if [ -z "${HUDI_JAR}" ]; then
    echo "[*] Hudi utilities bundle not found locally. Fetching from s3://${S3_BUCKET}/hudi-jars/ or maven..."
    mkdir -p "${HUDI_JAR_DIR}"
    aws s3 cp "s3://${S3_BUCKET}/hudi-jars/hudi-utilities-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar" "${HUDI_JAR_DIR}/hudi-utilities-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar" || \
    wget -q "https://repo1.maven.org/maven2/org/apache/hudi/hudi-utilities-bundle_${SCALA_VERSION}/${HUDI_VERSION}/hudi-utilities-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar" -O "${HUDI_JAR_DIR}/hudi-utilities-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"

    aws s3 cp "s3://${S3_BUCKET}/hudi-jars/hudi-spark3.5-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar" "${HUDI_JAR_DIR}/hudi-spark3.5-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar" || \
    wget -q "https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_${SCALA_VERSION}/${HUDI_VERSION}/hudi-spark3.5-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar" -O "${HUDI_JAR_DIR}/hudi-spark3.5-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"

    HUDI_JAR="${HUDI_JAR_DIR}/hudi-utilities-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"
    HUDI_SPARK_JAR="${HUDI_JAR_DIR}/hudi-spark3.5-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"
fi

echo "[*] Using Hudi utilities jar: ${HUDI_JAR}"
echo "[*] Using Hudi Spark jar:     ${HUDI_SPARK_JAR}"

# Bypass Hudi MultiTableStreamer internal dfs-source bug
docker exec spark-master bash -c "mkdir -p /opt/bitnami/spark/src/test/resources/streamer-config"
docker exec spark-master bash -c "touch /opt/bitnami/spark/src/test/resources/streamer-config/dfs-source.properties"

echo ""
echo "[*] Pass 1/2: Ingesting initial historical data from earliest offset..."
echo ""

docker exec spark-master spark-submit \
    --class org.apache.hudi.utilities.streamer.HoodieMultiTableStreamer \
    --master "spark://${SPARK_MASTER_IP}:7077" \
    --deploy-mode client \
    --num-executors 3 \
    --executor-cores 2 \
    --executor-memory 6g \
    --driver-memory 4g \
    --jars "${HUDI_SPARK_JAR}" \
    --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.hive.convertMetastoreParquet=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    "${HUDI_JAR}" \
    --config-folder /opt/hudi-configs/tables \
    --base-path-prefix "s3a://${S3_BUCKET}/" \
    --props "s3a://${S3_BUCKET}/hudi-configs/common.properties" \
    --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
    --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
    --table-type MERGE_ON_READ \
    --source-ordering-field __ts_ms

PASS1_EXIT=$?
if [ $PASS1_EXIT -ne 0 ]; then
    echo "[ERROR] Pass 1 ingestion failed with exit code ${PASS1_EXIT}."
    exit $PASS1_EXIT
fi

echo ""
echo "[*] Pass 2/2: Catching any remaining in-flight CDC events..."
echo ""

docker exec spark-master spark-submit \
    --class org.apache.hudi.utilities.streamer.HoodieMultiTableStreamer \
    --master "spark://${SPARK_MASTER_IP}:7077" \
    --deploy-mode client \
    --num-executors 3 \
    --executor-cores 2 \
    --executor-memory 6g \
    --driver-memory 4g \
    --jars "${HUDI_SPARK_JAR}" \
    --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.hive.convertMetastoreParquet=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    "${HUDI_JAR}" \
    --config-folder /opt/hudi-configs/tables \
    --base-path-prefix "s3a://${S3_BUCKET}/" \
    --props "s3a://${S3_BUCKET}/hudi-configs/common.properties" \
    --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
    --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
    --table-type MERGE_ON_READ \
    --source-ordering-field __ts_ms

echo ""
echo "============================================================"
echo "[SUCCESS] Initial load completed successfully!"
echo "All 15 tables bootstrapped to s3a://${S3_BUCKET}/default/"
echo "You can now safely run 'run_hudi_streamer_ec2.sh' for ongoing CDC."
echo "============================================================"
