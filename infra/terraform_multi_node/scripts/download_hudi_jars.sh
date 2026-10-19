#!/bin/bash
# =============================================
# download_hudi_jars.sh
# Run ONCE from your local machine to download
# Hudi JARs from Maven Central and upload to S3.
# EC2 nodes will then pull from S3 (no internet needed).
# =============================================

set -euo pipefail

HUDI_VERSION="0.15.0"
SCALA_VERSION="2.12"
SPARK_VERSION="3.5"
KAFKA_SPARK_VERSION="3.5.0"
HADOOP_AWS_VERSION="3.3.4"
AWS_SDK_VERSION="1.12.261"
S3_BUCKET="${1:-dvdrental-hudi-lake-dzung-2026}"
LOCAL_DIR="/tmp/hudi-jars-download"

echo "================================================"
echo "[*] Downloading Hudi JARs to ${LOCAL_DIR}..."
echo "================================================"
mkdir -p "${LOCAL_DIR}"

MAVEN="https://repo1.maven.org/maven2"

declare -A JARS=(
    ["hudi-utilities-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"]="${MAVEN}/org/apache/hudi/hudi-utilities-bundle_${SCALA_VERSION}/${HUDI_VERSION}/hudi-utilities-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"
    ["hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"]="${MAVEN}/org/apache/hudi/hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}/${HUDI_VERSION}/hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"
    ["spark-sql-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar"]="${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${KAFKA_SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar"
    ["hadoop-aws-${HADOOP_AWS_VERSION}.jar"]="${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
    ["aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"]="${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"
)

for JAR_NAME in "${!JARS[@]}"; do
    JAR_URL="${JARS[$JAR_NAME]}"
    LOCAL_PATH="${LOCAL_DIR}/${JAR_NAME}"
    if [ -f "${LOCAL_PATH}" ]; then
        echo "  [skip] ${JAR_NAME} already downloaded."
    else
        echo "  [download] ${JAR_NAME}..."
        wget -q --show-progress -O "${LOCAL_PATH}" "${JAR_URL}"
    fi
done

echo ""
echo "[*] Uploading JARs to s3://${S3_BUCKET}/hudi-jars/..."
aws s3 sync "${LOCAL_DIR}/" "s3://${S3_BUCKET}/hudi-jars/" \
    --exclude "*" \
    --include "*.jar"

echo ""
echo "[+] Done! JARs uploaded to s3://${S3_BUCKET}/hudi-jars/"
echo "    EC2 nodes will pull JARs on first run of run_hudi_streamer_ec2.sh"
