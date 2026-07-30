echo 'Initializing Apache Hudi MultiTableStreamer for all tables...'

# Sanitize properties file to remove CRLF (\r) carriage returns
tr -d '\r' < /opt/hudi-configs/common.properties > /tmp/common.properties
echo "bootstrap.servers=${KAFKA_BROKER_IP}:${KAFKA_PORT}" >> /tmp/common.properties

/opt/spark/bin/spark-submit --class org.apache.hudi.utilities.streamer.HoodieMultiTableStreamer --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.hudi:hudi-utilities-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.261 --master 'local[2]' --driver-memory 2g --executor-memory 2g --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} --conf spark.hadoop.fs.s3a.endpoint=s3.${AWS_REGION}.amazonaws.com --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=true /opt/spark/jars/spark-core_2.12-3.5.0.jar --config-folder-path /opt/hudi-configs/tables --base-path-prefix s3a://${S3_BUCKET_NAME}/bronze/ --props /tmp/common.properties --table-type MERGE_ON_READ --continuous
