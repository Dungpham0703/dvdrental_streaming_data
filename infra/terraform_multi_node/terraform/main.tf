terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "ap-southeast-2"
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

variable "existing_s3_bucket_name" {
  description = "Existing S3 Bucket Name for Raw Data"
  type        = string
  default     = "dvd-rental-lakehouse"
}

data "aws_s3_bucket" "hudi_data" {
  bucket = var.existing_s3_bucket_name
}

data "aws_region" "current" {}

resource "aws_vpc" "main" {
  cidr_block           = "172.31.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "kafka-hudi-vpc" }
}

resource "aws_subnet" "main" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "172.31.16.0/24"
  map_public_ip_on_launch = true
  availability_zone       = "${data.aws_region.current.name}a"
  tags                    = { Name = "kafka-hudi-subnet-a" }
}

# RDS requires subnets in at least 2 Availability Zones
resource "aws_subnet" "secondary" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "172.31.32.0/24"
  map_public_ip_on_launch = true
  availability_zone       = "${data.aws_region.current.name}b"
  tags                    = { Name = "kafka-hudi-subnet-b" }
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.main.id
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gw.id
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.main.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "secondary" {
  subnet_id      = aws_subnet.secondary.id
  route_table_id = aws_route_table.public.id
}

resource "aws_db_subnet_group" "postgres" {
  name       = "dvdrental-db-subnet-group-${random_id.bucket_suffix.hex}"
  subnet_ids = [aws_subnet.main.id, aws_subnet.secondary.id]

  tags = {
    Name = "dvdrental-db-subnet-group"
  }
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]
}

resource "aws_security_group" "allow_all_internal" {
  name        = "allow_all_internal_kafka_hudi_${random_id.bucket_suffix.hex}"
  description = "Allow internal traffic and outbound HTTPS"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "rds" {
  name        = "rds_sg_${random_id.bucket_suffix.hex}"
  description = "Security group for RDS PostgreSQL"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "PostgreSQL port from VPC and Public"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "RDS-PostgreSQL-SG"
  }
}


data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

variable "instance_type_cluster" {
  description = "Instance type for all nodes"
  type        = string
  default     = "m7i-flex.large"
}

# -----------------------------------------------------------------------------
# Kafka Cluster (3 Nodes)
# -----------------------------------------------------------------------------
resource "aws_instance" "kafka" {
  count                  = 3
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type_cluster
  subnet_id              = aws_subnet.main.id
  private_ip             = cidrhost(aws_subnet.main.cidr_block, 100 + count.index)
  vpc_security_group_ids = [aws_security_group.allow_all_internal.id]
  iam_instance_profile   = aws_iam_instance_profile.kafka_profile.name

  root_block_device {
    volume_size           = 30
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = <<-EOF
    #!/bin/bash
    set -euo pipefail
    exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

    echo "[*] Installing Docker on Ubuntu..."
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y docker.io
    systemctl enable --now docker
    usermod -aG docker ubuntu

    MY_IP="${cidrhost(aws_subnet.main.cidr_block, 100 + count.index)}"
    KAFKA_1_IP="${cidrhost(aws_subnet.main.cidr_block, 100)}"
    KAFKA_2_IP="${cidrhost(aws_subnet.main.cidr_block, 101)}"
    KAFKA_3_IP="${cidrhost(aws_subnet.main.cidr_block, 102)}"
    NODE_ID=$(( ${count.index} + 1 ))

    echo "[*] Running Kafka KRaft via Docker..."
    docker run -d \
      --name kafka \
      --network host \
      --restart always \
      -e KAFKA_NODE_ID=$NODE_ID \
      -e KAFKA_PROCESS_ROLES="broker,controller" \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@$KAFKA_1_IP:9093,2@$KAFKA_2_IP:9093,3@$KAFKA_3_IP:9093" \
      -e KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093" \
      -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://$MY_IP:9092" \
      -e KAFKA_CONTROLLER_LISTENER_NAMES="CONTROLLER" \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT" \
      -e KAFKA_INTER_BROKER_LISTENER_NAME="PLAINTEXT" \
      -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 \
      -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 \
      -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 \
      -e KAFKA_NUM_PARTITIONS=6 \
      -e KAFKA_DEFAULT_REPLICATION_FACTOR=3 \
      -e CLUSTER_ID="MkU3OEVBNTcwNTJENDM2Qk" \
      -e KAFKA_HEAP_OPTS="-Xms3g -Xmx3g" \
      apache/kafka:latest
  EOF

  tags = {
    Name = "Kafka-Node-${count.index + 1}"
  }
}

# -----------------------------------------------------------------------------
# Spark Cluster (1 Master + 3 Workers)
# -----------------------------------------------------------------------------
resource "aws_instance" "spark_master" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type_cluster
  subnet_id              = aws_subnet.main.id
  private_ip             = cidrhost(aws_subnet.main.cidr_block, 110)
  vpc_security_group_ids = [aws_security_group.allow_all_internal.id]
  iam_instance_profile   = aws_iam_instance_profile.spark_profile.name

  root_block_device {
    volume_size           = 30
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = <<-EOF
    #!/bin/bash
    set -euo pipefail
    exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

    echo "[*] Installing Docker, Python & AWS CLI on Ubuntu..."
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y docker.io awscli python3-pip
    pip3 install boto3 kafka-python --break-system-packages || pip3 install boto3 kafka-python
    systemctl enable --now docker
    usermod -aG docker ubuntu

    MASTER_IP="${cidrhost(aws_subnet.main.cidr_block, 110)}"
    mkdir -p /opt/hudi-configs /opt/hudi-jars

    echo "[*] Running Spark Master via Docker..."
    docker run -d \
      --name spark-master \
      --network host \
      --restart always \
      -v /opt/hudi-configs:/opt/hudi-configs \
      -v /opt/hudi-jars:/opt/hudi-jars \
      -v /tmp:/tmp \
      -e SPARK_MODE=master \
      -e SPARK_MASTER_HOST=$MASTER_IP \
      -e SPARK_MASTER_PORT=7077 \
      -e SPARK_MASTER_WEBUI_PORT=8080 \
      bitnamilegacy/spark:3.5.0
  EOF

  tags = {
    Name = "Spark-Master"
  }
}

resource "aws_instance" "spark_worker" {
  count                  = 3
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type_cluster
  subnet_id              = aws_subnet.main.id
  private_ip             = cidrhost(aws_subnet.main.cidr_block, 111 + count.index)
  vpc_security_group_ids = [aws_security_group.allow_all_internal.id]
  iam_instance_profile   = aws_iam_instance_profile.spark_profile.name

  root_block_device {
    volume_size           = 30
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = <<-EOF
    #!/bin/bash
    set -euo pipefail
    exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

    echo "[*] Installing Docker on Ubuntu..."
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y docker.io python3-pip
    pip3 install boto3 kafka-python --break-system-packages || pip3 install boto3 kafka-python
    systemctl enable --now docker
    usermod -aG docker ubuntu

    MASTER_IP="${cidrhost(aws_subnet.main.cidr_block, 110)}"

    echo "[*] Running Spark Worker via Docker..."
    docker run -d \
      --name spark-worker \
      --network host \
      --restart always \
      -v /opt/hudi-configs:/opt/hudi-configs \
      -v /opt/hudi-jars:/opt/hudi-jars \
      -v /tmp:/tmp \
      -e SPARK_MODE=worker \
      -e SPARK_MASTER_URL=spark://$MASTER_IP:7077 \
      -e SPARK_WORKER_MEMORY=6G \
      -e SPARK_WORKER_CORES=2 \
      bitnamilegacy/spark:3.5.0
  EOF

  tags = {
    Name = "Spark-Worker-${count.index + 1}"
  }
}

# -----------------------------------------------------------------------------
# Producer + Airflow Orchestrator Node
# -----------------------------------------------------------------------------
resource "aws_security_group" "producer_airflow" {
  name        = "producer_airflow_sg_${random_id.bucket_suffix.hex}"
  description = "Security group for combined Control Node (Producer + Airflow + Debezium)"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Airflow Webserver UI"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Debezium Kafka Connect API"
    from_port   = 8083
    to_port     = 8083
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block, "0.0.0.0/0"]
  }

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Internal VPC traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "Producer-Airflow-Debezium-SG"
  }
}

resource "aws_instance" "producer" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type_cluster
  subnet_id              = aws_subnet.main.id
  private_ip             = cidrhost(aws_subnet.main.cidr_block, 120)
  vpc_security_group_ids = [aws_security_group.producer_airflow.id]
  iam_instance_profile   = aws_iam_instance_profile.producer_profile.name

  root_block_device {
    volume_size           = 30
    volume_type           = "gp3"
    delete_on_termination = true
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "optional"
    http_put_response_hop_limit = 2
  }

  user_data = <<-EOF
    #!/bin/bash
    set -euo pipefail
    exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

    echo "[*] Updating system & installing Docker, Compose, Git, AWS CLI, jq, postgresql-client, curl..."
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y docker.io docker-compose-v2 git awscli jq curl python3-pip build-essential postgresql-client
    systemctl enable --now docker
    usermod -aG docker ubuntu

    echo "[*] Installing Python libraries for Producer..."
    pip3 install boto3 kafka-python lz4 --break-system-packages || pip3 install boto3 kafka-python lz4

    # 1. Enforce REPLICA IDENTITY FULL on PostgreSQL RDS
    cat > /home/ubuntu/enforce_replica_identity.sh <<'SCRIPT_EOF'
#!/bin/bash
set -euo pipefail
echo "[*] Waiting for PostgreSQL RDS (${aws_db_instance.postgres.address}:5432) to accept connections..."
export PGPASSWORD="${var.db_password}"
MAX_RETRIES=45
COUNT=0
until psql -h "${aws_db_instance.postgres.address}" -p 5432 -U "${var.db_username}" -d "${var.db_name}" -c '\q' >/dev/null 2>&1 || [ $COUNT -eq $MAX_RETRIES ]; do
  echo "Waiting for RDS PostgreSQL to be ready ($COUNT/$MAX_RETRIES)..."
  sleep 10
  COUNT=$((COUNT+1))
done

echo "[*] Enforcing REPLICA IDENTITY FULL on transactional tables for Debezium CDC..."
psql -h "${aws_db_instance.postgres.address}" -p 5432 -U "${var.db_username}" -d "${var.db_name}" <<'SQLEOF'
DO $$
DECLARE
  t text;
  tbls text[] := ARRAY[
    'actor', 'address', 'category', 'city', 'country',
    'customer', 'film', 'film_actor', 'film_category',
    'inventory', 'language', 'payment', 'rental', 'staff', 'store'
  ];
BEGIN
  FOREACH t IN ARRAY tbls LOOP
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = t) THEN
      EXECUTE 'ALTER TABLE public.' || quote_ident(t) || ' REPLICA IDENTITY FULL;';
      RAISE NOTICE 'Configured REPLICA IDENTITY FULL on public.%', t;
    END IF;
  END LOOP;
END $$;
SQLEOF
echo "[*] REPLICA IDENTITY FULL configured successfully."
SCRIPT_EOF

    chmod +x /home/ubuntu/enforce_replica_identity.sh
    chown ubuntu:ubuntu /home/ubuntu/enforce_replica_identity.sh
    /home/ubuntu/enforce_replica_identity.sh || true

    # 2. Start Debezium Kafka Connect container
    echo "[*] Starting Debezium Kafka Connect container..."
    KAFKA_BROKERS="${join(",", [for inst in aws_instance.kafka : "${inst.private_ip}:9092"])}"
    docker run -d \
      --name debezium-connect \
      --network host \
      --restart always \
      -e GROUP_ID=debezium-cluster \
      -e CONFIG_STORAGE_TOPIC=debezium_configs \
      -e OFFSET_STORAGE_TOPIC=debezium_offsets \
      -e STATUS_STORAGE_TOPIC=debezium_statuses \
      -e CONFIG_STORAGE_REPLICATION_FACTOR=1 \
      -e OFFSET_STORAGE_REPLICATION_FACTOR=1 \
      -e STATUS_STORAGE_REPLICATION_FACTOR=1 \
      -e BOOTSTRAP_SERVERS="$KAFKA_BROKERS" \
      -e KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
      -e VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
      -e KEY_CONVERTER_SCHEMAS_ENABLE=false \
      -e VALUE_CONVERTER_SCHEMAS_ENABLE=false \
      -e REST_PORT=8083 \
      debezium/connect:2.5

    # 3. Register Debezium PostgreSQL Connector
    echo "[*] Waiting for Debezium Kafka Connect REST API on port 8083..."
    until curl -s http://127.0.0.1:8083/connectors >/dev/null 2>&1; do
      sleep 5
    done

    echo "[*] Preparing Debezium connector registration payload..."
    cat > /home/ubuntu/register-debezium.json <<'CONEOF'
{
  "name": "dvdrental-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "plugin.name": "pgoutput",
    "database.hostname": "${aws_db_instance.postgres.address}",
    "database.port": "5432",
    "database.user": "${var.db_username}",
    "database.password": "${var.db_password}",
    "database.dbname": "${var.db_name}",
    "database.server.name": "dvdrental",
    "topic.prefix": "cdc.public",
    "schema.include.list": "public",
    "table.include.list": "public.actor,public.address,public.category,public.city,public.country,public.customer,public.film,public.film_actor,public.film_category,public.inventory,public.language,public.payment,public.rental,public.staff,public.store",
    "tombstones.on.delete": "false",
    "decimal.handling.mode": "double"
  }
}
CONEOF
    chown ubuntu:ubuntu /home/ubuntu/register-debezium.json

    curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
      http://127.0.0.1:8083/connectors/ \
      -d @/home/ubuntu/register-debezium.json || true

    # 4. Clone repo and start Airflow
    echo "[*] Cloning dvdrental repository..."
    APP_DIR="/home/ubuntu/dvdrental_streaming_data"
    if [ ! -d "$APP_DIR" ]; then
      git clone https://github.com/Dungpham0703/dvdrental_streaming_data.git "$APP_DIR"
    else
      cd "$APP_DIR" && git pull origin main
    fi

    echo "[*] Configuring Airflow environment..."
    mkdir -p /home/ubuntu/.aws "$APP_DIR/pipelines/airflow/logs" "$APP_DIR/pipelines/airflow/plugins"

    echo "[*] Fetching Airflow secrets from Secrets Manager..."
    AIRFLOW_SECRET=$(aws secretsmanager get-secret-value \
      --secret-id dvdrental/airflow_config \
      --region ${data.aws_region.current.name} \
      --query SecretString --output text)

    AIRFLOW_USER=$(echo "$AIRFLOW_SECRET" | jq -r '.airflow_admin_username')
    AIRFLOW_PASS=$(echo "$AIRFLOW_SECRET" | jq -r '.airflow_admin_password')
    SLACK_URL=$(echo "$AIRFLOW_SECRET" | jq -r '.slack_webhook_url')

    cat > "$APP_DIR/pipelines/airflow/.env" <<ENVEOF
AIRFLOW_UID=50000
AWS_REGION=${data.aws_region.current.name}
ATHENA_S3_OUTPUT=s3://${data.aws_s3_bucket.hudi_data.bucket}/bronze/athena-results/
_AIRFLOW_WWW_USER_USERNAME=$AIRFLOW_USER
_AIRFLOW_WWW_USER_PASSWORD=$AIRFLOW_PASS
AIRFLOW__AWS__S3_BUCKET_NAME=s3://${data.aws_s3_bucket.hudi_data.bucket}/bronze/athena-results/
SLACK_WEBHOOK_URL=$SLACK_URL
ENVEOF

    chown -R ubuntu:ubuntu "$APP_DIR" /home/ubuntu/.aws
    chmod -R 777 "$APP_DIR/pipelines/airflow/logs" "$APP_DIR/pipelines/airflow/plugins"

    echo "[*] Starting Airflow services via Docker Compose..."
    cd "$APP_DIR/pipelines/airflow"
    docker compose build
    docker compose up -d
  EOF

  tags = {
    Name = "Control-Node-Producer-Airflow-Debezium"
  }
}

# -----------------------------------------------------------------------------
# RDS — PostgreSQL with Logical Replication
# -----------------------------------------------------------------------------
resource "aws_db_parameter_group" "postgres_logical" {
  name        = "dvdrental-pg-logical-${random_id.bucket_suffix.hex}"
  family      = "postgres16"
  description = "Enable logical replication for Debezium CDC"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  parameter {
    name         = "max_slot_wal_keep_size"
    value        = "10240"
    apply_method = "immediate"
  }

  parameter {
    name         = "wal_sender_timeout"
    value        = "0"
    apply_method = "immediate"
  }

  tags = {
    Name = "dvdrental-pg-logical"
  }
}

resource "aws_db_instance" "postgres" {
  identifier        = "dvdrental-postgres-${random_id.bucket_suffix.hex}"
  engine            = "postgres"
  engine_version    = "16"
  instance_class    = var.db_instance_class
  allocated_storage = 20
  storage_type      = "gp3"

  db_name  = var.db_name
  username = var.db_username
  password = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.postgres.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  parameter_group_name   = aws_db_parameter_group.postgres_logical.name

  publicly_accessible     = true
  skip_final_snapshot     = true
  deletion_protection     = false
  backup_retention_period = 1 # Required for Free-Tier compliance (cannot exceed 1)
  apply_immediately       = true

  tags = {
    Name = "dvdrental-postgres"
  }
}


# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------
output "kafka_bootstrap_servers" {
  value = join(",", [for inst in aws_instance.kafka : "${inst.private_ip}:9092"])
}

output "spark_master_url" {
  value = "spark://${aws_instance.spark_master.private_ip}:7077"
}

output "spark_master_ip" {
  value = aws_instance.spark_master.private_ip
}

output "producer_ip" {
  value = aws_instance.producer.private_ip
}

output "airflow_private_ip" {
  value = aws_instance.producer.private_ip
}

output "airflow_public_ip" {
  value = aws_instance.producer.public_ip
}

output "airflow_web_ui" {
  value = "http://${aws_instance.producer.public_ip}:8080"
}

output "s3_bucket_name" {
  value = data.aws_s3_bucket.hudi_data.bucket
}

output "postgres_endpoint" {
  value = aws_db_instance.postgres.endpoint
}

output "postgres_address" {
  value = aws_db_instance.postgres.address
}

output "postgres_port" {
  value = aws_db_instance.postgres.port
}

output "debezium_private_ip" {
  value = aws_instance.producer.private_ip
}

output "debezium_public_ip" {
  value = aws_instance.producer.public_ip
}

output "debezium_connect_api" {
  value = "http://${aws_instance.producer.public_ip}:8083"
}
