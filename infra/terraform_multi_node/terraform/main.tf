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
  tags                    = { Name = "kafka-hudi-subnet" }
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
    cidr_blocks = ["172.31.16.0/24"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["172.31.16.0/24"]
  }

  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
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
  description = "Security group for combined Producer + Airflow EC2"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Airflow Webserver UI"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
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
    cidr_blocks = ["172.31.16.0/24"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "Producer-Airflow-SG"
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

    echo "[*] Updating system & installing Docker, Compose, Git, AWS CLI, jq..."
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y docker.io docker-compose-v2 git awscli jq python3-pip build-essential
    systemctl enable --now docker
    usermod -aG docker ubuntu

    echo "[*] Installing Python libraries for Producer..."
    pip3 install boto3 kafka-python lz4 --break-system-packages || pip3 install boto3 kafka-python lz4

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
    Name = "Producer-Airflow-Node"
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
