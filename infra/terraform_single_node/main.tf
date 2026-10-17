###############################################################
# main.tf — DVD Rental Streaming Infrastructure
# Provisions: VPC, Security Groups, 3 EC2 instances, 1 RDS
###############################################################

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

###############################################################
# DATA SOURCES
###############################################################

# Fetch the latest Amazon Linux 2023 AMI automatically
data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

###############################################################
# VPC & NETWORKING
###############################################################

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name    = "${var.project_name}-vpc"
    Project = var.project_name
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name    = "${var.project_name}-igw"
    Project = var.project_name
  }
}

# Public subnet — EC2 instances will live here
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true

  tags = {
    Name    = "${var.project_name}-public-subnet"
    Project = var.project_name
  }
}

# Private subnet — RDS lives here (no direct internet access)
resource "aws_subnet" "private_a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = "${var.aws_region}a"

  tags = {
    Name    = "${var.project_name}-private-subnet-a"
    Project = var.project_name
  }
}

# RDS requires subnets in at least 2 AZs
resource "aws_subnet" "private_b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.3.0/24"
  availability_zone = "${var.aws_region}b"

  tags = {
    Name    = "${var.project_name}-private-subnet-b"
    Project = var.project_name
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name    = "${var.project_name}-public-rt"
    Project = var.project_name
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

###############################################################
# SECURITY GROUPS
###############################################################

# ----- Kafka EC2 Security Group -----
# Ports:
#   9092  — Kafka broker (accessed by Debezium & Hudi Streamer)
#   2181  — Zookeeper (internal to the instance, also opened within VPC)
#   22    — SSH for management
resource "aws_security_group" "kafka" {
  name        = "${var.project_name}-kafka-sg"
  description = "Security group for Kafka broker EC2"
  vpc_id      = aws_vpc.main.id

  # SSH from your IP only
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  # Kafka broker port — allow all instances within the VPC
  ingress {
    description = "Kafka broker"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  # Zookeeper — within VPC only
  ingress {
    description = "Zookeeper"
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${var.project_name}-kafka-sg"
    Project = var.project_name
  }
}

# ----- Debezium EC2 Security Group -----
resource "aws_security_group" "debezium" {
  name        = "${var.project_name}-debezium-sg"
  description = "Security group for Debezium EC2"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  ingress {
    description = "Debezium Kafka Connect API"
    from_port   = 8083
    to_port     = 8083
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block, var.admin_cidr]
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${var.project_name}-debezium-sg"
    Project = var.project_name
  }
}

# ----- Airflow Orchestrator EC2 Security Group -----
resource "aws_security_group" "airflow" {
  name        = "${var.project_name}-airflow-sg"
  description = "Security group for dedicated Airflow EC2"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  ingress {
    description = "Airflow Webserver UI"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${var.project_name}-airflow-sg"
    Project = var.project_name
  }
}

# ----- Hudi Streamer EC2 Security Group -----
# Hudi Streamer is a Spark consumer — it only needs outbound access.
# It reads from Kafka (9092) and writes to S3 — no inbound ports needed
# beyond SSH for management.
resource "aws_security_group" "hudi_streamer" {
  name        = "${var.project_name}-hudi-streamer-sg"
  description = "Security group for Hudi Streamer EC2"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  egress {
    description = "Allow all outbound (Kafka, S3, AWS APIs)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${var.project_name}-hudi-streamer-sg"
    Project = var.project_name
  }
}

# ----- RDS Security Group -----
# Port 5432 — only accessible from within the VPC (EC2s)
resource "aws_security_group" "rds" {
  name        = "${var.project_name}-rds-sg"
  description = "Security group for PostgreSQL RDS"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "PostgreSQL from VPC"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${var.project_name}-rds-sg"
    Project = var.project_name
  }
}

###############################################################
# IAM ROLE FOR EC2 (S3 Access for Hudi Streamer)
###############################################################

resource "aws_iam_role" "ec2_role" {
  name = "${var.project_name}-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })

  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_role_policy_attachment" "s3_full_access" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_access" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
}

resource "aws_iam_role_policy_attachment" "athena_access" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = "${var.project_name}-ec2-profile"
  role = aws_iam_role.ec2_role.name
}

# Secrets Manager for Airflow Sensitive Config
resource "aws_secretsmanager_secret" "airflow_config" {
  name                    = "${var.project_name}/airflow_config"
  description             = "Airflow credentials and integration secrets"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "airflow_config" {
  secret_id = aws_secretsmanager_secret.airflow_config.id
  secret_string = jsonencode({
    airflow_admin_username = var.airflow_admin_username
    airflow_admin_password = var.airflow_admin_password
    slack_webhook_url      = var.slack_webhook_url
  })
}

resource "aws_iam_policy" "secrets_read" {
  name        = "${var.project_name}-secrets-read"
  description = "Allow EC2 to read Airflow secrets from Secrets Manager"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = [
          aws_secretsmanager_secret.airflow_config.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "secrets_access" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = aws_iam_policy.secrets_read.arn
}

###############################################################
# EC2 KEY PAIR
###############################################################

resource "aws_key_pair" "dvdrental" {
  key_name   = "${var.project_name}-key"
  public_key = var.ssh_public_key

  tags = {
    Project = var.project_name
  }
}

###############################################################
# EC2 INSTANCES
###############################################################

# User data script: installs Docker & Docker Compose on Amazon Linux 2023
locals {
  docker_install_script = <<-EOF
    #!/bin/bash
    dnf update -y
    dnf install -y docker git
    systemctl enable docker
    systemctl start docker
    usermod -aG docker ec2-user
    curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
    ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
  EOF
}

# --- Instance 1: Kafka Zookeeper/Controller ---
resource "aws_instance" "kafka_controller" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.kafka.id]
  key_name               = aws_key_pair.dvdrental.key_name
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  user_data = local.docker_install_script

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    delete_on_termination = true
  }

  tags = {
    Name    = "${var.project_name}-kafka-controller"
    Role    = "kafka-controller"
    Project = var.project_name
  }
}

# --- Instance 1.1: Kafka Brokers (Workers) ---
resource "aws_instance" "kafka_broker" {
  count                  = 3
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.kafka.id]
  key_name               = aws_key_pair.dvdrental.key_name
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  user_data = local.docker_install_script

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    delete_on_termination = true
  }

  tags = {
    Name    = "${var.project_name}-kafka-broker-${count.index + 1}"
    Role    = "kafka-broker"
    Project = var.project_name
  }
}

# --- Instance 2: Debezium ---
resource "aws_instance" "debezium" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.debezium.id]
  key_name               = aws_key_pair.dvdrental.key_name
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  user_data = local.docker_install_script

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    delete_on_termination = true
  }

  tags = {
    Name    = "${var.project_name}-debezium"
    Role    = "debezium"
    Project = var.project_name
  }
}

# --- Instance 2.1: Airflow Orchestrator ---
resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.airflow.id]
  key_name               = aws_key_pair.dvdrental.key_name
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  user_data = <<-EOF
    #!/bin/bash
    set -euo pipefail
    exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

    dnf update -y
    dnf install -y docker git aws-cli jq
    systemctl enable docker
    systemctl start docker
    usermod -aG docker ec2-user
    curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose

    echo "[*] Cloning dvdrental repository..."
    APP_DIR="/home/ec2-user/dvdrental_streaming_data"
    if [ ! -d "$APP_DIR" ]; then
      git clone https://github.com/Dungpham0703/dvdrental_streaming_data.git "$APP_DIR"
    else
      cd "$APP_DIR" && git pull origin main
    fi

    echo "[*] Configuring Airflow environment..."
    mkdir -p /home/ec2-user/.aws "$APP_DIR/pipelines/airflow/logs" "$APP_DIR/pipelines/airflow/plugins"

    echo "[*] Fetching Airflow secrets from Secrets Manager..."
    AIRFLOW_SECRET=$(aws secretsmanager get-secret-value \
      --secret-id ${aws_secretsmanager_secret.airflow_config.name} \
      --region ${var.aws_region} \
      --query SecretString --output text)

    AIRFLOW_USER=$(echo "$AIRFLOW_SECRET" | jq -r '.airflow_admin_username')
    AIRFLOW_PASS=$(echo "$AIRFLOW_SECRET" | jq -r '.airflow_admin_password')
    SLACK_URL=$(echo "$AIRFLOW_SECRET" | jq -r '.slack_webhook_url')

    cat > "$APP_DIR/pipelines/airflow/.env" <<ENVEOF
AIRFLOW_UID=50000
AWS_REGION=${var.aws_region}
ATHENA_S3_OUTPUT=s3://${aws_s3_bucket.data_lake.bucket}/bronze/athena-results/
_AIRFLOW_WWW_USER_USERNAME=$AIRFLOW_USER
_AIRFLOW_WWW_USER_PASSWORD=$AIRFLOW_PASS
AIRFLOW__AWS__S3_BUCKET_NAME=s3://${aws_s3_bucket.data_lake.bucket}/bronze/athena-results/
SLACK_WEBHOOK_URL=$SLACK_URL
ENVEOF

    chown -R ec2-user:ec2-user "$APP_DIR" /home/ec2-user/.aws
    chmod -R 777 "$APP_DIR/pipelines/airflow/logs" "$APP_DIR/pipelines/airflow/plugins"

    echo "[*] Building Airflow image..."
    export HOME=/home/ec2-user
    cd "$APP_DIR/pipelines/airflow"
    docker build -t ghcr.io/dungpham0703/dvdrental_streaming_data/airflow:latest .

    echo "[*] Starting Airflow services via Docker Compose..."
    docker compose up -d
  EOF

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    delete_on_termination = true
  }

  tags = {
    Name    = "${var.project_name}-airflow"
    Role    = "airflow"
    Project = var.project_name
  }
}

# --- Instance 3: Hudi Streamer (Master) ---
resource "aws_instance" "hudi_master" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.hudi_streamer.id]
  key_name               = aws_key_pair.dvdrental.key_name
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  user_data = local.docker_install_script

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    delete_on_termination = true
  }

  tags = {
    Name    = "${var.project_name}-hudi-master"
    Role    = "hudi-master"
    Project = var.project_name
  }
}

# --- Instance 3.1: Hudi Streamer (Workers) ---
resource "aws_instance" "hudi_worker" {
  count                  = 3
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.hudi_streamer.id]
  key_name               = aws_key_pair.dvdrental.key_name
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  user_data = local.docker_install_script

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    delete_on_termination = true
  }

  tags = {
    Name    = "${var.project_name}-hudi-worker-${count.index + 1}"
    Role    = "hudi-worker"
    Project = var.project_name
  }
}

###############################################################
# RDS — PostgreSQL with Logical Replication
###############################################################

resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-db-subnet-group"
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]

  tags = {
    Name    = "${var.project_name}-db-subnet-group"
    Project = var.project_name
  }
}

# Parameter group to enable logical replication (wal_level = logical)
# This is required by Debezium CDC to read WAL logs from Postgres.
resource "aws_db_parameter_group" "postgres_logical" {
  name        = "${var.project_name}-pg-logical"
  family      = "postgres16"
  description = "Enable logical replication for Debezium CDC"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  tags = {
    Name    = "${var.project_name}-pg-logical"
    Project = var.project_name
  }
}

resource "aws_db_instance" "postgres" {
  identifier        = "${var.project_name}-db"
  engine            = "postgres"
  engine_version    = "16"
  instance_class    = var.db_instance_class
  allocated_storage = 20
  storage_type      = "gp3"

  db_name  = var.db_name
  username = var.db_username
  password = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  parameter_group_name   = aws_db_parameter_group.postgres_logical.name

  publicly_accessible     = false
  skip_final_snapshot     = true
  deletion_protection     = false
  backup_retention_period = 7

  tags = {
    Name    = "${var.project_name}-postgres"
    Project = var.project_name
  }
}

###############################################################
# S3 BUCKET (Data Lake for Hudi Streamer)
###############################################################

resource "aws_s3_bucket" "data_lake" {
  bucket        = var.s3_bucket_name
  force_destroy = true

  tags = {
    Name    = var.s3_bucket_name
    Project = var.project_name
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}
