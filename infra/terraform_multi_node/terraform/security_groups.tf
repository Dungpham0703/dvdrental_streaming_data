# =============================================================================
# Security Groups
# =============================================================================

resource "aws_security_group" "allow_all_internal" {
  name        = "allow_all_internal_kafka_hudi_${random_id.bucket_suffix.hex}"
  description = "Allow internal traffic and outbound HTTPS"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "SSH from outside"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Spark Master Web UI"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Spark Application Web UI"
    from_port   = 8090
    to_port     = 8090
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

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
