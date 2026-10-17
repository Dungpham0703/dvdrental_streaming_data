# =============================================
# iam.tf - IAM Roles & Instance Profiles for EC2 Cluster
# =============================================

data "aws_caller_identity" "current" {}

# 1. Kafka Nodes Profile
resource "aws_iam_role" "kafka_role" {
  name = "Kafka_EC2_Role_${random_id.bucket_suffix.hex}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "kafka_ssm" {
  role       = aws_iam_role.kafka_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "kafka_profile" {
  name = "Kafka_EC2_Profile_${random_id.bucket_suffix.hex}"
  role = aws_iam_role.kafka_role.name
}

# 2. Spark Nodes Profile (Master & Workers)
resource "aws_iam_role" "spark_role" {
  name = "Spark_EC2_Role_${random_id.bucket_suffix.hex}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "spark_ssm" {
  role       = aws_iam_role.spark_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_policy" "spark_s3_custom" {
  name        = "Spark_S3_Access_${random_id.bucket_suffix.hex}"
  description = "Allows full S3 CRUD on dvd-rental-lakehouse"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Effect = "Allow"
        Resource = [
          "arn:aws:s3:::dvd-rental-lakehouse",
          "arn:aws:s3:::dvd-rental-lakehouse/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "spark_s3_attach" {
  role       = aws_iam_role.spark_role.name
  policy_arn = aws_iam_policy.spark_s3_custom.arn
}

resource "aws_iam_instance_profile" "spark_profile" {
  name = "Spark_EC2_Profile_${random_id.bucket_suffix.hex}"
  role = aws_iam_role.spark_role.name
}

# 3. Producer Node Profile
resource "aws_iam_role" "producer_role" {
  name = "Kafka_Producer_Role_${random_id.bucket_suffix.hex}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "producer_ssm" {
  role       = aws_iam_role.producer_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# Attach same S3 Custom policy to Producer
resource "aws_iam_role_policy_attachment" "producer_s3_attach" {
  role       = aws_iam_role.producer_role.name
  policy_arn = aws_iam_policy.spark_s3_custom.arn
}

resource "aws_iam_policy" "producer_secrets" {
  name        = "Producer_Secrets_Access_${random_id.bucket_suffix.hex}"
  description = "Allows Producer to read dvdrental secrets"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:dvdrental/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "producer_secrets_attach" {
  role       = aws_iam_role.producer_role.name
  policy_arn = aws_iam_policy.producer_secrets.arn
}

resource "aws_iam_instance_profile" "producer_profile" {
  name = "Kafka_Producer_Profile_${random_id.bucket_suffix.hex}"
  role = aws_iam_role.producer_role.name
}

# 4. Airflow Orchestrator Node Profile
resource "aws_iam_role" "airflow_role" {
  name = "Airflow_EC2_Role_${random_id.bucket_suffix.hex}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_ssm" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "airflow_s3_attach" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.spark_s3_custom.arn
}

resource "aws_iam_role_policy_attachment" "airflow_secrets_attach" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.producer_secrets.arn
}

resource "aws_iam_policy" "airflow_athena_glue" {
  name        = "Airflow_Athena_Glue_Access_${random_id.bucket_suffix.hex}"
  description = "Allows Airflow to run Athena queries and access Glue Catalog for Iceberg & dbt"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_athena_glue_attach" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.airflow_athena_glue.arn
}

resource "aws_iam_instance_profile" "airflow_profile" {
  name = "Airflow_EC2_Profile_${random_id.bucket_suffix.hex}"
  role = aws_iam_role.airflow_role.name
}
