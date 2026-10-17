# =============================================
# secrets.tf - AWS Secrets Manager Configuration
# Store sensitive configuration values for Kafka and S3.
# Values are automatically injected into EC2 nodes via IAM roles.
# =============================================

# Secret 1: Kafka Bootstrap Servers (populated dynamically from EC2 Kafka instances)
resource "aws_secretsmanager_secret" "kafka_bootstrap" {
  name                    = "dvdrental/kafka_bootstrap_servers"
  description             = "EC2 Kafka bootstrap servers endpoints"
  recovery_window_in_days = 0 # Immediate deletion without 7-day recovery (test environment)
}

resource "aws_secretsmanager_secret_version" "kafka_bootstrap" {
  secret_id = aws_secretsmanager_secret.kafka_bootstrap.id
  secret_string = jsonencode({
    bootstrap_servers = join(",", [for inst in aws_instance.kafka : "${inst.private_ip}:9092"])
    kafka_version     = "3.5.1"
    topic_prefix      = "cdc.public"
  })
}

# Secret 2: S3 Bucket Info & Base Path
resource "aws_secretsmanager_secret" "s3_config" {
  name                    = "dvdrental/s3_config"
  description             = "S3 bucket name and base path prefix for Hudi tables"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "s3_config" {
  secret_id = aws_secretsmanager_secret.s3_config.id
  secret_string = jsonencode({
    bucket_name = data.aws_s3_bucket.hudi_data.bucket
    base_path   = "s3a://${data.aws_s3_bucket.hudi_data.bucket}/bronze/default/"
    region      = data.aws_region.current.name
  })
}

# Grant read access for secrets to Spark, Kafka, and Producer nodes
resource "aws_iam_policy" "read_dvdrental_secrets" {
  name        = "ReadDvdrentalSecrets_${random_id.bucket_suffix.hex}"
  description = "Allow reading dvdrental secrets from Secrets Manager"
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
          aws_secretsmanager_secret.kafka_bootstrap.arn,
          aws_secretsmanager_secret.s3_config.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "spark_read_secrets" {
  role       = aws_iam_role.spark_role.name
  policy_arn = aws_iam_policy.read_dvdrental_secrets.arn
}

resource "aws_iam_role_policy_attachment" "kafka_read_secrets" {
  role       = aws_iam_role.kafka_role.name
  policy_arn = aws_iam_policy.read_dvdrental_secrets.arn
}

resource "aws_iam_role_policy_attachment" "producer_read_secrets" {
  role       = aws_iam_role.producer_role.name
  policy_arn = aws_iam_policy.read_dvdrental_secrets.arn
}

# Outputs for reference
output "secret_kafka_arn" {
  value = aws_secretsmanager_secret.kafka_bootstrap.arn
}

output "secret_s3_arn" {
  value = aws_secretsmanager_secret.s3_config.arn
}
