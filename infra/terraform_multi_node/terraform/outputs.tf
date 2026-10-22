# =============================================================================
# Outputs
# =============================================================================

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

output "ssh_producer_command" {
  description = "SSH command to connect to Producer / Airflow node"
  value       = "ssh -i dvdrental-key.pem ubuntu@${aws_instance.producer.public_ip}"
}

output "airflow_web_ui" {
  value = "http://${aws_instance.producer.public_ip}:8080"
}

output "s3_bucket_name" {
  value = data.aws_s3_bucket.hudi_data.bucket
}

output "postgres_endpoint" {
  value = var.enable_postgres_and_debezium ? aws_db_instance.postgres[0].endpoint : null
}

output "postgres_address" {
  value = var.enable_postgres_and_debezium ? aws_db_instance.postgres[0].address : null
}

output "postgres_port" {
  value = var.enable_postgres_and_debezium ? aws_db_instance.postgres[0].port : null
}

output "debezium_private_ip" {
  value = var.enable_postgres_and_debezium ? aws_instance.producer.private_ip : null
}

output "debezium_public_ip" {
  value = var.enable_postgres_and_debezium ? aws_instance.producer.public_ip : null
}

output "debezium_connect_api" {
  value = var.enable_postgres_and_debezium ? "http://${aws_instance.producer.public_ip}:8083" : null
}
