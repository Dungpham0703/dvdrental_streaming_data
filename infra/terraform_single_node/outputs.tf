###############################################################
# outputs.tf — Useful connection info printed after apply
###############################################################

output "kafka_public_ip" {
  description = "Public IP of the Kafka controller EC2 — use this as KAFKA_ADVERTISED_IP in your .env"
  value       = aws_instance.kafka_controller.public_ip
}

output "debezium_public_ip" {
  description = "Public IP of the dedicated Debezium EC2"
  value       = aws_instance.debezium.public_ip
}

output "airflow_public_ip" {
  description = "Public IP of the dedicated Airflow EC2"
  value       = aws_instance.airflow.public_ip
}

output "hudi_master_public_ip" {
  description = "Public IP of the Hudi Master EC2"
  value       = aws_instance.hudi_master.public_ip
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint — use this as DB_HOST in your .env files"
  value       = aws_db_instance.postgres.endpoint
}

output "rds_port" {
  description = "RDS PostgreSQL port"
  value       = aws_db_instance.postgres.port
}

output "s3_bucket_name" {
  description = "S3 data lake bucket name"
  value       = aws_s3_bucket.data_lake.bucket
}

output "airflow_ui_url" {
  description = "Airflow Web UI URL"
  value       = "http://${aws_instance.airflow.public_ip}:8080"
}

output "debezium_api_url" {
  description = "Debezium Kafka Connect REST API URL"
  value       = "http://${aws_instance.debezium.public_ip}:8083"
}

output "ssh_kafka" {
  description = "SSH command to connect to Kafka EC2"
  value       = "ssh -i ~/.ssh/dvdrental-key.pem ec2-user@${aws_instance.kafka_controller.public_ip}"
}

output "ssh_debezium" {
  description = "SSH command to connect to Debezium EC2"
  value       = "ssh -i ~/.ssh/dvdrental-key.pem ec2-user@${aws_instance.debezium.public_ip}"
}

output "ssh_airflow" {
  description = "SSH command to connect to Airflow EC2"
  value       = "ssh -i ~/.ssh/dvdrental-key.pem ec2-user@${aws_instance.airflow.public_ip}"
}

output "ssh_hudi_master" {
  description = "SSH command to connect to Hudi Master EC2"
  value       = "ssh -i ~/.ssh/dvdrental-key.pem ec2-user@${aws_instance.hudi_master.public_ip}"
}
