variable "airflow_admin_username" {
  description = "Airflow Web UI admin username"
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "airflow_admin_password" {
  description = "Airflow Web UI admin password"
  type        = string
  sensitive   = true
}

variable "slack_webhook_url" {
  description = "Slack incoming webhook URL for Airflow alerting"
  type        = string
  sensitive   = true
}

variable "db_name" {
  description = "PostgreSQL Database Name"
  type        = string
  default     = "dvdrental"
}

variable "db_username" {
  description = "PostgreSQL Master Username"
  type        = string
  default     = "postgres"
}

variable "db_password" {
  description = "PostgreSQL Master Password"
  type        = string
  default     = "postgres123456"
  sensitive   = true
}

variable "db_instance_class" {
  description = "RDS PostgreSQL Instance Class (Free-Tier eligible)"
  type        = string
  default     = "db.t3.micro"
}

variable "enable_postgres_and_debezium" {
  description = "Flag to enable/disable PostgreSQL RDS and Debezium Kafka Connect (default false for direct-to-kafka testing)"
  type        = bool
  default     = false
}
