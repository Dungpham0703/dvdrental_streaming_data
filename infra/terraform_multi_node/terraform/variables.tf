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
