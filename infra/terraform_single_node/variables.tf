###############################################################
# variables.tf — All configurable inputs
###############################################################

variable "aws_region" {
  description = "AWS region to deploy all resources"
  type        = string
  default     = "ap-southeast-2"
}

variable "project_name" {
  description = "Prefix tag applied to all resources. Makes it easy to find everything in AWS Console."
  type        = string
  default     = "dvdrental-streaming"
}

variable "instance_type" {
  description = "EC2 instance type for all 3 servers"
  type        = string
  default     = "m7i-flex.large"
}

variable "admin_cidr" {
  description = "Your personal IP in CIDR notation. Restricts SSH and Airflow UI access to only you."
  type        = string
  default     = "[IP_ADDRESS]"
}

variable "ssh_public_key" {
  description = "Your SSH public key content (e.g. content of ~/.ssh/id_rsa.pub)"
  type        = string
  sensitive   = true
}

# ------ Database Variables ------

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "db_name" {
  description = "PostgreSQL database name"
  type        = string
  default     = "dvdrental"
}

variable "db_username" {
  description = "PostgreSQL master username"
  type        = string
  default     = "postgres"
  sensitive   = true
}

variable "db_password" {
  description = "PostgreSQL master password. Never commit this value — always use terraform.tfvars or env vars."
  type        = string
  sensitive   = true
}

variable "s3_bucket_name" {
  description = "S3 bucket name for Hudi Streamer data lake. Must be globally unique!"
  type        = string
  default     = "dvdrental-hudi-lake-dzung-2026"
}

# ------ Airflow Variables ------
variable "airflow_admin_username" {
  description = "Airflow web UI admin username"
  type        = string
  default     = "s3cret"
}

variable "airflow_admin_password" {
  description = "Airflow web UI admin password"
  type        = string
  sensitive   = true
  default     = "s3cret"
}

variable "slack_webhook_url" {
  description = "Slack webhook URL for Airflow alerts"
  type        = string
  sensitive   = true
  default     = ""
}
