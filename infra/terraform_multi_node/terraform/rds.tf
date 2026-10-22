# =============================================================================
# RDS — PostgreSQL with Logical Replication
# =============================================================================

resource "aws_db_parameter_group" "postgres_logical" {
  count       = var.enable_postgres_and_debezium ? 1 : 0
  name        = "dvdrental-pg-logical-${random_id.bucket_suffix.hex}"
  family      = "postgres16"
  description = "Enable logical replication for Debezium CDC"

  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  parameter {
    name         = "max_slot_wal_keep_size"
    value        = "10240"
    apply_method = "immediate"
  }

  parameter {
    name         = "wal_sender_timeout"
    value        = "0"
    apply_method = "immediate"
  }

  tags = {
    Name = "dvdrental-pg-logical"
  }
}

resource "aws_db_instance" "postgres" {
  count             = var.enable_postgres_and_debezium ? 1 : 0
  identifier        = "dvdrental-postgres-${random_id.bucket_suffix.hex}"
  engine            = "postgres"
  engine_version    = "16"
  instance_class    = var.db_instance_class
  allocated_storage = 20
  storage_type      = "gp3"

  db_name  = var.db_name
  username = var.db_username
  password = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.postgres.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  parameter_group_name   = aws_db_parameter_group.postgres_logical[0].name

  publicly_accessible     = true
  skip_final_snapshot     = true
  deletion_protection     = false
  backup_retention_period = 1 # Required for Free-Tier compliance (cannot exceed 1)
  apply_immediately       = true

  tags = {
    Name = "dvdrental-postgres"
  }
}
