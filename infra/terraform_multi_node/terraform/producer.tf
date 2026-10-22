# =============================================================================
# Producer + Airflow Orchestrator Node
# =============================================================================

resource "aws_instance" "producer" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type_cluster
  subnet_id              = aws_subnet.main.id
  private_ip             = cidrhost(aws_subnet.main.cidr_block, 120)
  vpc_security_group_ids = [aws_security_group.producer_airflow.id]
  iam_instance_profile   = aws_iam_instance_profile.producer_profile.name
  key_name               = var.key_name

  root_block_device {
    volume_size           = 100
    volume_type           = "gp3"
    delete_on_termination = true
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "optional"
    http_put_response_hop_limit = 2
  }

  user_data = templatefile("${path.module}/templates/producer_user_data.sh.tftpl", {
    enable_postgres_and_debezium = var.enable_postgres_and_debezium
    db_address                   = length(aws_db_instance.postgres) > 0 ? aws_db_instance.postgres[0].address : ""
    db_username                  = var.db_username
    db_password                  = var.db_password
    db_name                      = var.db_name
    kafka_brokers                = join(",", [for inst in aws_instance.kafka : "${inst.private_ip}:9092"])
    aws_region                   = data.aws_region.current.name
    s3_bucket                    = data.aws_s3_bucket.hudi_data.bucket
  })

  tags = {
    Name = "Control-Node-Producer-Airflow-Debezium"
  }
}
