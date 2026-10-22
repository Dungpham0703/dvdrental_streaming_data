# =============================================================================
# Spark Cluster (1 Master + 3 Workers)
# =============================================================================

resource "aws_instance" "spark_master" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type_cluster
  subnet_id              = aws_subnet.main.id
  private_ip             = cidrhost(aws_subnet.main.cidr_block, 110)
  vpc_security_group_ids = [aws_security_group.allow_all_internal.id]
  iam_instance_profile   = aws_iam_instance_profile.spark_profile.name
  key_name               = var.key_name

  root_block_device {
    volume_size           = 100
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/templates/spark_master_user_data.sh.tftpl", {
    master_ip = cidrhost(aws_subnet.main.cidr_block, 110)
  })

  tags = {
    Name = "Spark-Master"
  }
}

resource "aws_instance" "spark_worker" {
  count                  = 3
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type_cluster
  subnet_id              = aws_subnet.main.id
  private_ip             = cidrhost(aws_subnet.main.cidr_block, 111 + count.index)
  vpc_security_group_ids = [aws_security_group.allow_all_internal.id]
  iam_instance_profile   = aws_iam_instance_profile.spark_profile.name
  key_name               = var.key_name

  root_block_device {
    volume_size           = 100
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/templates/spark_worker_user_data.sh.tftpl", {
    master_ip = cidrhost(aws_subnet.main.cidr_block, 110)
  })

  tags = {
    Name = "Spark-Worker-${count.index + 1}"
  }
}
