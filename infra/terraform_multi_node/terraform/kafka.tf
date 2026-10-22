# =============================================================================
# Kafka Cluster (3 Nodes)
# =============================================================================

resource "aws_instance" "kafka" {
  count                  = 3
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type_cluster
  subnet_id              = aws_subnet.main.id
  private_ip             = cidrhost(aws_subnet.main.cidr_block, 100 + count.index)
  vpc_security_group_ids = [aws_security_group.allow_all_internal.id]
  iam_instance_profile   = aws_iam_instance_profile.kafka_profile.name
  key_name               = var.key_name

  root_block_device {
    volume_size           = 100
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/templates/kafka_user_data.sh.tftpl", {
    my_ip      = cidrhost(aws_subnet.main.cidr_block, 100 + count.index)
    kafka_1_ip = cidrhost(aws_subnet.main.cidr_block, 100)
    kafka_2_ip = cidrhost(aws_subnet.main.cidr_block, 101)
    kafka_3_ip = cidrhost(aws_subnet.main.cidr_block, 102)
    node_id    = count.index + 1
  })

  tags = {
    Name = "Kafka-Node-${count.index + 1}"
  }
}
