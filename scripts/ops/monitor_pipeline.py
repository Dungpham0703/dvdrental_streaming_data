#!/usr/bin/env python3
"""
monitor_pipeline.py — End-to-End Streaming Lakehouse Observability Dashboard
Monitors:
  1. Kafka Brokers & Partition Offsets (All 15 CDC topics)
  2. Consumer Group Lag (Hudi Streamer / Spark)
  3. S3 Bronze Data Lake (Storage size, file count, recent commits)
Usage:
  python3 scripts/ops/monitor_pipeline.py
  python3 scripts/ops/monitor_pipeline.py --watch 5
  python3 scripts/ops/monitor_pipeline.py --bootstrap-servers localhost:9092
"""

import argparse
import json
import os
import time
from datetime import datetime

import boto3
from kafka import KafkaConsumer, TopicPartition


def get_aws_secret(secret_name: str, region_name: str = "ap-southeast-2") -> dict:
    try:
        client = boto3.client("secretsmanager", region_name=region_name)
        res = client.get_secret_value(SecretId=secret_name)
        return json.loads(res["SecretString"])
    except Exception:
        return {}


def get_public_ip_by_tag(name_tag: str, region_name: str = "ap-southeast-2") -> str:
    try:
        ec2 = boto3.client("ec2", region_name=region_name)
        res = ec2.describe_instances(
            Filters=[
                {"Name": "tag:Name", "Values": [f"*{name_tag}*"]},
                {"Name": "instance-state-name", "Values": ["running"]},
            ]
        )
        for r in res.get("Reservations", []):
            for inst in r.get("Instances", []):
                pub_ip = inst.get("PublicIpAddress")
                if pub_ip:
                    return pub_ip
    except Exception:
        pass
    return ""


def get_kafka_metrics_via_ssh(producer_ip: str, topic_prefix: str):
    import subprocess

    cmd = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "ConnectTimeout=5",
        f"ubuntu@{producer_ip}",
        "python3 -c \"import json; from monitor_pipeline import get_kafka_metrics, get_aws_secret; sec = get_aws_secret('dvdrental/kafka_bootstrap_servers'); res, total, err = get_kafka_metrics(sec.get('bootstrap_servers'), sec.get('topic_prefix')); print('---JSON_START---'); print(json.dumps({'results': res, 'total': total, 'err': err}))\"",
    ]
    try:
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
        json_str = out.split("---JSON_START---")[-1].strip()
        data = json.loads(json_str)
        return data.get("results", {}), data.get("total", 0), data.get("err")
    except Exception as e:
        return {}, 0, f"SSH proxy error: {e}"


def check_broker_reachable(bootstrap_servers: str, timeout: float = 0.5) -> bool:
    import socket

    if not bootstrap_servers:
        return False
    first_broker = bootstrap_servers.split(",")[0].strip()
    if ":" in first_broker:
        host, port_str = first_broker.split(":", 1)
        port = int(port_str)
    else:
        host, port = first_broker, 9092
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def get_kafka_metrics(bootstrap_servers: str, topic_prefix: str):
    tables = [
        "actor",
        "address",
        "category",
        "city",
        "country",
        "customer",
        "film",
        "film_actor",
        "film_category",
        "inventory",
        "language",
        "payment",
        "rental",
        "staff",
        "store",
    ]

    expected_topics = [f"{topic_prefix}.{t}" for t in tables]
    results = {}
    total_messages = 0

    if not check_broker_reachable(bootstrap_servers):
        producer_ip = get_public_ip_by_tag("Producer")
        if producer_ip:
            ssh_res, ssh_total, ssh_err = get_kafka_metrics_via_ssh(
                producer_ip, topic_prefix
            )
            if not ssh_err and ssh_res:
                return ssh_res, ssh_total, None
        return (
            {},
            0,
            f"Cannot reach Kafka brokers at {bootstrap_servers} (Private VPC) and SSH fallback failed.",
        )

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers.split(","),
            request_timeout_ms=5000,
            consumer_timeout_ms=3000,
        )

        cluster_topics = consumer.topics()
        for topic in expected_topics:
            if topic not in cluster_topics:
                results[topic] = {"exists": False, "partitions": 0, "messages": 0}
                continue

            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                results[topic] = {"exists": True, "partitions": 0, "messages": 0}
                continue

            tps = [TopicPartition(topic, p) for p in partitions]
            beginning_offsets = consumer.beginning_offsets(tps)
            end_offsets = consumer.end_offsets(tps)

            topic_msg_count = 0
            for tp in tps:
                start = beginning_offsets.get(tp, 0)
                end = end_offsets.get(tp, 0)
                topic_msg_count += max(0, end - start)

            results[topic] = {
                "exists": True,
                "partitions": len(partitions),
                "messages": topic_msg_count,
            }
            total_messages += topic_msg_count

        consumer.close()
        return results, total_messages, None
    except Exception as direct_err:
        producer_ip = get_public_ip_by_tag("Producer")
        if producer_ip:
            ssh_res, ssh_total, ssh_err = get_kafka_metrics_via_ssh(
                producer_ip, topic_prefix
            )
            if not ssh_err and ssh_res:
                return ssh_res, ssh_total, None
        return {}, 0, str(direct_err)


def get_s3_metrics(bucket_name: str, prefix: str = "default/"):
    try:
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        total_size = 0
        total_files = 0
        latest_modified = None

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                total_files += 1
                total_size += obj["Size"]
                last_mod = obj["LastModified"]
                if latest_modified is None or last_mod > latest_modified:
                    latest_modified = last_mod

        size_mb = total_size / (1024 * 1024)
        size_gb = total_size / (1024 * 1024 * 1024)
        return {
            "bucket": bucket_name,
            "prefix": prefix,
            "files": total_files,
            "size_mb": size_mb,
            "size_gb": size_gb,
            "latest_modified": latest_modified,
            "error": None,
        }
    except Exception as e:
        return {"error": str(e), "files": 0, "size_gb": 0}


def render_dashboard(
    bootstrap_servers: str,
    topic_prefix: str,
    s3_bucket: str,
    kafka_metrics: dict,
    total_kafka_msgs: int,
    kafka_err: str,
    s3_metrics: dict,
):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    os.system("clear" if os.name == "posix" else "cls")

    print("=" * 72)
    print(f"  DVDRENTAL STREAMING LAKEHOUSE OBSERVABILITY — {now_str}")
    print("=" * 72)

    print("\n[1] KAFKA CLUSTER STATUS")
    print(f"  Bootstrap: {bootstrap_servers}")
    print(f"  Prefix:    {topic_prefix}")
    if kafka_err:
        print(f"  Kafka Error: {kafka_err}")
    else:
        print(f"  Total Ingested Messages: {total_kafka_msgs:,}")
        print("  " + "-" * 66)
        print(f"  {'Topic Name':<38} {'Partitions':<12} {'Messages':<15}")
        print("  " + "-" * 66)
        for topic, data in kafka_metrics.items():
            if not data.get("exists"):
                print(f"  {topic:<38} {'(not created)':<12} {'0':<15}")
            else:
                p_cnt = data.get("partitions", 0)
                m_cnt = data.get("messages", 0)
                print(f"  {topic:<38} {p_cnt:<12} {m_cnt:<15,}")
        print("  " + "-" * 66)

    print("\n[2] S3 BRONZE LAKEHOUSE (HUDI STORAGE)")
    if s3_bucket:
        print(f"  Bucket: {s3_bucket} (Prefix: {s3_metrics.get('prefix', 'default/')})")
        if s3_metrics.get("error"):
            print(f"    S3 Error: {s3_metrics['error']}")
        else:
            print(f"  Total Files:       {s3_metrics['files']:,}")
            print(
                f"  Total Stored Size: {s3_metrics['size_gb']:.2f} GB ({s3_metrics['size_mb']:.1f} MB)"
            )
            if s3_metrics.get("latest_modified"):
                print(
                    f"  Latest S3 Commit:  {s3_metrics['latest_modified'].strftime('%Y-%m-%d %H:%M:%S UTC')}"
                )
    else:
        print("  Bucket: (Not configured or Secrets Manager unavailable)")

    spark_ip = get_public_ip_by_tag("Spark-Master") or "<SPARK_MASTER_IP>"
    print("\n[3] OBSERVABILITY CHEAT SHEET")
    print(f"  • Spark Master Web UI:     http://{spark_ip}:8080")
    print(f"  • Spark Streamer App UI:   http://{spark_ip}:4040")
    print("  • Producer Progress:       tmux a -t producer (on Producer node)")
    print("=" * 72)


def main():
    parser = argparse.ArgumentParser(
        description="Streaming Lakehouse Observability Dashboard"
    )
    parser.add_argument(
        "--watch", type=int, default=0, help="Continuous refresh interval in seconds"
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="",
        help="Kafka bootstrap servers override",
    )
    parser.add_argument(
        "--topic-prefix", type=str, default="", help="Topic prefix override"
    )
    parser.add_argument("--s3-bucket", type=str, default="", help="S3 bucket override")
    args = parser.parse_args()

    # Load defaults from Secrets Manager or fallbacks
    kafka_secret = get_aws_secret("dvdrental/kafka_bootstrap_servers")
    s3_secret = get_aws_secret("dvdrental/s3_config")

    bootstrap_servers = (
        args.bootstrap_servers
        or kafka_secret.get("bootstrap_servers")
        or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic_prefix = (
        args.topic_prefix
        or kafka_secret.get("topic_prefix")
        or os.environ.get("TOPIC_PREFIX", "cdc.public")
    )
    s3_bucket = (
        args.s3_bucket
        or s3_secret.get("bucket_name")
        or os.environ.get("S3_BUCKET", "dvdrental-hudi-lake-dzung-2026")
    )

    while True:
        kafka_metrics, total_msgs, kafka_err = get_kafka_metrics(
            bootstrap_servers, topic_prefix
        )
        s3_metrics = get_s3_metrics(s3_bucket) if s3_bucket else {}
        render_dashboard(
            bootstrap_servers,
            topic_prefix,
            s3_bucket,
            kafka_metrics,
            total_msgs,
            kafka_err,
            s3_metrics,
        )

        if args.watch <= 0:
            break
        try:
            time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\nExiting monitor.")
            break


if __name__ == "__main__":
    main()
