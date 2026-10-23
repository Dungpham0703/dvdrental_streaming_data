#!/usr/bin/env python3
"""
monitor_pipeline.py — End-to-End Streaming Lakehouse Observability Dashboard
Monitors:
  1. Kafka Brokers & Partition Offsets (LSO, LEO / High Watermark, Message Count)
  2. Consumer Group Lag & Partition Distribution (Hudi Streamer / Spark)
  3. S3 Hudi Lakehouse Timeline (Latest Commit, Checkpoint Offset, Ingestion Lag, Compaction)
  4. Spark Master & Cluster Runtime (Active Workers, Cores, RAM, Streaming Applications)
  5. Architecture & State Management Reference Guide

Usage:
  python3 scripts/ops/monitor_pipeline.py
  python3 scripts/ops/monitor_pipeline.py --watch 5
  python3 scripts/ops/monitor_pipeline.py --table rental
  python3 scripts/ops/monitor_pipeline.py --explain
  python3 scripts/ops/monitor_pipeline.py --bootstrap-servers localhost:9092
"""

import argparse
import json
import os
import re
import socket
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import Any

import boto3
import botocore.exceptions
from botocore.config import Config
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError

# DVD Rental 15 CDC Tables
TABLES = [
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


# =====================================================================
# AWS HELPERS
# =====================================================================


def get_s3_client(region_name: str = "ap-southeast-2"):
    """Returns a boto3 S3 client configured with fast timeouts."""
    cfg = Config(connect_timeout=3, read_timeout=4, retries={"max_attempts": 1})
    return boto3.client("s3", region_name=region_name, config=cfg)


def get_aws_secret(
    secret_name: str, region_name: str = "ap-southeast-2"
) -> dict[str, Any]:
    """Fetches a JSON secret from AWS Secrets Manager."""
    try:
        cfg = Config(connect_timeout=3, read_timeout=3, retries={"max_attempts": 1})
        client = boto3.client("secretsmanager", region_name=region_name, config=cfg)
        res = client.get_secret_value(SecretId=secret_name)
        return json.loads(res.get("SecretString", "{}"))
    except (
        botocore.exceptions.BotoCoreError,
        botocore.exceptions.ClientError,
        json.JSONDecodeError,
    ):
        return {}


def get_instance_ip_by_tag(
    name_tag: str, region_name: str = "ap-southeast-2", prefer_private: bool = True
) -> str:
    """Discovers IP of an EC2 instance by Name tag (prefers private IP for in-VPC calls)."""
    try:
        cfg = Config(connect_timeout=3, read_timeout=3, retries={"max_attempts": 1})
        ec2 = boto3.client("ec2", region_name=region_name, config=cfg)
        res = ec2.describe_instances(
            Filters=[
                {"Name": "tag:Name", "Values": [f"*{name_tag}*"]},
                {"Name": "instance-state-name", "Values": ["running"]},
            ]
        )
        for r in res.get("Reservations", []):
            for inst in r.get("Instances", []):
                priv_ip = inst.get("PrivateIpAddress")
                pub_ip = inst.get("PublicIpAddress")
                if prefer_private and priv_ip:
                    return priv_ip
                if pub_ip:
                    return pub_ip
                if priv_ip:
                    return priv_ip
    except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError):
        pass
    return ""


def resolve_spark_master_ip(
    cli_arg: str = "", region_name: str = "ap-southeast-2"
) -> str:
    """
    Intelligently resolves Spark Master IP:
    1. CLI argument (--spark-ip)
    2. SPARK_MASTER_IP environment variable
    3. EC2 tag discovery (prioritizing Private IP)
    4. VPC cluster static private IP fallback (172.31.16.110 from terraform/main.tf)
    """
    if cli_arg:
        return cli_arg.strip()

    env_ip = os.environ.get("SPARK_MASTER_IP", "").strip()
    if env_ip:
        return env_ip

    # Tag discovery (private IP preferred)
    tagged_ip = get_instance_ip_by_tag(
        "Spark-Master", region_name=region_name, prefer_private=True
    )
    if tagged_ip:
        return tagged_ip

    # Default VPC static IP for Spark Master (172.31.16.110)
    default_vpc_ip = "172.31.16.110"
    if check_broker_reachable(
        f"{default_vpc_ip}:8080", timeout=0.8
    ) or check_broker_reachable(f"{default_vpc_ip}:7077", timeout=0.8):
        return default_vpc_ip

    # If running on an EC2 instance in 172.31.x.x
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
        if local_ip.startswith("172.31."):
            return default_vpc_ip
    except OSError:
        pass

    return default_vpc_ip


# =====================================================================
# KAFKA METRICS & OFFSETS
# =====================================================================


def check_broker_reachable(bootstrap_servers: str, timeout: float = 0.5) -> bool:
    """Verifies TCP socket reachability to the first broker."""
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


def get_kafka_metrics(
    bootstrap_servers: str,
    topic_prefix: str,
    target_table: str | None = None,
) -> tuple[dict[str, Any], int, str | None]:
    """
    Fetches Kafka partition-level metrics:
      - Log Start Offset (Earliest available offset)
      - Log End Offset / High Watermark (Latest produced offset)
      - Message count per partition
      - Consumer Group committed offsets & consumer lag (if available)
    """
    if not check_broker_reachable(bootstrap_servers):
        return (
            {},
            0,
            (
                f"Cannot reach Kafka brokers at {bootstrap_servers} (Private VPC). "
                "Run directly on EC2 Control Node/Spark Master or forward port 9092 via SSH."
            ),
        )

    tables_to_query = (
        [target_table] if target_table and target_table in TABLES else TABLES
    )
    expected_topics = {t: f"{topic_prefix}.{t}" for t in tables_to_query}

    results: dict[str, Any] = {}
    total_cluster_messages = 0

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers.split(","),
            request_timeout_ms=5000,
            consumer_timeout_ms=3000,
        )
        cluster_topics = consumer.topics()

        # Attempt to query KafkaAdminClient for consumer groups
        admin_client: KafkaAdminClient | None = None
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers.split(","),
                request_timeout_ms=4000,
            )
        except (KafkaError, OSError):
            admin_client = None

        for table, topic in expected_topics.items():
            if topic not in cluster_topics:
                results[table] = {
                    "topic": topic,
                    "exists": False,
                    "partitions_count": 0,
                    "total_messages": 0,
                    "total_lag": 0,
                    "partitions": {},
                }
                continue

            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                results[table] = {
                    "topic": topic,
                    "exists": True,
                    "partitions_count": 0,
                    "total_messages": 0,
                    "total_lag": 0,
                    "partitions": {},
                }
                continue

            tps = [TopicPartition(topic, p) for p in sorted(partitions)]
            beginning_offsets = consumer.beginning_offsets(tps)
            end_offsets = consumer.end_offsets(tps)

            # Query consumer group committed offsets
            consumer_offsets: dict[TopicPartition, int] = {}
            group_id = f"hudi-streamer-{table}-group"
            if admin_client:
                try:
                    fn = getattr(
                        admin_client,
                        "list_consumer_group_offsets",
                        getattr(admin_client, "list_group_offsets", None),
                    )
                    if fn:
                        group_data = fn(group_id)
                        if isinstance(group_data, dict):
                            for tp, offset_meta in group_data.items():
                                off = getattr(offset_meta, "offset", None)
                                if off is not None:
                                    consumer_offsets[tp] = off
                except Exception:  # noqa: BLE001
                    pass

            topic_messages = 0
            topic_lag = 0
            has_consumer_offset = False
            partition_details: dict[int, dict[str, Any]] = {}

            for tp in tps:
                p_id = tp.partition
                lso = beginning_offsets.get(tp, 0)
                leo = end_offsets.get(tp, 0)
                msg_count = max(0, leo - lso)
                topic_messages += msg_count

                committed_offset = consumer_offsets.get(tp)
                if committed_offset is not None:
                    has_consumer_offset = True
                    p_lag = max(0, leo - committed_offset)
                    topic_lag += p_lag
                else:
                    p_lag = None

                partition_details[p_id] = {
                    "lso": lso,
                    "leo": leo,
                    "messages": msg_count,
                    "consumer_offset": committed_offset,
                    "lag": p_lag,
                }

            results[table] = {
                "topic": topic,
                "exists": True,
                "partitions_count": len(partitions),
                "total_messages": topic_messages,
                "total_lag": topic_lag if has_consumer_offset else None,
                "partitions": partition_details,
            }
            total_cluster_messages += topic_messages

        consumer.close()
        if admin_client:
            admin_client.close()

        return results, total_cluster_messages, None

    except (KafkaError, OSError, ValueError) as e:
        return {}, 0, f"Kafka query error: {e}"


# =====================================================================
# S3 & HUDI CHECKPOINT TIMELINE METRICS
# =====================================================================


def parse_hudi_checkpoint_string(checkpoint_str: str) -> dict[int, int]:
    """
    Parses Hudi Kafka checkpoint key from commit extraMetadata.
    Supported formats:
      - 'cdc.public.actor,0:1542,1:1538,2:1550'
      - '{"cdc.public.actor":{"0":1542,"1":1538}}'
      - '0:1542,1:1538'
    """
    if not checkpoint_str or not isinstance(checkpoint_str, str):
        return {}

    checkpoint_str = checkpoint_str.strip()

    # Try JSON format
    if checkpoint_str.startswith("{"):
        try:
            data = json.loads(checkpoint_str)
            if isinstance(data, dict):
                # Format: {"topic": {"0": 100}} or {"0": 100}
                res = {}
                for k, v in data.items():
                    if isinstance(v, dict):
                        for p, o in v.items():
                            res[int(p)] = int(o)
                    else:
                        res[int(k)] = int(v)
                return res
        except (json.JSONDecodeError, ValueError):
            pass

    # Format: topic,0:1542,1:1538 or 0:1542,1:1538
    res = {}
    tokens = checkpoint_str.split(",")
    for token in tokens:
        token = token.strip()
        if ":" in token:
            p_str, o_str = token.split(":", 1)
            try:
                res[int(p_str)] = int(o_str)
            except ValueError:
                pass
    return res


def get_hudi_lakehouse_metrics(
    s3_bucket: str,
    base_prefix: str,
    region_name: str = "ap-southeast-2",
    target_table: str | None = None,
    kafka_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Scans S3 for:
      1. General storage size and object count.
      2. Table-by-table Hudi commit timeline (.hoodie/*.deltacommit, *.commit).
      3. Hudi streamer checkpoint key (Kafka partition offsets checkpointed in S3).
      4. End-to-end ingestion lag (Kafka LEO vs. Hudi Checkpointed Offset).
    """
    if not s3_bucket:
        return {"error": "S3 bucket not configured"}

    s3 = get_s3_client(region_name=region_name)
    tables_to_check = (
        [target_table] if target_table and target_table in TABLES else TABLES
    )

    # 1. Overall S3 Storage Statistics (Sample up to 1,000 objects fast)
    total_files = 0
    total_bytes = 0
    latest_s3_modified = None
    try:
        res = s3.list_objects_v2(Bucket=s3_bucket, Prefix=base_prefix, MaxKeys=1000)
        for obj in res.get("Contents", []):
            total_files += 1
            total_bytes += obj["Size"]
            last_mod = obj["LastModified"]
            if latest_s3_modified is None or last_mod > latest_s3_modified:
                latest_s3_modified = last_mod
    except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError) as e:
        return {
            "error": f"S3 list error: {e}",
            "bucket": s3_bucket,
            "prefix": base_prefix,
            "total_files": 0,
            "size_mb": 0.0,
            "tables": {},
        }

    # 2. Discover table directories in S3
    detected_prefixes: dict[str, str] = {}
    active_base = base_prefix
    candidate_bases = [base_prefix, "bronze/default/", "default/", "bronze/"]
    for base in candidate_bases:
        clean = base.strip("/") + "/" if base.strip("/") else ""
        try:
            res = s3.list_objects_v2(
                Bucket=s3_bucket, Prefix=clean, Delimiter="/", MaxKeys=50
            )
            for cp in res.get("CommonPrefixes", []):
                p_str = cp.get("Prefix", "")
                folder = p_str.rstrip("/").split("/")[-1].replace("default.", "")
                if folder in TABLES:
                    detected_prefixes[folder] = p_str
            if detected_prefixes:
                active_base = clean
                break
        except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError):
            continue

    # If initial base_prefix had 0 files but active_base contains tables, recalculate volume
    if total_files == 0 and active_base != base_prefix:
        try:
            res = s3.list_objects_v2(Bucket=s3_bucket, Prefix=active_base, MaxKeys=1000)
            for obj in res.get("Contents", []):
                total_files += 1
                total_bytes += obj["Size"]
                last_mod = obj["LastModified"]
                if latest_s3_modified is None or last_mod > latest_s3_modified:
                    latest_s3_modified = last_mod
        except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError):
            pass

    # 3. Inspect Hudi commit timeline for each table
    tables_data: dict[str, Any] = {}
    now_utc = datetime.now(UTC)

    for table in tables_to_check:
        table_prefix = detected_prefixes.get(table)
        if not table_prefix:
            tables_data[table] = {
                "initialized": False,
                "latest_instant": None,
                "commit_type": None,
                "commit_age_sec": None,
                "checkpoint_offsets": {},
                "total_checkpointed_offset": 0,
                "ingestion_lag": None,
                "records_written": 0,
                "bytes_written": 0,
                "delta_commits_since_compaction": 0,
            }
            continue

        hoodie_cand = f"{table_prefix}.hoodie/"
        commit_keys: list[
            tuple[str, str, str]
        ] = []  # (key, instant_str, type: deltacommit/commit)
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=s3_bucket, Prefix=hoodie_cand):
                for item in page.get("Contents", []):
                    key = item["Key"]
                    match = re.search(r"(\d{14,17})\.(deltacommit|commit)$", key)
                    if match:
                        commit_keys.append((key, match.group(1), match.group(2)))
        except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError):
            pass

        if not commit_keys:
            tables_data[table] = {
                "initialized": False,
                "latest_instant": None,
                "commit_type": None,
                "commit_age_sec": None,
                "checkpoint_offsets": {},
                "total_checkpointed_offset": 0,
                "ingestion_lag": None,
                "records_written": 0,
                "bytes_written": 0,
                "delta_commits_since_compaction": 0,
            }
            continue

        # Sort commits chronologically descending (latest instant first)
        commit_keys.sort(key=lambda x: x[1], reverse=True)
        latest_key, latest_instant, latest_type = commit_keys[0]

        # Count delta commits since the last compaction commit (.commit)
        delta_count = 0
        for _, _, ctype in commit_keys:
            if ctype == "deltacommit":
                delta_count += 1
            else:
                break

        # Read JSON metadata of the latest completed commit
        checkpoint_offsets: dict[int, int] = {}
        records_written = 0
        bytes_written = 0
        commit_age_sec = None

        try:
            # Parse instant into datetime
            # Instant formats: YYYYMMDDHHMMSS or YYYYMMDDHHMMSSmmm
            dt_str = latest_instant[:14]
            commit_dt = datetime.strptime(dt_str, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
            commit_age_sec = max(0, int((now_utc - commit_dt).total_seconds()))
        except (ValueError, TypeError, IndexError):
            commit_age_sec = None

        try:
            obj_res = s3.get_object(Bucket=s3_bucket, Key=latest_key)
            commit_json = json.loads(obj_res["Body"].read().decode("utf-8"))

            records_written = commit_json.get("totalRecordsWritten", 0)
            bytes_written = commit_json.get("totalBytesWritten", 0)

            # Extract checkpoint key
            extra_meta = commit_json.get("extraMetadata", {})
            raw_ckpt = (
                extra_meta.get("deltastreamer.checkpoint.key")
                or extra_meta.get("hoodie.deltastreamer.checkpoint.key")
                or extra_meta.get("checkpoint.reset_key")
                or ""
            )
            checkpoint_offsets = parse_hudi_checkpoint_string(raw_ckpt)
        except (
            botocore.exceptions.BotoCoreError,
            botocore.exceptions.ClientError,
            json.JSONDecodeError,
        ):
            pass

        # Calculate True End-to-End Ingestion Lag if Kafka metrics are available
        total_ckpt_offset = sum(checkpoint_offsets.values())
        ingestion_lag = None

        if (
            kafka_metrics
            and table in kafka_metrics
            and kafka_metrics[table].get("exists")
        ):
            t_data = kafka_metrics[table]
            kafka_parts = t_data.get("partitions", {})
            if checkpoint_offsets:
                calc_lag = 0
                for p_id, p_info in kafka_parts.items():
                    leo = p_info.get("leo", 0)
                    ckpt = checkpoint_offsets.get(p_id, 0)
                    calc_lag += max(0, leo - ckpt)
                ingestion_lag = calc_lag
            elif total_ckpt_offset == 0:
                # No checkpoint parsed yet; lag equals total messages
                ingestion_lag = t_data.get("total_messages", 0)

        tables_data[table] = {
            "initialized": True,
            "hoodie_prefix": hoodie_cand,
            "latest_instant": latest_instant,
            "commit_type": latest_type.upper(),
            "commit_age_sec": commit_age_sec,
            "checkpoint_offsets": checkpoint_offsets,
            "total_checkpointed_offset": total_ckpt_offset,
            "ingestion_lag": ingestion_lag,
            "records_written": records_written,
            "bytes_written": bytes_written,
            "delta_commits_since_compaction": delta_count,
        }

    return {
        "error": None,
        "bucket": s3_bucket,
        "prefix": active_base,
        "total_files": total_files,
        "size_mb": total_bytes / (1024 * 1024),
        "size_gb": total_bytes / (1024 * 1024 * 1024),
        "latest_modified": latest_s3_modified,
        "tables": tables_data,
    }


# =====================================================================
# SPARK CLUSTER RUNTIME METRICS
# =====================================================================


def get_spark_master_metrics(spark_master_ip: str, port: int = 8080) -> dict[str, Any]:
    """
    Queries Spark Master REST API (http://<SPARK_MASTER_IP>:8080/json/)
    for active workers, total/used cores, total/used RAM, and running streamer applications.
    """
    if not spark_master_ip or spark_master_ip.startswith("<"):
        return {"reachable": False, "error": "Spark Master IP not identified"}

    url = f"http://{spark_master_ip}:{port}/json/"
    req = urllib.request.Request(url, headers={"User-Agent": "PipelineMonitor/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=2.0) as res:
            data = json.loads(res.read().decode("utf-8"))
            workers = data.get("workers", [])
            alive_workers = [w for w in workers if w.get("state") == "ALIVE"]
            active_apps = data.get("activeapps", [])

            return {
                "reachable": True,
                "url": f"http://{spark_master_ip}:{port}",
                "status": data.get("status", "UNKNOWN"),
                "workers_total": len(workers),
                "workers_alive": len(alive_workers),
                "cores_total": data.get("cores", 0),
                "cores_used": data.get("coresused", 0),
                "memory_total_mb": data.get("memory", 0),
                "memory_used_mb": data.get("memoryused", 0),
                "active_apps": [
                    {
                        "id": app.get("id"),
                        "name": app.get("name"),
                        "cores": app.get("cores"),
                        "memory_per_slave": app.get("memoryperslave"),
                        "duration_sec": int(app.get("duration", 0) / 1000),
                        "state": app.get("state"),
                    }
                    for app in active_apps
                ],
                "error": None,
            }
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as e:
        return {
            "reachable": False,
            "url": f"http://{spark_master_ip}:{port}",
            "status": "UNREACHABLE",
            "error": str(e),
        }


# =====================================================================
# DASHBOARD RENDERING
# =====================================================================


def format_age(seconds: int | None) -> str:
    """Formats seconds into human readable duration (e.g. 15s ago, 2m 10s ago)."""
    if seconds is None:
        return "N/A"
    if seconds < 60:
        return f"{seconds}s ago"
    mins = seconds // 60
    rem_secs = seconds % 60
    if mins < 60:
        return f"{mins}m {rem_secs}s ago"
    hours = mins // 60
    rem_mins = mins % 60
    return f"{hours}h {rem_mins}m ago"


def render_dashboard(
    bootstrap_servers: str,
    topic_prefix: str,
    s3_bucket: str,
    kafka_metrics: dict[str, Any],
    total_kafka_msgs: int,
    kafka_err: str | None,
    s3_metrics: dict[str, Any],
    spark_metrics: dict[str, Any],
    target_table: str | None = None,
):
    """Renders the comprehensive observability dashboard to terminal."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    os.system("clear" if os.name == "posix" else "cls")

    print("=" * 88)
    print(f"  DVDRENTAL STREAMING LAKEHOUSE OBSERVABILITY — {now_str}")
    print("=" * 88)

    # -------------------------------------------------------------
    # [1] KAFKA CDC INGESTION LAYER
    # -------------------------------------------------------------
    print("\n[1] KAFKA CDC INGESTION LAYER (DEBEZIUM -> KAFKA BROKERS)")
    print(f"  Bootstrap Servers: {bootstrap_servers}")
    print(f"  CDC Topic Prefix:  {topic_prefix}")

    if kafka_err:
        print("  Status: [!] OFFLINE / UNREACHABLE")
        print(f"  Reason: {kafka_err}")
    else:
        print(
            f"  Status: [OK] ONLINE  |  Total CDC Messages in Topics: {total_kafka_msgs:,}"
        )
        print("  " + "-" * 84)
        print(
            f"  {'Table Name':<15} {'Partitions':<11} {'LSO (Start)':<13} "
            f"{'LEO (Latest)':<14} {'Messages':<12} {'Consumer Lag':<14}"
        )
        print("  " + "-" * 84)

        for table, data in kafka_metrics.items():
            if not data.get("exists"):
                print(
                    f"  {table:<15} {'(not created)':<11} {'-':<13} {'-':<14} {'0':<12} {'-':<14}"
                )
                continue

            p_cnt = data.get("partitions_count", 0)
            m_cnt = data.get("total_messages", 0)
            parts = data.get("partitions", {})

            # Summarize partition ranges
            if parts:
                min_lso = min(p["lso"] for p in parts.values())
                max_leo = max(p["leo"] for p in parts.values())
                lso_str = f"{min_lso:,}" if len(parts) == 1 else f"{min_lso:,}..min"
                leo_str = f"{max_leo:,}" if len(parts) == 1 else f"{max_leo:,}..max"
            else:
                lso_str, leo_str = "0", "0"

            lag_val = data.get("total_lag")
            lag_str = f"{lag_val:,}" if lag_val is not None else "(S3 Managed)"

            print(
                f"  {table:<15} {p_cnt:<11} {lso_str:<13} {leo_str:<14} {m_cnt:<12,} {lag_str:<14}"
            )
        print("  " + "-" * 84)

    # -------------------------------------------------------------
    # [2] S3 HUDI LAKEHOUSE & CHECKPOINT TIMELINE
    # -------------------------------------------------------------
    print("\n[2] S3 HUDI BRONZE LAKEHOUSE & CHECKPOINT TIMELINE")
    print(
        f"  S3 Bucket: {s3_bucket} (Prefix: {s3_metrics.get('prefix', 'bronze/default/')})"
    )

    if s3_metrics.get("error"):
        print(f"  Error: {s3_metrics['error']}")
    else:
        total_files = s3_metrics.get("total_files", 0)
        size_mb = s3_metrics.get("size_mb", 0.0)
        size_gb = s3_metrics.get("size_gb", 0.0)
        last_mod = s3_metrics.get("latest_modified")
        last_mod_str = (
            last_mod.strftime("%Y-%m-%d %H:%M:%S UTC") if last_mod else "None"
        )

        print(
            f"  Stored Volume:     {size_gb:.2f} GB ({size_mb:.1f} MB)  |  Files: {total_files:,}"
        )
        print(f"  Latest S3 Object:  {last_mod_str}")

        hudi_tables = s3_metrics.get("tables", {})
        if hudi_tables:
            print("  " + "-" * 84)
            print(
                f"  {'Table Name':<15} {'Latest Commit Instant':<22} {'Commit Age':<12} "
                f"{'Type':<12} {'Compaction':<12} {'Ingestion Lag':<12}"
            )
            print("  " + "-" * 84)

            for table, h_data in hudi_tables.items():
                if not h_data.get("initialized"):
                    print(
                        f"  {table:<15} {'(pending initial load)':<22} {'-':<12} "
                        f"{'-':<12} {'-':<12} {'-':<12}"
                    )
                    continue

                instant = h_data.get("latest_instant", "N/A")
                age_str = format_age(h_data.get("commit_age_sec"))
                ctype = h_data.get("commit_type", "DELTA")
                delta_cnt = h_data.get("delta_commits_since_compaction", 0)
                compaction_str = f"{delta_cnt}/5 deltas"

                lag_val = h_data.get("ingestion_lag")
                if lag_val is not None:
                    lag_str = f"{lag_val:,} msgs" if lag_val > 0 else "0 (Synced)"
                else:
                    lag_str = "N/A (No Kafka)"

                print(
                    f"  {table:<15} {instant:<22} {age_str:<12} "
                    f"{ctype:<12} {compaction_str:<12} {lag_str:<12}"
                )
            print("  " + "-" * 84)

    # -------------------------------------------------------------
    # [3] SPARK STREAMING CLUSTER RUNTIME
    # -------------------------------------------------------------
    print("\n[3] SPARK MASTER & STREAMING ENGINE RUNTIME")
    spark_url = spark_metrics.get("url", "N/A")
    if spark_metrics.get("reachable"):
        workers_alive = spark_metrics.get("workers_alive", 0)
        workers_tot = spark_metrics.get("workers_total", 0)
        c_used = spark_metrics.get("cores_used", 0)
        c_tot = spark_metrics.get("cores_total", 0)
        m_used_gb = spark_metrics.get("memory_used_mb", 0) / 1024
        m_tot_gb = spark_metrics.get("memory_total_mb", 0) / 1024

        print(f"  Master Web UI: {spark_url} [STATUS: {spark_metrics.get('status')}]")
        print(f"  Workers:       {workers_alive}/{workers_tot} ALIVE")
        print(f"  Cores:         {c_used}/{c_tot} Cores Used ({c_tot - c_used} free)")
        print(f"  Cluster RAM:   {m_used_gb:.1f}/{m_tot_gb:.1f} GB Allocated")

        active_apps = spark_metrics.get("active_apps", [])
        if active_apps:
            print("  Active Streaming Applications:")
            for app in active_apps:
                dur_str = format_age(app.get("duration_sec")).replace(" ago", "")
                print(
                    f"    • {app.get('name')} (ID: {app.get('id')}) | "
                    f"Cores: {app.get('cores')}, RAM/Worker: {app.get('memory_per_slave')}MB, "
                    f"Uptime: {dur_str}, State: {app.get('state')}"
                )
        else:
            print("  Active Streaming Applications: None currently active")
    else:
        print(f"  Master Web UI: {spark_url} [STATUS: UNREACHABLE / STANDBY]")
        if spark_metrics.get("error"):
            print(f"  Note: {spark_metrics['error']}")

    # -------------------------------------------------------------
    # [4] TARGET TABLE DEEP INSPECTION (If --table is specified)
    # -------------------------------------------------------------
    if target_table and target_table in TABLES:
        print(f"\n[4] DEEP DIVE: TABLE '{target_table.upper()}'")
        # Kafka partition breakdown
        k_t = kafka_metrics.get(target_table, {})
        if k_t and k_t.get("exists"):
            print("  Kafka Partitions:")
            for p_id, p_info in sorted(k_t.get("partitions", {}).items()):
                print(
                    f"    Partition {p_id}: Start(LSO)={p_info['lso']:,} | "
                    f"End(LEO)={p_info['leo']:,} | Messages={p_info['messages']:,} | "
                    f"ConsumerOffset={p_info['consumer_offset']} | Lag={p_info['lag']}"
                )
        # Hudi checkpoint breakdown
        h_t = s3_metrics.get("tables", {}).get(target_table, {})
        if h_t and h_t.get("initialized"):
            print("  Hudi Streamer Checkpoint Metadata:")
            ckpts = h_t.get("checkpoint_offsets", {})
            if ckpts:
                for p_id, ckpt in sorted(ckpts.items()):
                    print(f"    Partition {p_id} S3 Checkpointed Offset: {ckpt:,}")
            else:
                print("    Checkpoint Key: (Default earliest / bootstrap)")
            print(
                f"    Latest Write Stats: {h_t.get('records_written'):,} records, "
                f"{h_t.get('bytes_written') / 1024:.1f} KB written"
            )

    # -------------------------------------------------------------
    # [5] OBSERVABILITY CHEAT SHEET & HOW IT WORKS
    # -------------------------------------------------------------
    print("\n[5] HOW IT WORKS: KAFKA OFFSETS VS. SPARK/HUDI CHECKPOINTS")
    print(
        "  • Log Start Offset (LSO):  Earliest record retained in Kafka partition log."
    )
    print(
        "  • Log End Offset (LEO):    Latest watermark published by Debezium/Producer."
    )
    print(
        "  • Hudi S3 Checkpoint:      Offset committed to S3 .hoodie/<instant>.deltacommit."
    )
    print(
        "                             (Hudi manages checkpoints in S3 for transactional idempotency)."
    )
    print(
        "  • Ingestion Lag:           Pending records in Kafka yet to be flushed to S3 (LEO - Checkpoint)."
    )
    print(
        "  • Compaction:              Merge-On-Read compresses delta logs into Parquet base files every 5 commits."
    )
    print("=" * 88)


# =====================================================================
# EDUCATIONAL EXPLANATION MODE (--explain)
# =====================================================================


def print_explanation_guide():
    """Prints a detailed architectural explanation of the streaming lakehouse state mechanics."""
    guide = """
========================================================================================
   DVDRENTAL STREAMING LAKEHOUSE: KAFKA OFFSETS & SPARK CHECKPOINTING MECHANICS
========================================================================================

1. ARCHITECTURAL FLOW & STATE BOUNDARIES:

   PostgreSQL WAL ──► Debezium CDC ──► Kafka Topics ──► HoodieMultiTableStreamer ──► AWS S3
   (Source of Truth)  (Publish Events)  (Partition Log)   (Spark Micro-Batch Engine)   (Bronze Lake)

2. KAFKA OFFSETS EXPLAINED:
   - Topic Partitions:
     Each CDC topic (e.g. cdc.public.rental) is split into partitions (0, 1, 2) to parallelize throughput.
   - Log Start Offset (LSO):
     The oldest event offset available in Kafka. Records before LSO have been pruned by retention.
   - Log End Offset (LEO) / High Watermark:
     The next offset to be written by the producer. All messages in [LSO .. LEO - 1] are valid unread/read data.

3. SPARK & APACHE HUDI CHECKPOINTING:
   - Why not Kafka Consumer Groups?
     Traditional Kafka consumers commit offsets to the Kafka '__consumer_offsets' topic.
     If a Spark worker crashes between writing files to S3 and committing to Kafka, a split-brain
     duplicate write occurs.
   - Hudi's Transactional S3 Checkpoint:
     HoodieMultiTableStreamer stores its checkpoint key DIRECTLY inside the table's S3 commit metadata:
     File: s3://<bucket>/bronze/default/<table_name>/.hoodie/<instant>.deltacommit
     Key:  "deltastreamer.checkpoint.key": "cdc.public.rental,0:15200,1:15180,2:15210"
     -> Exactly-Once semantics at the storage layer via atomic atomic commit metadata.

4. IDEMPOTENT UPSERT & DEDUPLICATION:
   - Precombine Field: '__ts_ms' (PostgreSQL transaction timestamp).
   - Record Key: Primary key (e.g. rental_id).
   - If a failure forces Hudi to replay a micro-batch from the last checkpoint, Hudi evaluates '__ts_ms'.
     The newer record always overwrites older records, guaranteeing zero duplicates on replay.

5. MERGE-ON-READ (MOR) & COMPACTION:
   - Delta Commits: Incoming CDC changes are written quickly to columnar Avro delta logs (.log).
   - Compaction: Every 5 delta commits (hoodie.compact.inline.max.delta.commits=5), Hudi merges
     the delta logs into Parquet base files (.parquet), optimizing subsequent Athena / dbt read performance.
========================================================================================
"""
    print(guide)


# =====================================================================
# MAIN ENTRY POINT
# =====================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Streaming Lakehouse Observability Dashboard & Checkpoint Monitor"
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
    parser.add_argument(
        "--s3-prefix",
        type=str,
        default="",
        help="S3 table base prefix override (e.g. bronze/default/)",
    )
    parser.add_argument(
        "--spark-ip",
        type=str,
        default="",
        help="Spark Master IP override (default: auto-detects private IP 172.31.16.110 in VPC)",
    )
    parser.add_argument(
        "--table",
        type=str,
        default="",
        help="Focus inspection on a single table (e.g. --table rental)",
    )
    parser.add_argument(
        "--explain",
        action="store_true",
        help="Print deep architectural guide on Kafka offsets & Spark checkpoints",
    )
    args = parser.parse_args()

    if args.explain:
        print_explanation_guide()
        sys.exit(0)

    # 1. Load defaults from Secrets Manager or environment
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

    # Derive S3 prefix from base_path in secret or fallback
    secret_base_path = s3_secret.get("base_path", "")
    default_prefix = "bronze/default/"
    if secret_base_path:
        match = re.search(r"s3a?://[^/]+/(.+)", secret_base_path)
        if match:
            default_prefix = match.group(1).rstrip("/") + "/"

    s3_prefix = args.s3_prefix or default_prefix
    spark_master_ip = resolve_spark_master_ip(args.spark_ip)
    target_table = args.table.strip().lower() if args.table else None

    while True:
        # Fetch Kafka metrics
        kafka_metrics, total_msgs, kafka_err = get_kafka_metrics(
            bootstrap_servers, topic_prefix, target_table=target_table
        )

        # Fetch S3 Hudi Lakehouse metrics & Checkpoints
        s3_metrics = get_hudi_lakehouse_metrics(
            s3_bucket,
            s3_prefix,
            target_table=target_table,
            kafka_metrics=kafka_metrics,
        )

        # Fetch Spark Master REST metrics
        spark_metrics = get_spark_master_metrics(spark_master_ip)

        # Render dashboard
        render_dashboard(
            bootstrap_servers,
            topic_prefix,
            s3_bucket,
            kafka_metrics,
            total_msgs,
            kafka_err,
            s3_metrics,
            spark_metrics,
            target_table=target_table,
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
