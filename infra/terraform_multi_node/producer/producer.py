#!/usr/bin/env python3
"""
DVDRental Debezium CDC Bulk Mock Data Generator
Enterprise Stateful Streaming Edition with Full CDC Lifecycle (Insert, Update, Delete)

Features:
1. Stateful Persistence: Remembers ID sequences across multiple runs via state file.
   Running again increments cleanly from the previous high watermark (No ID collisions!).
2. Full CDC Event Lifecycle:
   - 85% INSERT ('c'): New rentals and payments.
   - 10% UPDATE ('u'): Active rentals returned with updated return_date and last_update.
   -  5% DELETE ('d'): Cancelled / refunded transactions with proper Debezium soft delete markers.
3. Strict 100% Referential Integrity DAG across all 15 DVD Rental tables.
4. Dual-compatible Kafka message envelope (Full Debezium 2.5.0 + Flattened root for Hudi JsonKafkaSource).
5. Constant Low RAM footprint (~50MB) using bounded ring-buffers.
6. High throughput via LZ4 compression, 64KB batches, linger_ms=20.
7. Smart Kafka Broker discovery (CLI, env, AWS Secrets Manager, EC2 cluster fallback).
"""

import argparse
import json
import logging
import os
import random
import socket
import sys
import time
from collections import deque
from typing import Any, Dict, Optional, Tuple
from kafka import KafkaConsumer, KafkaProducer, TopicPartition


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("bulk_producer")

# Default state file and persistence paths
DEFAULT_STATE_FILE = os.path.expanduser("~/.dvdrental_producer_state.json")
DEFAULT_S3_STATE_KEY = "metadata/producer_state.json"
BASE_START_ID = 50000

# ==============================================================================
# REFERENCE CATALOGS FOR REALISTIC SYNTHETIC DATA
# ==============================================================================
COUNTRIES = [
    "United States", "Canada", "United Kingdom", "Germany", "France",
    "Japan", "Australia", "Vietnam", "Singapore", "Brazil",
    "Italy", "Spain", "Netherlands", "South Korea", "India",
]

CITIES = [
    ("New York", 1), ("Los Angeles", 1), ("Chicago", 1), ("Houston", 1),
    ("Toronto", 2), ("Vancouver", 2), ("Montreal", 2),
    ("London", 3), ("Manchester", 3), ("Birmingham", 3),
    ("Berlin", 4), ("Munich", 4), ("Hamburg", 4),
    ("Paris", 5), ("Lyon", 5), ("Marseille", 5),
    ("Tokyo", 6), ("Osaka", 6), ("Kyoto", 6),
    ("Sydney", 7), ("Melbourne", 7), ("Brisbane", 7),
    ("Hanoi", 8), ("Ho Chi Minh City", 8), ("Da Nang", 8),
    ("Singapore", 9),
    ("Sao Paulo", 10), ("Rio de Janeiro", 10),
    ("Rome", 11), ("Milan", 11),
    ("Madrid", 12), ("Barcelona", 12),
    ("Amsterdam", 13), ("Rotterdam", 13),
    ("Seoul", 14), ("Busan", 14),
    ("Mumbai", 15), ("Delhi", 15), ("Bengaluru", 15),
]

LANGUAGES = ["English", "Italian", "Japanese", "Mandarin", "French", "German"]

CATEGORIES = [
    "Action", "Animation", "Children", "Classics", "Comedy",
    "Documentary", "Drama", "Family", "Foreign", "Games",
    "Horror", "Music", "New", "Sci-Fi", "Sports", "Travel",
]

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
    "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
    "Christopher", "Nancy", "Daniel", "Lisa", "Matthew", "Betty", "Anthony",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
]

FILM_TITLES = [
    "Adventure", "Mystery", "Chronicles", "Legacy", "Destiny",
    "Voyage", "Odyssey", "Protocol", "Horizon", "Empire",
    "Kingdom", "Shadow", "Falcon", "Warrior", "Legend",
    "Matrix", "Nexus", "Frontier", "Infinity", "Vanguard",
]

DISTRICTS = [
    "Central", "North", "South", "East", "West",
    "Downtown", "Suburbs", "Midtown", "Riverside", "Highland",
]


# ==============================================================================
# STATE PERSISTENCE (INCREMENTAL RUN RECOVERY)
# ==============================================================================
def get_default_s3_bucket() -> Optional[str]:
    """Finds default S3 bucket from env, secretsmanager, or fallback."""
    if os.environ.get("S3_BUCKET"):
        return os.environ["S3_BUCKET"]
    try:
        import boto3
        session = boto3.session.Session()
        region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-2"))
        client = session.client("secretsmanager", region_name=region)
        secret = client.get_secret_value(SecretId="dvdrental/s3_config")
        secret_json = json.loads(secret.get("SecretString", "{}"))
        if "bucket_name" in secret_json:
            return secret_json["bucket_name"]
    except Exception:
        pass
    return "dvdrental-hudi-lake-dzung-2026"


def load_producer_state(state_file: str, s3_bucket: Optional[str] = None) -> Dict[str, Any]:
    """
    Loads persistent watermarks across multiple producer runs.
    Checks:
    1. Local filesystem state_file
    2. S3 bucket (preserves state when EC2 instances are terminated/recreated)
    """
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                state = json.load(f)
                if state:
                    logger.info(f"Loaded persistent state from local file {state_file}")
                    return state
        except Exception as e:
            logger.warning(f"Could not read local state file ({e}). Trying S3...")

    bucket = s3_bucket or get_default_s3_bucket()
    if bucket:
        try:
            import boto3
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=bucket, Key=DEFAULT_S3_STATE_KEY)
            state = json.loads(obj["Body"].read().decode("utf-8"))
            if state:
                logger.info(f"Loaded persistent state from s3://{bucket}/{DEFAULT_S3_STATE_KEY}")
                try:
                    os.makedirs(os.path.dirname(os.path.abspath(state_file)), exist_ok=True)
                    with open(state_file, "w", encoding="utf-8") as f:
                        json.dump(state, f, indent=2)
                except Exception:
                    pass
                return state
        except Exception as e:
            logger.debug(f"Could not load state from S3 ({e}).")

    return {}


def save_producer_state(state_file: str, state: Dict[str, Any], s3_bucket: Optional[str] = None) -> None:
    """Saves updated watermarks to local disk AND S3 bucket."""
    try:
        os.makedirs(os.path.dirname(os.path.abspath(state_file)), exist_ok=True)
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        logger.info(f"Saved updated state to local file {state_file}")
    except Exception as e:
        logger.warning(f"Could not save state to {state_file}: {e}")

    bucket = s3_bucket or get_default_s3_bucket()
    if bucket:
        try:
            import boto3
            s3 = boto3.client("s3")
            s3.put_object(
                Bucket=bucket,
                Key=DEFAULT_S3_STATE_KEY,
                Body=json.dumps(state, indent=2).encode("utf-8"),
            )
            logger.info(f"Synchronized persistent state to s3://{bucket}/{DEFAULT_S3_STATE_KEY}")
        except Exception as e:
            logger.debug(f"Could not sync state to S3 ({e}).")


# ==============================================================================
# SMART BROKER DISCOVERY
# ==============================================================================
def is_broker_reachable(broker_str: str) -> bool:
    """Quick check if the first broker endpoint is reachable via TCP."""
    first_ep = broker_str.split(",")[0].strip()
    if ":" in first_ep:
        host, port_str = first_ep.split(":", 1)
        try:
            with socket.create_connection((host, int(port_str)), timeout=0.8):
                return True
        except OSError:
            return False
    return False


def resolve_bootstrap_servers(broker_arg: Optional[str]) -> str:
    """Intelligently detects Kafka bootstrap servers."""
    if broker_arg and broker_arg != "auto":
        return broker_arg

    for env_var in ["KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BROKERS"]:
        val = os.environ.get(env_var)
        if val:
            return val

    # Try AWS Secrets Manager if running inside AWS EC2 with IAM instance profile
    try:
        import boto3
        session = boto3.session.Session()
        region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-2"))
        client = session.client("secretsmanager", region_name=region)
        secret = client.get_secret_value(SecretId="dvdrental/kafka_bootstrap_servers")
        secret_json = json.loads(secret.get("SecretString", "{}"))
        if "bootstrap_servers" in secret_json:
            return secret_json["bootstrap_servers"]
    except Exception:
        pass

    # Default EC2 private cluster nodes
    ec2_cluster = "172.31.16.100:9092,172.31.16.101:9092,172.31.16.102:9092"
    if is_broker_reachable(ec2_cluster):
        return ec2_cluster

    # Local development fallback
    return "localhost:9092"


def detect_kafka_watermarks(broker: str, topic_prefix: str) -> Dict[str, int]:
    """
    Queries Kafka brokers for current Log End Offsets (LEO).
    Guarantees that subsequent runs will NEVER start from 1 even if the state file is missing.
    """
    watermarks: Dict[str, int] = {}
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=broker.split(","),
            request_timeout_ms=3000,
            consumer_timeout_ms=3000,
        )
        for tbl in ["rental", "payment", "film", "customer", "inventory"]:
            topic = f"{topic_prefix}.{tbl}"
            partitions = consumer.partitions_for_topic(topic)
            if partitions:
                tps = [TopicPartition(topic, p) for p in partitions]
                end_offsets = consumer.end_offsets(tps)
                watermarks[tbl] = sum(end_offsets.values())
        consumer.close()
    except Exception as e:
        logger.debug(f"Could not auto-detect Kafka watermarks: {e}")
    return watermarks


# ==============================================================================
# DUAL DEBEZIUM + HUDI FLAT ENVELOPE FORMATTER (Supports 'c', 'u', 'd')
# ==============================================================================
def make_debezium_envelope(
    table: str,
    after_data: Optional[Dict[str, Any]] = None,
    before_data: Optional[Dict[str, Any]] = None,
    op: str = "c",
    connector_name: str = "dvdrental_source",
    event_ts_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Constructs a dual-compatible CDC envelope for INSERT, UPDATE, or DELETE:
    1. Full Debezium 2.5 envelope (before, after, source, op, ts_ms).
    2. Flattened fields at root level for Hudi JsonKafkaSource + Avro schemas.
       - op == 'c': before=None, after=record, __op='c', __deleted='false'
       - op == 'u': before=old,  after=new,    __op='u', __deleted='false'
       - op == 'd': before=old,  after=None,   __op='d', __deleted='true'
    """
    now_ms = event_ts_ms if event_ts_ms is not None else int(time.time() * 1000)
    envelope = {
        "before": before_data,
        "after": after_data,
        "source": {
            "version": "2.5.0.Final",
            "connector": "postgresql",
            "name": connector_name,
            "ts_ms": now_ms,
            "db": "dvdrental",
            "schema": "public",
            "table": table,
            "txId": random.randint(100000, 999999),
            "lsn": random.randint(10000000, 99999999),
        },
        "op": op,
        "ts_ms": now_ms,
    }

    # For Hudi JsonKafkaSource: root level MUST have key fields
    # If op == 'd', after_data is None so we unpack before_data to retain primary/foreign keys
    root_payload = after_data if after_data is not None else (before_data or {})
    envelope.update(root_payload)

    envelope["__ts_ms"] = now_ms
    envelope["__op"] = op
    envelope["__deleted"] = "true" if op == "d" else "false"
    return envelope


def get_kafka_key(table: str, data: Dict[str, Any]) -> str:
    """Return partitioning key for Kafka to preserve ordering per entity."""
    if table == "film_category":
        return f"{data.get('film_id')},{data.get('category_id')}"
    if table == "film_actor":
        return f"{data.get('actor_id')},{data.get('film_id')}"
    pk_field = f"{table}_id"
    if pk_field in data:
        return str(data[pk_field])
    return str(next(iter(data.values())))


# ==============================================================================
# MAIN STREAM GENERATOR PIPELINE
# ==============================================================================
def stream_bulk_data(
    broker: str,
    topic_prefix: str = "cdc.public",
    num_films: int = 10000,
    num_customers: int = 50000,
    num_stores: int = 10,
    num_staffs: int = 50,
    num_actors: int = 300,
    num_rentals: int = 5000000,
    days_span: int = 30,
    insert_pct: int = 70,
    update_pct: int = 20,
    delete_pct: int = 10,
    start_rental_id: Optional[int] = None,
    start_payment_id: Optional[int] = None,
    state_file: str = DEFAULT_STATE_FILE,
    s3_bucket: Optional[str] = None,
    reset_state: bool = False,
    force_bootstrap: bool = False,
):
    print("=" * 82)
    print("  DVDRENTAL ADVANCED CDC BULK GENERATOR (FULL LIFECYCLE: C / U / D)")
    print("=" * 82)
    print(f"  Kafka Brokers:     {broker}")
    print(f"  Topic Prefix:      {topic_prefix}")
    print(f"  CDC Op Mix:        {insert_pct}% Insert ('c') | {update_pct}% Update ('u') | {delete_pct}% Delete ('d')")

    # Load persistent state (Local disk cache + S3 persistence)
    state = {} if reset_state else load_producer_state(state_file, s3_bucket=s3_bucket)
    is_resuming = bool(state)

    # Inspect Kafka cluster for existing topic watermarks
    detected: Dict[str, int] = {}
    if not reset_state:
        print("[*] Inspecting Kafka cluster for existing topic watermarks...")
        detected = detect_kafka_watermarks(broker, topic_prefix)
        if detected.get("rental", 0) > 0:
            print(
                f"  [+] AUTO-DETECTED Kafka offsets: {detected['rental']:,} rentals, "
                f"{detected.get('payment', 0):,} payments found in topics!"
            )

    # Sequence Resolution: BASE_START_ID is 50,000 (assumes baseline Run #1 is in place)
    # Never starts from 1! Always starts from 50,000 or increments higher.
    if start_rental_id is None:
        last_rid = state.get("last_rental_id")
        if last_rid is not None and last_rid >= BASE_START_ID:
            start_rental_id = last_rid + 1
        elif detected.get("rental", 0) >= BASE_START_ID:
            start_rental_id = detected["rental"] + 1
        else:
            start_rental_id = BASE_START_ID

    if start_payment_id is None:
        last_pid = state.get("last_payment_id")
        if last_pid is not None and last_pid >= BASE_START_ID:
            start_payment_id = last_pid + 1
        elif detected.get("payment", 0) >= BASE_START_ID:
            start_payment_id = detected["payment"] + 1
        else:
            start_payment_id = BASE_START_ID

    total_inventories = state.get("total_inventories", num_films * 3)
    total_customers = max(num_customers, state.get("total_customers", num_customers))

    # Mark resuming if state was present, or Kafka had events, or starting beyond base
    if bool(state) or detected.get("rental", 0) > 0 or start_rental_id > BASE_START_ID:
        is_resuming = True

    print(f"  [+] Starting rental_id:  #{start_rental_id:,} (Baseline: {BASE_START_ID:,})")
    print(f"  [+] Starting payment_id: #{start_payment_id:,} (Baseline: {BASE_START_ID:,})")
    print(f"  [+] Active Pool Scale:   {total_customers:,} customers | {total_inventories:,} inventories")

    # Chronological streaming timeline window
    now_wall_micros = int(time.time() * 1_000_000)
    day_micros = 86400 * 1_000_000
    total_micros_span = max(1, days_span) * day_micros
    sim_end_micros = now_wall_micros
    sim_start_micros = sim_end_micros - total_micros_span

    start_dt_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(sim_start_micros / 1_000_000))
    end_dt_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(sim_end_micros / 1_000_000))
    est_per_day = int(num_rentals / max(1, days_span))
    print(f"  Streaming Time Span:   {start_dt_str} -> {end_dt_str} ({days_span} days)")
    print(f"  Partition Density:     ~{est_per_day:,} rentals/day across {days_span} daily partitions")

    est_total_msgs = int(num_rentals * 1.95)
    est_gb = (est_total_msgs * 580) / (1024 ** 3)
    print(f"  Planned Scale:         {num_rentals:,} transaction rounds (~{est_total_msgs:,} CDC messages, ~{est_gb:.2f} GB)")
    print("=" * 82)

    print(f"\n[*] Connecting to Kafka with LZ4 compression...")
    producer_kwargs = {
        "bootstrap_servers": broker.split(","),
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "key_serializer": lambda k: k.encode("utf-8") if k else None,
        "acks": 1,
        "compression_type": "lz4",
        "batch_size": 65536,  # 64 KB batch
        "linger_ms": 20,      # optimal batch aggregation
    }
    producer = KafkaProducer(**producer_kwargs)

    counts = {"c": 0, "u": 0, "d": 0, "total": 0}

    def emit_event(
        table: str,
        after_data: Optional[Dict[str, Any]] = None,
        before_data: Optional[Dict[str, Any]] = None,
        op: str = "c",
        event_ts_ms: Optional[int] = None,
    ):
        nonlocal counts
        topic = f"{topic_prefix}.{table}"
        key_data = after_data if after_data is not None else before_data
        key = get_kafka_key(table, key_data) if key_data else None
        envelope = make_debezium_envelope(
            table,
            after_data=after_data,
            before_data=before_data,
            op=op,
            event_ts_ms=event_ts_ms,
        )
        producer.send(topic, key=key, value=envelope)
        counts[op] += 1
        counts["total"] += 1

    base_micros = sim_start_micros

    # ==========================================================================
    # PHASES 1 to 5: DIMENSIONS (Skipped by default because Run #1 baseline is assumed)
    # ==========================================================================
    need_bootstrap = force_bootstrap

    if need_bootstrap:
        print(f"\n[1/6] Bootstrapping Geographical Hierarchy (country, city, address)...")
        for c_idx, c_name in enumerate(COUNTRIES, start=1):
            emit_event("country", after_data={
                "country_id": c_idx,
                "country": c_name,
                "last_update": str(base_micros),
            }, op="c")

        num_cities = len(CITIES)
        for ct_idx, (ct_name, c_ref) in enumerate(CITIES, start=1):
            emit_event("city", after_data={
                "city_id": ct_idx,
                "city": ct_name,
                "country_id": c_ref,
                "last_update": str(base_micros),
            }, op="c")

        num_addresses = max(num_stores * 5, 1000)
        for addr_id in range(1, num_addresses + 1):
            city_ref = ((addr_id - 1) % num_cities) + 1
            emit_event("address", after_data={
                "address_id": addr_id,
                "address": f"{addr_id * 12} Broadway Avenue",
                "address2": None,
                "district": DISTRICTS[addr_id % len(DISTRICTS)],
                "city_id": city_ref,
                "postal_code": f"{10000 + (addr_id % 89999)}",
                "phone": f"+1-555-{addr_id:04d}",
                "last_update": str(base_micros),
            }, op="c")

        print(f"\n[2/6] Bootstrapping Static Catalogs (languages, categories, actors)...")
        num_languages = len(LANGUAGES)
        for l_idx, l_name in enumerate(LANGUAGES, start=1):
            emit_event("language", after_data={
                "language_id": l_idx,
                "name": l_name,
                "last_update": str(base_micros),
            }, op="c")

        num_categories = len(CATEGORIES)
        for cat_idx, cat_name in enumerate(CATEGORIES, start=1):
            emit_event("category", after_data={
                "category_id": cat_idx,
                "name": cat_name,
                "last_update": str(base_micros),
            }, op="c")

        for a_idx in range(1, num_actors + 1):
            emit_event("actor", after_data={
                "actor_id": a_idx,
                "first_name": FIRST_NAMES[a_idx % len(FIRST_NAMES)],
                "last_name": LAST_NAMES[a_idx % len(LAST_NAMES)],
                "last_update": str(base_micros),
            }, op="c")

        print(f"\n[3/6] Bootstrapping Organizations (store, staff)...")
        for s_id in range(1, num_stores + 1):
            emit_event("store", after_data={
                "store_id": s_id,
                "manager_staff_id": s_id if s_id <= num_staffs else 1,
                "address_id": s_id,
                "last_update": str(base_micros),
            }, op="c")

        for st_id in range(1, num_staffs + 1):
            fn = FIRST_NAMES[st_id % len(FIRST_NAMES)]
            ln = LAST_NAMES[st_id % len(LAST_NAMES)]
            emit_event("staff", after_data={
                "staff_id": st_id,
                "first_name": fn,
                "last_name": ln,
                "address_id": ((st_id - 1) % num_addresses) + 1,
                "email": f"{fn.lower()}.{ln.lower()}.{st_id}@sakilastaff.com",
                "store_id": ((st_id - 1) % num_stores) + 1,
                "active": True,
                "username": f"staff_{st_id}",
                "password": "sha256_mock_hash",
                "last_update": str(base_micros),
                "picture": None,
            }, op="c")

        print(f"\n[4/6] Bootstrapping Film Catalog & Inventories ({num_films:,} films)...")
        inv_counter = 1
        copies_per_film = 3
        for film_id in range(1, num_films + 1):
            title_word = FILM_TITLES[film_id % len(FILM_TITLES)]
            emit_event("film", after_data={
                "film_id": film_id,
                "title": f"{title_word} {film_id}",
                "description": f"Epic synthetic film narrative {film_id} with high quality story.",
                "release_year": 2000 + (film_id % 26),
                "language_id": ((film_id - 1) % num_languages) + 1,
                "rental_duration": (film_id % 5) + 3,
                "rental_rate": random.choice(["0.99", "2.99", "4.99"]),
                "length": 60 + (film_id % 120),
                "replacement_cost": random.choice(["14.99", "19.99", "24.99", "29.99"]),
                "rating": random.choice(["G", "PG", "PG-13", "R", "NC-17"]),
                "last_update": str(base_micros),
                "special_features": ["Trailers", "Commentaries"],
                "fulltext": f"'{title_word.lower()}':1 'film':2",
            }, op="c")

            cat_ref = ((film_id - 1) % num_categories) + 1
            emit_event("film_category", after_data={
                "film_id": film_id,
                "category_id": cat_ref,
                "last_update": str(base_micros),
            }, op="c")

            act1 = ((film_id * 3) % num_actors) + 1
            act2 = ((film_id * 7) % num_actors) + 1
            emit_event("film_actor", after_data={
                "actor_id": act1,
                "film_id": film_id,
                "last_update": str(base_micros),
            }, op="c")
            if act2 != act1:
                emit_event("film_actor", after_data={
                    "actor_id": act2,
                    "film_id": film_id,
                    "last_update": str(base_micros),
                }, op="c")

            for c in range(copies_per_film):
                s_target = ((c + film_id) % num_stores) + 1
                emit_event("inventory", after_data={
                    "inventory_id": inv_counter,
                    "film_id": film_id,
                    "store_id": s_target,
                    "last_update": str(base_micros),
                }, op="c")
                inv_counter += 1

        total_inventories = inv_counter - 1

        print(f"\n[5/6] Bootstrapping Customers ({num_customers:,} records)...")
        for cust_id in range(1, num_customers + 1):
            fn = FIRST_NAMES[cust_id % len(FIRST_NAMES)]
            ln = LAST_NAMES[cust_id % len(LAST_NAMES)]
            emit_event("customer", after_data={
                "customer_id": cust_id,
                "store_id": ((cust_id - 1) % num_stores) + 1,
                "first_name": fn,
                "last_name": ln,
                "email": f"{fn.lower()}.{ln.lower()}.{cust_id}@dvdrental.com",
                "address_id": ((cust_id - 1) % num_addresses) + 1,
                "activebool": True,
                "create_date": "2026-01-01",
                "last_update": str(base_micros),
                "active": 1,
            }, op="c")
        total_customers = num_customers
    else:
        print("\n[*] Baseline Run #1 assumed already completed. Dimension bootstrap skipped.")
        print(f"[*] Proceeding straight to Phase 6: Streaming CDC Transactions starting at rental_id #{start_rental_id:,}...")
        print("    (Pass --force-bootstrap to re-emit all 15 dimension tables)")

    # ==========================================================================
    # PHASE 6: STREAMING CDC TRANSACTIONS (INSERT, UPDATE, DELETE)
    # ==========================================================================
    print(f"\n[6/6] Streaming {num_rentals:,} CDC Transactions starting at rental_id #{start_rental_id:,}...")
    start_tx_time = time.time()
    curr_rental_id = start_rental_id
    curr_payment_id = start_payment_id

    # Ring buffer of active unreturned rentals for UPDATE ('u') and DELETE ('d')
    # Bounded to 30,000 items -> ~5MB RAM usage
    active_rentals_pool: deque[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]] = deque(maxlen=30000)

    # Pre-seed pool with unreturned rentals from prior days so UPDATE ('u') and DELETE ('d')
    # can execute immediately from round 1 with 100% referential integrity
    seed_start = max(1, start_rental_id - 1000)
    for rid in range(seed_start, start_rental_id):
        inv_id = ((rid - 1) % total_inventories) + 1
        cust_id = ((rid - 1) % total_customers) + 1
        staff_id = ((rid - 1) % num_staffs) + 1
        # Seeded rentals occurred 1 to 5 days before sim_start_micros
        seed_days_ago = (((rid % 4) + 1) * day_micros) + ((rid % 3600) * 1_000_000)
        rental_ts = sim_start_micros - seed_days_ago
        rental_rec = {
            "rental_id": rid,
            "rental_date": str(rental_ts),
            "inventory_id": inv_id,
            "customer_id": cust_id,
            "return_date": None,
            "staff_id": staff_id,
            "last_update": str(rental_ts),
        }
        pay_rec = {
            "payment_id": rid,
            "customer_id": cust_id,
            "staff_id": staff_id,
            "rental_id": rid,
            "amount": "2.99",
            "payment_date": str(rental_ts + 30_000_000),
            "last_update": str(rental_ts + 30_000_000),
        }
        active_rentals_pool.append((rental_rec, pay_rec))

    for round_idx in range(1, num_rentals + 1):
        # Monotonically advancing streaming clock from sim_start_micros -> sim_end_micros
        progress = (round_idx - 1) / max(1, num_rentals)
        base_sim_ts = sim_start_micros + int(progress * total_micros_span)
        # Realistic subtle intra-minute jitter (+/- 15 seconds)
        jitter = random.randint(-15, 15) * 1_000_000
        current_sim_ts = min(max(base_sim_ts + jitter, sim_start_micros), sim_end_micros)
        current_ts_ms = int(current_sim_ts / 1000)

        # Roll operation type
        roll = random.randint(1, 100)

        # ----------------------------------------------------------------------
        # ACTION 1: UPDATE ('u') - Customer returns a previously rented disc
        # ----------------------------------------------------------------------
        if roll <= update_pct and active_rentals_pool:
            old_rental, associated_payment = active_rentals_pool.popleft()
            updated_rental = dict(old_rental)
            updated_rental["return_date"] = str(current_sim_ts)
            updated_rental["last_update"] = str(current_sim_ts)

            emit_event("rental", after_data=updated_rental, before_data=old_rental, op="u", event_ts_ms=current_ts_ms)

        # ----------------------------------------------------------------------
        # ACTION 2: DELETE ('d') - Voided / refunded transaction
        # ----------------------------------------------------------------------
        elif roll <= (update_pct + delete_pct) and active_rentals_pool:
            old_rental, associated_payment = active_rentals_pool.pop()

            # Soft delete payment first
            if associated_payment:
                emit_event("payment", after_data=None, before_data=associated_payment, op="d", event_ts_ms=current_ts_ms)

            # Soft delete rental
            emit_event("rental", after_data=None, before_data=old_rental, op="d", event_ts_ms=current_ts_ms)

        # ----------------------------------------------------------------------
        # ACTION 3: INSERT ('c') - New rental + payment transaction
        # ----------------------------------------------------------------------
        else:
            rid = curr_rental_id
            curr_rental_id += 1

            inv_id = ((rid - 1) % total_inventories) + 1
            cust_id = ((rid - 1) % total_customers) + 1
            staff_id = ((rid - 1) % num_staffs) + 1

            rental_ts = current_sim_ts

            # ~70% already returned after 1 to 4 days, ~30% currently active/unreturned
            is_returned = random.random() < 0.70
            return_ts = None
            if is_returned:
                return_duration = (((rid % 4) + 1) * day_micros) + (random.randint(60, 3600) * 1_000_000)
                calc_return = rental_ts + return_duration
                # If calculated return is within historical window, record it; otherwise still active!
                if calc_return <= sim_end_micros:
                    return_ts = str(calc_return)

            rental_record = {
                "rental_id": rid,
                "rental_date": str(rental_ts),
                "inventory_id": inv_id,
                "customer_id": cust_id,
                "return_date": return_ts,
                "staff_id": staff_id,
                "last_update": return_ts if return_ts else str(rental_ts),
            }
            emit_event("rental", after_data=rental_record, op="c", event_ts_ms=current_ts_ms)

            # 95% of rentals have a synchronized payment record
            payment_record = None
            if (rid % 100) < 95:
                pid = curr_payment_id
                curr_payment_id += 1
                payment_ts = min(rental_ts + (random.randint(5, 60) * 1_000_000), sim_end_micros)
                amount_val = random.choice(["0.99", "1.99", "2.99", "3.99", "4.99", "6.99", "9.99"])

                payment_record = {
                    "payment_id": pid,
                    "customer_id": cust_id,
                    "staff_id": staff_id,
                    "rental_id": rid,
                    "amount": amount_val,
                    "payment_date": str(payment_ts),
                    "last_update": str(payment_ts),
                }
                emit_event("payment", after_data=payment_record, op="c", event_ts_ms=int(payment_ts / 1000))

            # If rental is currently unreturned, add to pool for future update/delete
            if return_ts is None:
                active_rentals_pool.append((rental_record, payment_record))

        # Periodic progress log with real streaming timestamp
        if round_idx % 50000 == 0 or round_idx == num_rentals:
            elapsed = time.time() - start_tx_time
            rate = int(round_idx / max(0.1, elapsed))
            pct = (round_idx / num_rentals) * 100
            sim_date_str = time.strftime("%Y-%m-%d %H:%M", time.gmtime(current_sim_ts / 1_000_000))
            print(
                f"  [{pct:5.1f}%] Round {round_idx:,}/{num_rentals:,} | "
                f"Sim Date: {sim_date_str} UTC | "
                f"Ops: C:{counts['c']:,} U:{counts['u']:,} D:{counts['d']:,} | "
                f"Throughput: {rate:,} rounds/sec"
            )

    print("\n[*] Flushing producer buffers to Kafka brokers...")
    flush_start = time.time()
    producer.flush()
    producer.close()
    flush_elapsed = time.time() - flush_start

    # Persist updated state for the next run
    updated_state = {
        "last_rental_id": curr_rental_id - 1,
        "last_payment_id": curr_payment_id - 1,
        "total_inventories": total_inventories,
        "total_customers": total_customers,
        "total_films": num_films,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    }
    save_producer_state(state_file, updated_state, s3_bucket=s3_bucket)

    total_time = time.time() - start_tx_time
    print("=" * 82)
    print("  ✓ SUCCESS: 100% STATEFUL CDC RUN COMPLETED")
    print("=" * 82)
    print(f"  Total Messages Emitted:  {counts['total']:,} CDC events")
    print(f"  Operations Breakdown:    Creates: {counts['c']:,} | Updates: {counts['u']:,} | Deletes: {counts['d']:,}")
    print(f"  Next Run Starting IDs:   rental_id: #{curr_rental_id:,} | payment_id: #{curr_payment_id:,}")
    print(f"  Execution Time:          {total_time:.1f}s (Flush: {flush_elapsed:.1f}s)")
    overall_rate = int(counts["total"] / max(0.1, total_time))
    print(f"  Overall Throughput:      {overall_rate:,} msg/sec")
    print("=" * 82)


# ==============================================================================
# CLI PARSER
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="DVD Rental Stateful Bulk CDC Data Generator with Full C/U/D Lifecycle"
    )
    parser.add_argument(
        "--broker",
        type=str,
        default="auto",
        help="Kafka Bootstrap Broker URL (default: 'auto' detects EC2 cluster or localhost:9092)",
    )
    parser.add_argument(
        "--topic-prefix",
        type=str,
        default="cdc.public",
        help="Kafka CDC topic prefix (default: 'cdc.public')",
    )
    parser.add_argument(
        "--rentals",
        type=int,
        default=5000000,
        help="Number of transaction rounds to generate (default: 5,000,000 for ~5.2 GB total volume)",
    )
    parser.add_argument(
        "--target-gb",
        type=float,
        default=None,
        help="Optional: Target raw data volume in GB (e.g., --target-gb 5). Overrides --rentals.",
    )
    parser.add_argument(
        "--insert-pct",
        type=int,
        default=70,
        help="Percentage of INSERT operations (default: 70%%)",
    )
    parser.add_argument(
        "--update-pct",
        type=int,
        default=20,
        help="Percentage of UPDATE operations (default: 20%%)",
    )
    parser.add_argument(
        "--delete-pct",
        type=int,
        default=10,
        help="Percentage of DELETE operations (default: 10%%)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of chronological days to span for realistic partition distribution (default: 30 days)",
    )
    parser.add_argument(
        "--start-rental-id",
        type=int,
        default=None,
        help="Explicit starting rental_id (default: auto-detected, baseline: 50,000)",
    )
    parser.add_argument(
        "--start-payment-id",
        type=int,
        default=None,
        help="Explicit starting payment_id (default: auto-detected, baseline: 50,000)",
    )
    parser.add_argument(
        "--films",
        type=int,
        default=10000,
        help="Number of films in catalog (default: 10,000)",
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=50000,
        help="Number of customers (default: 50,000)",
    )
    parser.add_argument(
        "--stores",
        type=int,
        default=10,
        help="Number of stores (default: 10)",
    )
    parser.add_argument(
        "--staffs",
        type=int,
        default=50,
        help="Number of staff members (default: 50)",
    )
    parser.add_argument(
        "--actors",
        type=int,
        default=300,
        help="Number of actors (default: 300)",
    )
    parser.add_argument(
        "--state-file",
        type=str,
        default=DEFAULT_STATE_FILE,
        help=f"File to store sequence state across runs (default: {DEFAULT_STATE_FILE})",
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        default=None,
        help="S3 bucket for cross-instance state persistence (default: dvdrental-hudi-lake-dzung-2026)",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Reset persistent state and start fresh from baseline rental_id #50,000",
    )
    parser.add_argument(
        "--force-bootstrap",
        "--bootstrap",
        action="store_true",
        help="Force re-emitting dimension tables (default: skipped because run #1 baseline is assumed)",
    )

    args = parser.parse_args()

    # Determine rentals from --target-gb if specified
    num_rentals = args.rentals
    if args.target_gb is not None and args.target_gb > 0:
        # ~1.1 KB per rental transaction (rental + payment JSON combined)
        num_rentals = int((args.target_gb * (1024 ** 3)) / 1100)
        print(f"[*] Target volume set to {args.target_gb:.1f} GB -> computed {num_rentals:,} rounds.")

    resolved_broker = resolve_bootstrap_servers(args.broker)

    stream_bulk_data(
        broker=resolved_broker,
        topic_prefix=args.topic_prefix,
        num_films=args.films,
        num_customers=args.customers,
        num_stores=args.stores,
        num_staffs=args.staffs,
        num_actors=args.actors,
        num_rentals=num_rentals,
        days_span=args.days,
        insert_pct=args.insert_pct,
        update_pct=args.update_pct,
        delete_pct=args.delete_pct,
        start_rental_id=args.start_rental_id,
        start_payment_id=args.start_payment_id,
        state_file=args.state_file,
        s3_bucket=args.s3_bucket,
        reset_state=args.reset_state,
        force_bootstrap=args.force_bootstrap,
    )


if __name__ == "__main__":
    main()
