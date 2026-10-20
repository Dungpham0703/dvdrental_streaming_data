#!/usr/bin/env python3
"""
DVDRental Kafka Direct Pump - AWS MSK/EC2 Edition
Produces Debezium mock records for all 15 tables.
"""

import json
import logging
import multiprocessing
import os
import random
import time
from datetime import datetime, timedelta

import boto3
from kafka import KafkaProducer
from kafka.errors import KafkaError
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (Worker-%(process)d) - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def get_secret(secret_name: str) -> dict:
    logger.info(f"Fetching secret {secret_name} from AWS Secrets Manager...")
    client = boto3.client("secretsmanager", region_name="ap-southeast-2")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def load_config() -> dict:
    logger.info("Loading config from AWS Secrets Manager...")
    try:
        kafka_secret = get_secret("dvdrental/kafka_bootstrap_servers")
        bootstrap_servers = kafka_secret.get("bootstrap_servers")
        topic_prefix = kafka_secret.get("topic_prefix")
    except Exception as e:
        logger.error(f"Could not read secret after multiple retries. Error: {e}")
        raise

    config = {
        "bootstrap_servers": bootstrap_servers,
        "topic_prefix": topic_prefix,
    }
    logger.info(f"Kafka Bootstrap Servers: {config['bootstrap_servers']}")
    return config


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def get_producer(bootstrap_servers: str) -> KafkaProducer:
    logger.info("Attempting to connect to Kafka Broker...")
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=50,
        batch_size=131072,
        compression_type="lz4",
        acks="all",
        retries=5,
    )


def generate_mock_payload(
    table: str,
    current_id: int,
    ts_ms: int,
    event_time_micros: str,
    create_date: str,
    op: str = "c",
) -> dict:
    """Mock a Debezium record for the given table.

    Under REPLICA IDENTITY FULL:
    DELETE ('d') events contain the complete before-image with ALL table columns,
    __op = 'd', and __deleted = 'true'.
    """
    is_deleted = "true" if op == "d" else "false"
    base_payload = {
        "__op": op,
        "__ts_ms": ts_ms,
        "__deleted": is_deleted,
    }

    if table == "actor":
        base_payload.update(
            {
                "actor_id": current_id,
                "first_name": "John" if op != "u" else "Johnny",
                "last_name": "Doe",
                "last_update": event_time_micros,
            }
        )
    elif table == "address":
        base_payload.update(
            {
                "address_id": current_id,
                "address": "123 Main St",
                "address2": "",
                "district": "California",
                "city_id": (current_id % 600) + 1,
                "postal_code": "90210",
                "phone": "555-1234",
                "last_update": event_time_micros,
            }
        )
    elif table == "category":
        base_payload.update(
            {
                "category_id": current_id,
                "name": "Action",
                "last_update": event_time_micros,
            }
        )
    elif table == "city":
        base_payload.update(
            {
                "city_id": current_id,
                "city": "Los Angeles",
                "country_id": (current_id % 109) + 1,
                "last_update": event_time_micros,
            }
        )
    elif table == "country":
        base_payload.update(
            {
                "country_id": current_id,
                "country": "USA",
                "last_update": event_time_micros,
            }
        )
    elif table == "customer":
        base_payload.update(
            {
                "customer_id": current_id,
                "store_id": (current_id % 2) + 1,
                "first_name": "Jane",
                "last_name": "Smith",
                "email": "jane@example.com",
                "address_id": (current_id % 600) + 1,
                "activebool": False if op == "u" else True,
                "create_date": create_date,
                "last_update": event_time_micros,
                "active": 0 if op == "u" else 1,
            }
        )
    elif table == "film":
        base_payload.update(
            {
                "film_id": current_id,
                "title": "Epic Movie",
                "description": "A really epic movie.",
                "release_year": 2006,
                "language_id": 1,
                "original_language_id": None,
                "rental_duration": 3,
                "rental_rate": "4.99" if op != "u" else "5.99",
                "length": 120,
                "replacement_cost": "19.99",
                "rating": "PG-13",
                "last_update": event_time_micros,
                "special_features": "{Trailers,Commentaries}",
            }
        )
    elif table == "film_actor":
        base_payload.update(
            {
                "actor_id": current_id,
                "film_id": (current_id % 1000) + 1,
                "last_update": event_time_micros,
            }
        )
    elif table == "film_category":
        base_payload.update(
            {
                "film_id": current_id,
                "category_id": (current_id % 16) + 1,
                "last_update": event_time_micros,
            }
        )
    elif table == "inventory":
        base_payload.update(
            {
                "inventory_id": current_id,
                "film_id": (current_id % 1000) + 1,
                "store_id": (current_id % 2) + 1,
                "last_update": event_time_micros,
            }
        )
    elif table == "language":
        base_payload.update(
            {
                "language_id": current_id,
                "name": "English",
                "last_update": event_time_micros,
            }
        )
    elif table == "payment":
        base_payload.update(
            {
                "payment_id": current_id,
                "customer_id": (current_id % 599) + 1,
                "staff_id": (current_id % 2) + 1,
                "rental_id": current_id,
                "amount": str(round(random.uniform(0.99, 49.99), 2)),
                "payment_date": event_time_micros,
            }
        )
    elif table == "rental":
        base_payload.update(
            {
                "rental_id": current_id,
                "rental_date": event_time_micros,
                "inventory_id": (current_id % 4581) + 1,
                "customer_id": (current_id % 599) + 1,
                "return_date": event_time_micros if op == "u" else None,
                "staff_id": (current_id % 2) + 1,
                "last_update": event_time_micros,
            }
        )
    elif table == "staff":
        base_payload.update(
            {
                "staff_id": current_id,
                "first_name": "Mike",
                "last_name": "Hillyer",
                "address_id": 3,
                "email": "mike@example.com",
                "store_id": 1,
                "active": True,
                "username": "mike",
                "password": "password",
                "last_update": event_time_micros,
                "picture": None,
            }
        )
    elif table == "store":
        base_payload.update(
            {
                "store_id": current_id,
                "manager_staff_id": current_id,
                "address_id": current_id,
                "last_update": event_time_micros,
            }
        )

    return base_payload


# === WORKER FUNCTION ===
def pump_worker(
    worker_id: int,
    bootstrap_servers: str,
    topic_prefix: str,
    messages_per_worker: int,
    stop_event: multiprocessing.Event,
):
    producer = get_producer(bootstrap_servers)

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

    topics = {t: f"{topic_prefix}.{t}" for t in tables}

    op_insert_pct = int(os.environ.get("OP_INSERT_PCT", "70"))
    op_update_pct = int(os.environ.get("OP_UPDATE_PCT", "20"))
    op_delete_pct = int(os.environ.get("OP_DELETE_PCT", "10"))
    log_interval = int(os.environ.get("LOG_INTERVAL_ROUNDS", "1000"))

    start_id = worker_id * messages_per_worker + 1
    current_max_id = start_id
    count = 0
    start_time = time.time()

    c_count = 0
    u_count = 0
    d_count = 0

    logger.info(
        f"Started Worker-{worker_id}: Target {messages_per_worker:,} rounds | "
        f"Distribution: C={op_insert_pct}%, U={op_update_pct}%, D={op_delete_pct}% (REPLICA IDENTITY FULL)"
    )

    try:
        while count < messages_per_worker and not stop_event.is_set():
            now_sec = time.time()
            ts_ms = int(now_sec * 1000)
            ts_micros_str = str(int(now_sec * 1000000))
            create_date_str = str(int(now_sec / 86400))

            # Initial seed rounds: 100% INSERT to create baseline records
            if (current_max_id - start_id) < 50:
                op = "c"
            else:
                roll = random.randint(1, 100)
                if roll <= op_insert_pct:
                    op = "c"
                elif roll <= (op_insert_pct + op_update_pct):
                    op = "u"
                else:
                    op = "d"

            if op == "c":
                current_id = current_max_id
                current_max_id += 1
                c_count += 1
            elif op == "u":
                current_id = random.randint(start_id, current_max_id - 1)
                u_count += 1
            else:
                current_id = random.randint(start_id, current_max_id - 1)
                d_count += 1

            for table in tables:
                payload = generate_mock_payload(
                    table=table,
                    current_id=current_id,
                    ts_ms=ts_ms,
                    event_time_micros=ts_micros_str,
                    create_date=create_date_str,
                    op=op,
                )

                if table == "film_actor":
                    key = f"{payload['actor_id']},{payload['film_id']}"
                elif table == "film_category":
                    key = f"{payload['film_id']},{payload['category_id']}"
                else:
                    key = str(current_id)

                try:
                    producer.send(topics[table], key=key, value=payload)
                except KafkaError as e:
                    logger.error(f"Failed to send message to {topics[table]}: {e}")

            count += 1

            if count % log_interval == 0:
                producer.flush()
                now = time.time()
                elapsed = now - start_time
                rate = (count * 15) / elapsed if elapsed > 0 else 0
                pct = (count / messages_per_worker) * 100
                remaining_rounds = messages_per_worker - count
                eta_s = (remaining_rounds * 15) / rate if rate > 0 else 0
                eta_m = int(eta_s // 60)
                eta_rem_s = int(eta_s % 60)

                logger.info(
                    f"[{pct:5.1f}%] {count:,}/{messages_per_worker:,} rounds | "
                    f"{rate:,.0f} msg/s | Elapsed: {elapsed:.1f}s | ETA: {eta_m}m{eta_rem_s}s | "
                    f"Ops: C:{c_count:,} ({(c_count/count)*100:.1f}%) "
                    f"U:{u_count:,} ({(u_count/count)*100:.1f}%) "
                    f"D:{d_count:,} ({(d_count/count)*100:.1f}%)"
                )

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        logger.info(
            f"Worker-{worker_id} Finished: {count * 15:,} msgs in {elapsed:.1f}s | "
            f"Final Ops: C={c_count:,}, U={u_count:,}, D={d_count:,}"
        )


def main():
    config = load_config()

    TARGET_ROUNDS = int(os.environ.get("TARGET_ROUNDS", "1_000_000"))
    WORKERS = int(os.environ.get("WORKERS", "4"))
    OP_INSERT_PCT = int(os.environ.get("OP_INSERT_PCT", "70"))
    OP_UPDATE_PCT = int(os.environ.get("OP_UPDATE_PCT", "20"))
    OP_DELETE_PCT = int(os.environ.get("OP_DELETE_PCT", "10"))

    logger.info("=" * 65)
    logger.info("DVDRENTAL KAFKA PUMP - 15 TABLES (CDC REPLICA IDENTITY FULL)")
    logger.info(f"Broker:         {config['bootstrap_servers']}")
    logger.info(f"Topic Prefix:   {config['topic_prefix']}")
    logger.info(f"Target:         {TARGET_ROUNDS:,} rounds ({TARGET_ROUNDS * 15:,} messages)")
    logger.info(f"Workers:        {WORKERS}")
    logger.info(f"Ops Mix:        Insert: {OP_INSERT_PCT}% | Update: {OP_UPDATE_PCT}% | Delete: {OP_DELETE_PCT}%")
    logger.info("=" * 65)

    stop_event = multiprocessing.Event()
    msgs_per_worker = TARGET_ROUNDS // WORKERS

    workers = []
    for i in range(WORKERS):
        p = multiprocessing.Process(
            target=pump_worker,
            args=(
                i,
                config["bootstrap_servers"],
                config["topic_prefix"],
                msgs_per_worker,
                stop_event,
            ),
        )
        p.start()
        workers.append(p)

    try:
        for p in workers:
            p.join()
    except KeyboardInterrupt:
        logger.warning("Stopping all workers...")
        stop_event.set()
        for p in workers:
            p.join()

    logger.info("All workers completed successfully!")


if __name__ == "__main__":
    main()
