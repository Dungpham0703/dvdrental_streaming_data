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

# Setup Production-grade Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] (Worker-%(process)d) - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# === READ CONFIG FROM AWS SECRETS MANAGER ===
@retry(
    stop=stop_after_attempt(5), 
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
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
        raise # Fail-fast principle in Production
    
    config = {
        "bootstrap_servers": bootstrap_servers,
        "topic_prefix": topic_prefix,
    }
    logger.info(f"Kafka Bootstrap Servers: {config['bootstrap_servers']}")
    return config


# === KAFKA PRODUCER ===
@retry(
    stop=stop_after_attempt(5), 
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def get_producer(bootstrap_servers: str) -> KafkaProducer:
    logger.info("Attempting to connect to Kafka Broker...")
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=50,         # Batching for higher throughput
        batch_size=131072,    # 128KB batch
        compression_type="lz4",
        acks="all",           # PRODUCTION: Changed from 1 to 'all' for Exactly-once/No data loss
        retries=5,            # PRODUCTION: Allow Kafka client to retry internally
    )


def generate_mock_payload(table: str, current_id: int, ts_ms: int, event_time: str) -> dict:
    """Mock a Debezium record for the given table."""
    base_payload = {
        "__op": "c",
        "__ts_ms": ts_ms,
        "__deleted": "false",
    }
    
    if table == "actor":
        base_payload.update({"actor_id": current_id, "first_name": "John", "last_name": "Doe", "last_update": event_time})
    elif table == "address":
        base_payload.update({"address_id": current_id, "address": "123 Main St", "address2": "", "district": "California", "city_id": (current_id % 600) + 1, "postal_code": "90210", "phone": "555-1234", "last_update": event_time})
    elif table == "category":
        base_payload.update({"category_id": current_id, "name": "Action", "last_update": event_time})
    elif table == "city":
        base_payload.update({"city_id": current_id, "city": "Los Angeles", "country_id": (current_id % 109) + 1, "last_update": event_time})
    elif table == "country":
        base_payload.update({"country_id": current_id, "country": "USA", "last_update": event_time})
    elif table == "customer":
        base_payload.update({"customer_id": current_id, "store_id": (current_id % 2) + 1, "first_name": "Jane", "last_name": "Smith", "email": "jane@example.com", "address_id": (current_id % 600) + 1, "activebool": True, "create_date": event_time, "last_update": event_time, "active": 1})
    elif table == "film":
        base_payload.update({"film_id": current_id, "title": "Epic Movie", "description": "A really epic movie.", "release_year": 2006, "language_id": 1, "original_language_id": None, "rental_duration": 3, "rental_rate": "4.99", "length": 120, "replacement_cost": "19.99", "rating": "PG-13", "last_update": event_time, "special_features": "{Trailers,Commentaries}"})
    elif table == "film_actor":
        base_payload.update({"actor_id": current_id, "film_id": (current_id % 1000) + 1, "last_update": event_time})
    elif table == "film_category":
        base_payload.update({"film_id": current_id, "category_id": (current_id % 16) + 1, "last_update": event_time})
    elif table == "inventory":
        base_payload.update({"inventory_id": current_id, "film_id": (current_id % 1000) + 1, "store_id": (current_id % 2) + 1, "last_update": event_time})
    elif table == "language":
        base_payload.update({"language_id": current_id, "name": "English", "last_update": event_time})
    elif table == "payment":
        base_payload.update({"payment_id": current_id, "customer_id": (current_id % 599) + 1, "staff_id": (current_id % 2) + 1, "rental_id": current_id, "amount": str(round(random.uniform(0.99, 49.99), 2)), "payment_date": event_time})
    elif table == "rental":
        base_payload.update({"rental_id": current_id, "rental_date": event_time, "inventory_id": (current_id % 4581) + 1, "customer_id": (current_id % 599) + 1, "return_date": None, "staff_id": (current_id % 2) + 1, "last_update": event_time})
    elif table == "staff":
        base_payload.update({"staff_id": current_id, "first_name": "Mike", "last_name": "Hillyer", "address_id": 3, "email": "mike@example.com", "store_id": 1, "active": True, "username": "mike", "password": "password", "last_update": event_time, "picture": None})
    elif table == "store":
        base_payload.update({"store_id": current_id, "manager_staff_id": current_id, "address_id": current_id, "last_update": event_time})
    
    return base_payload


# === WORKER FUNCTION ===
def pump_worker(worker_id: int, bootstrap_servers: str, topic_prefix: str,
                messages_per_worker: int, stop_event: multiprocessing.Event):
    producer = get_producer(bootstrap_servers)
    
    tables = [
        "actor", "address", "category", "city", "country", "customer",
        "film", "film_actor", "film_category", "inventory", "language",
        "payment", "rental", "staff", "store"
    ]
    
    # Pre-compute topic names
    topics = {t: f"{topic_prefix}.{t}" for t in tables}

    start_id = worker_id * messages_per_worker
    count = 0
    start_time = time.time()
    logger.info(f"Started pumping {messages_per_worker:,} messages for 15 tables...")

    try:
        while count < messages_per_worker and not stop_event.is_set():
            ts_ms = int(time.time() * 1000)
            event_time = (datetime.now() - timedelta(minutes=random.randint(1, 10000))).isoformat()
            
            current_id = start_id + count

            for table in tables:
                payload = generate_mock_payload(table, current_id, ts_ms, event_time)
                
                # Determine key (use id or composite id)
                if table == "film_actor":
                    key = f"{payload['actor_id']},{payload['film_id']}"
                elif table == "film_category":
                    key = f"{payload['film_id']},{payload['category_id']}"
                else:
                    key = str(current_id)

                try:
                    # Fire and forget with callback for error logging
                    future = producer.send(topics[table], key=key, value=payload)
                    # Optional: wait for it to succeed if strict ordering is required
                    # future.get(timeout=10)
                except KafkaError as e:
                    logger.error(f"Failed to send message to {topics[table]}: {e}")

            count += 1

            if count % 1000 == 0:
                producer.flush()
                elapsed = time.time() - start_time
                rate = (count * 15) / elapsed if elapsed > 0 else 0
                logger.info(f"{count:,} rounds | {rate:.0f} msg/s | Elapsed: {elapsed:.1f}s")

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        logger.info(f"Done! Total {count * 15:,} messages in {elapsed:.1f}s")


# === MAIN ===
def main():
    config = load_config()

    TARGET_ROUNDS = int(os.environ.get("TARGET_ROUNDS", "1_000_000"))
    WORKERS = int(os.environ.get("WORKERS", "4"))

    logger.info("=" * 65)
    logger.info(f"DVDRENTAL KAFKA PUMP - 15 TABLES")
    logger.info(f"Broker: {config['bootstrap_servers']}")
    logger.info(f"Target: {TARGET_ROUNDS:,} rounds per table")
    logger.info(f"Workers: {WORKERS}")
    logger.info("=" * 65)

    stop_event = multiprocessing.Event()
    msgs_per_worker = TARGET_ROUNDS // WORKERS

    workers = []
    for i in range(WORKERS):
        p = multiprocessing.Process(
            target=pump_worker,
            args=(i, config["bootstrap_servers"], config["topic_prefix"],
                  msgs_per_worker, stop_event),
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

    logger.info("All done!")


if __name__ == "__main__":
    main()
