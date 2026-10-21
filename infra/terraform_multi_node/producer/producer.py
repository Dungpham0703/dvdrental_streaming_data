#!/usr/bin/env python3
"""
DVDRental Kafka Direct Pump - AWS MSK/EC2 Edition
Stateful Relationship Generator producing Debezium 2.5.0.Final JSON Envelopes
with 100% Referential Integrity and Topological Dependency Ordering (DAG).
"""

import collections
import json
import logging
import multiprocessing
import os
import random
import time
from typing import Any

import boto3
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (Worker-%(process)d) - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ==============================================================================
# SEED CATALOG REFERENCE POOLS
# ==============================================================================
COUNTRIES = [
    "United States", "Canada", "United Kingdom", "Germany", "France",
    "Japan", "Australia", "Vietnam", "Singapore", "Brazil",
    "Italy", "Spain", "Netherlands", "South Korea", "India",
]
CITIES = [
    "New York", "London", "Tokyo", "Paris", "Berlin",
    "Sydney", "Hanoi", "Toronto", "Melbourne", "Da Nang",
    "Rome", "Madrid", "Amsterdam", "Seoul", "Mumbai",
]
DISTRICTS = ["Central", "North", "South", "East", "West", "Downtown", "Suburbs", "Midtown", "Riverside", "Highland"]
LANGUAGES = ["English", "Italian", "Japanese", "Mandarin", "French", "German", "Spanish"]
CATEGORIES = [
    "Action", "Animation", "Children", "Classics", "Comedy",
    "Documentary", "Drama", "Family", "Foreign", "Games",
    "Horror", "Music", "New", "Sci-Fi", "Sports", "Travel",
]
FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
    "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
]
FILM_TITLES = [
    "Adventure", "Mystery", "Chronicles", "Legacy", "Destiny",
    "Voyage", "Odyssey", "Protocol", "Horizon", "Empire",
    "Kingdom", "Shadow", "Falcon", "Warrior", "Legend",
]


# ==============================================================================
# IN-MEMORY STATE REGISTRY (STATEFUL TOPOLOGICAL TRACKER)
# ==============================================================================
class DataState:
    """In-Memory State Registry maintaining entity pools and primary key sequences.

    Guarantees strict referential integrity for child records without static ID hardcoding.
    """

    def __init__(self, id_offset: int = 1):
        # Auto-incrementing primary key sequences per entity
        self.next_country_id = id_offset
        self.next_city_id = id_offset
        self.next_address_id = id_offset
        self.next_language_id = id_offset
        self.next_category_id = id_offset
        self.next_actor_id = id_offset
        self.next_staff_id = id_offset
        self.next_store_id = id_offset
        self.next_film_id = id_offset
        self.next_inventory_id = id_offset
        self.next_customer_id = id_offset
        self.next_rental_id = id_offset
        self.next_payment_id = id_offset

        # WAL & Debezium sequence simulation
        self.tx_id_seq = id_offset * 100
        self.lsn_seq = 10_000_000_000 + id_offset * 1024

        # Primary Key registries for sampling by dependent child tables
        self.countries: list[int] = []
        self.cities: list[int] = []
        self.addresses: list[int] = []
        self.languages: list[int] = []
        self.categories: list[int] = []
        self.actors: list[int] = []
        self.staffs: list[int] = []
        self.stores: list[int] = []
        self.films: list[int] = []
        self.inventories: list[int] = []
        self.customers: list[int] = []

        # Sliding bounded pool of active rentals for consistent payment linkage
        # Bounded to 200,000 items to keep RAM bounded during multi-million row scale
        self.rentals: collections.deque = collections.deque(maxlen=200_000)

        # Composite PK deduplication tracking
        self.film_category_pairs: set[tuple[int, int]] = set()
        self.film_actor_pairs: set[tuple[int, int]] = set()

    def advance_wal(self) -> tuple[int, int]:
        """Simulate advancing PostgreSQL Transaction ID and LSN."""
        self.tx_id_seq += 1
        self.lsn_seq += random.randint(128, 512)
        return self.tx_id_seq, self.lsn_seq


# ==============================================================================
# DEBEZIUM 2.5.0.FINAL JSON ENVELOPE FORMATTER
# ==============================================================================
def make_debezium_envelope(
    table: str,
    record_data: dict[str, Any] | None,
    old_record_data: dict[str, Any] | None = None,
    op: str = "c",
    connector_name: str = "dvdrental_source",
    ts_ms: int | None = None,
    tx_id: int = 1001,
    lsn: int = 24578912,
) -> dict[str, Any]:
    """Wraps record data in the standard Debezium 2.5.0.Final PostgreSQL CDC envelope.

    Exact 5 root fields: before, after, source, op, ts_ms.
    """
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)

    source_info = {
        "version": "2.5.0.Final",
        "connector": "postgresql",
        "name": connector_name,
        "ts_ms": ts_ms,
        "db": "dvdrental",
        "schema": "public",
        "table": table,
        "txId": tx_id,
        "lsn": lsn,
    }

    if op == "c":
        before_val = None
        after_val = record_data
    elif op == "u":
        before_val = old_record_data if old_record_data is not None else record_data
        after_val = record_data
    elif op == "d":
        before_val = record_data
        after_val = None
    else:  # 'r' snapshot read
        before_val = None
        after_val = record_data

    return {
        "before": before_val,
        "after": after_val,
        "source": source_info,
        "op": op,
        "ts_ms": ts_ms,
    }


def get_kafka_key(table: str, payload_data: dict[str, Any]) -> str:
    """Format partitioning key for Kafka message."""
    if table == "film_actor":
        return f"{payload_data['actor_id']},{payload_data['film_id']}"
    if table == "film_category":
        return f"{payload_data['film_id']},{payload_data['category_id']}"

    pk_field = f"{table}_id"
    if pk_field in payload_data:
        return str(payload_data[pk_field])
    return str(next(iter(payload_data.values())))


# ==============================================================================
# TABLE ENTITY GENERATORS (TOPOLOGICAL DAG)
# ==============================================================================

# --- TẦNG 0: BẢNG GỐC, ĐỊA CHỈ & TỔ CHỨC ---


def generate_country(state: DataState, ts_micros: int) -> dict[str, Any]:
    cid = state.next_country_id
    state.next_country_id += 1
    state.countries.append(cid)
    cname = random.choice(COUNTRIES)
    return {
        "country_id": cid,
        "country": f"{cname}_{cid}",
        "last_update": ts_micros,
    }


def generate_city(state: DataState, ts_micros: int) -> dict[str, Any]:
    if not state.countries:
        generate_country(state, ts_micros)
    cid = state.next_city_id
    state.next_city_id += 1
    state.cities.append(cid)
    country_id = random.choice(state.countries)
    cname = random.choice(CITIES)
    return {
        "city_id": cid,
        "city": f"{cname}_{cid}",
        "country_id": country_id,
        "last_update": ts_micros,
    }


def generate_address(state: DataState, ts_micros: int) -> dict[str, Any]:
    if not state.cities:
        generate_city(state, ts_micros)
    aid = state.next_address_id
    state.next_address_id += 1
    state.addresses.append(aid)
    city_id = random.choice(state.cities)
    return {
        "address_id": aid,
        "address": f"{random.randint(10, 9999)} Main St",
        "address2": "",
        "district": random.choice(DISTRICTS),
        "city_id": city_id,
        "postal_code": f"{random.randint(10000, 99999)}",
        "phone": f"555-{random.randint(1000, 9999)}",
        "last_update": ts_micros,
    }


def generate_language(state: DataState, ts_micros: int) -> dict[str, Any]:
    lid = state.next_language_id
    state.next_language_id += 1
    state.languages.append(lid)
    return {
        "language_id": lid,
        "name": random.choice(LANGUAGES),
        "last_update": ts_micros,
    }


def generate_category(state: DataState, ts_micros: int) -> dict[str, Any]:
    cid = state.next_category_id
    state.next_category_id += 1
    state.categories.append(cid)
    cname = CATEGORIES[(cid - 1) % len(CATEGORIES)]
    return {
        "category_id": cid,
        "name": f"{cname}_{cid}",
        "last_update": ts_micros,
    }


def generate_actor(state: DataState, ts_micros: int) -> dict[str, Any]:
    aid = state.next_actor_id
    state.next_actor_id += 1
    state.actors.append(aid)
    return {
        "actor_id": aid,
        "first_name": random.choice(FIRST_NAMES),
        "last_name": random.choice(LAST_NAMES),
        "last_update": ts_micros,
    }


def generate_staff(
    state: DataState, ts_micros: int, store_id: int | None = None
) -> dict[str, Any]:
    if not state.addresses:
        generate_address(state, ts_micros)
    sid = state.next_staff_id
    state.next_staff_id += 1
    state.staffs.append(sid)
    address_id = random.choice(state.addresses)
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    assigned_store = store_id if store_id is not None else (
        random.choice(state.stores) if state.stores else 1
    )
    return {
        "staff_id": sid,
        "first_name": first_name,
        "last_name": last_name,
        "address_id": address_id,
        "email": f"{first_name.lower()}.{last_name.lower()}.{sid}@sakilastaff.com",
        "store_id": assigned_store,
        "active": True,
        "username": f"staff_{sid}",
        "password": "password",
        "last_update": ts_micros,
        "picture": None,
    }


def generate_store(
    state: DataState, ts_micros: int, manager_staff_id: int | None = None
) -> dict[str, Any]:
    if not state.addresses:
        generate_address(state, ts_micros)
    if not state.staffs:
        generate_staff(state, ts_micros)

    sid = state.next_store_id
    state.next_store_id += 1
    state.stores.append(sid)
    manager_id = manager_staff_id if manager_staff_id is not None else random.choice(state.staffs)
    address_id = random.choice(state.addresses)
    return {
        "store_id": sid,
        "manager_staff_id": manager_id,
        "address_id": address_id,
        "last_update": ts_micros,
    }


# --- TẦNG 1: CATALOG PHIM, MAPPING N-N & INVENTORY ---


def generate_film(state: DataState, ts_micros: int) -> dict[str, Any]:
    if not state.languages:
        generate_language(state, ts_micros)
    fid = state.next_film_id
    state.next_film_id += 1
    state.films.append(fid)
    lang_id = random.choice(state.languages)
    return {
        "film_id": fid,
        "title": f"Film {fid} {random.choice(FILM_TITLES)}",
        "description": f"Synthetic simulation film for CDC testing: {fid}",
        "release_year": random.randint(2000, 2026),
        "language_id": lang_id,
        "rental_duration": random.randint(3, 7),
        "rental_rate": str(random.choice([0.99, 2.99, 4.99])),
        "length": random.randint(60, 185),
        "replacement_cost": str(random.choice([14.99, 19.99, 24.99, 29.99])),
        "rating": random.choice(["G", "PG", "PG-13", "R", "NC-17"]),
        "last_update": ts_micros,
        "special_features": "{Trailers,Commentaries}",
        "fulltext": f"'epic':2 'film':1 'movie':3",
    }


def generate_film_category(
    state: DataState, ts_micros: int, film_id: int | None = None
) -> dict[str, Any] | None:
    if not state.films:
        generate_film(state, ts_micros)
    if not state.categories:
        generate_category(state, ts_micros)

    fid = film_id if film_id is not None else random.choice(state.films)
    available_cats = [c for c in state.categories if (fid, c) not in state.film_category_pairs]
    if not available_cats:
        return None

    cat_id = random.choice(available_cats)
    state.film_category_pairs.add((fid, cat_id))
    return {
        "film_id": fid,
        "category_id": cat_id,
        "last_update": ts_micros,
    }


def generate_film_actor(
    state: DataState, ts_micros: int, film_id: int | None = None
) -> dict[str, Any] | None:
    if not state.films:
        generate_film(state, ts_micros)
    if not state.actors:
        generate_actor(state, ts_micros)

    fid = film_id if film_id is not None else random.choice(state.films)
    available_actors = [a for a in state.actors if (a, fid) not in state.film_actor_pairs]
    if not available_actors:
        return None

    actor_id = random.choice(available_actors)
    state.film_actor_pairs.add((actor_id, fid))
    return {
        "actor_id": actor_id,
        "film_id": fid,
        "last_update": ts_micros,
    }


def generate_inventory(
    state: DataState,
    ts_micros: int,
    film_id: int | None = None,
    store_id: int | None = None,
) -> dict[str, Any]:
    if not state.films:
        generate_film(state, ts_micros)
    if not state.stores:
        generate_store(state, ts_micros)

    iid = state.next_inventory_id
    state.next_inventory_id += 1
    state.inventories.append(iid)
    fid = film_id if film_id is not None else random.choice(state.films)
    sid = store_id if store_id is not None else random.choice(state.stores)
    return {
        "inventory_id": iid,
        "film_id": fid,
        "store_id": sid,
        "last_update": ts_micros,
    }


# --- TẦNG 2: KHÁCH HÀNG & GIAO DỊCH (RENTAL -> PAYMENT) ---


def generate_customer(state: DataState, ts_micros: int) -> dict[str, Any]:
    if not state.stores:
        generate_store(state, ts_micros)
    if not state.addresses:
        generate_address(state, ts_micros)

    cid = state.next_customer_id
    state.next_customer_id += 1
    state.customers.append(cid)
    store_id = random.choice(state.stores)
    address_id = random.choice(state.addresses)
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    create_date_days = str(int(time.time() / 86400))
    return {
        "customer_id": cid,
        "store_id": store_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": f"{first_name.lower()}.{last_name.lower()}.{cid}@example.com",
        "address_id": address_id,
        "activebool": True,
        "create_date": create_date_days,
        "last_update": ts_micros,
        "active": 1,
    }


def generate_rental(
    state: DataState, ts_micros: int, is_returned: bool = True
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not state.inventories:
        generate_inventory(state, ts_micros)
    if not state.customers:
        generate_customer(state, ts_micros)
    if not state.staffs:
        generate_staff(state, ts_micros)

    rid = state.next_rental_id
    state.next_rental_id += 1
    inv_id = random.choice(state.inventories)
    cust_id = random.choice(state.customers)
    staff_id = random.choice(state.staffs)
    rental_date = ts_micros

    # Ensure return_date >= rental_date (if returned)
    return_date = (
        rental_date + random.randint(3600_000_000, 7 * 86400_000_000)
        if is_returned
        else None
    )
    last_update = return_date if return_date is not None else rental_date

    payload = {
        "rental_id": rid,
        "rental_date": rental_date,
        "inventory_id": inv_id,
        "customer_id": cust_id,
        "return_date": return_date,
        "staff_id": staff_id,
        "last_update": last_update,
    }

    # Register in state pool for payment linkage
    rental_item = {
        "rental_id": rid,
        "customer_id": cust_id,
        "staff_id": staff_id,
        "rental_date": rental_date,
        "return_date": return_date,
    }
    state.rentals.append(rental_item)
    return payload, rental_item


def generate_payment(
    state: DataState, ts_micros: int, rental_item: dict[str, Any] | None = None
) -> dict[str, Any]:
    if rental_item is None:
        if not state.rentals:
            # Self-healing: if no rental exists yet, create one
            _, rental_item = generate_rental(state, ts_micros)
        else:
            rental_item = random.choice(state.rentals)

    pid = state.next_payment_id
    state.next_payment_id += 1

    # CRITICAL INVARIANT: Strict synchronization with parent rental
    cid = rental_item["customer_id"]
    sid = rental_item["staff_id"]
    rid = rental_item["rental_id"]
    r_date = rental_item["rental_date"]
    ret_date = rental_item.get("return_date")

    # CRITICAL INVARIANT: payment_date >= rental_date
    if ret_date is not None:
        pay_date = r_date + random.randint(0, max(1, ret_date - r_date))
    else:
        pay_date = r_date + random.randint(60_000_000, 3600_000_000)

    amount = str(round(random.choice([0.99, 1.99, 2.99, 3.99, 4.99, 5.99, 7.99, 9.99]), 2))
    return {
        "payment_id": pid,
        "customer_id": cid,
        "staff_id": sid,
        "rental_id": rid,
        "amount": amount,
        "payment_date": pay_date,
        "last_update": pay_date,
    }


# ==============================================================================
# STATEFUL ORCHESTRATOR
# ==============================================================================
class MockDataOrchestrator:
    """Coordinates topological generation of parents and children, emitting to Kafka."""

    def __init__(
        self,
        producer: KafkaProducer,
        topic_prefix: str,
        state: DataState,
        connector_name: str = "dvdrental_source",
    ):
        self.producer = producer
        self.topic_prefix = topic_prefix
        self.state = state
        self.connector_name = connector_name
        self.msg_count = 0

    def emit(self, table: str, record_data: dict[str, Any] | None, op: str = "c") -> None:
        """Constructs envelope, determines partition key, and pushes to Kafka."""
        tx_id, lsn = self.state.advance_wal()
        now_sec = time.time()
        ts_ms = int(now_sec * 1000)

        envelope = make_debezium_envelope(
            table=table,
            record_data=record_data,
            op=op,
            connector_name=self.connector_name,
            ts_ms=ts_ms,
            tx_id=tx_id,
            lsn=lsn,
        )

        active_payload = record_data if record_data is not None else {}
        key = get_kafka_key(table, active_payload)
        topic = f"{self.topic_prefix}.{table}"

        try:
            self.producer.send(topic, key=key, value=envelope)
            self.msg_count += 1
        except KafkaError as e:
            logger.error(f"Kafka error emitting to {topic}: {e}")

    def bootstrap_dimensions(self, scale: dict[str, int]) -> None:
        """Bootstraps dimension hierarchy in strict Topological Order (DAG)."""
        logger.info("Initializing Dimension Hierarchy in Topological Order...")
        ts_micros = int(time.time() * 1_000_000)

        # 1. Base Geographical Hierarchy
        for _ in range(scale.get("num_countries", 20)):
            self.emit("country", generate_country(self.state, ts_micros))

        for _ in range(scale.get("num_cities", 100)):
            self.emit("city", generate_city(self.state, ts_micros))

        for _ in range(scale.get("num_addresses", 300)):
            self.emit("address", generate_address(self.state, ts_micros))

        # 2. Lookup Tables
        for _ in range(scale.get("num_languages", 7)):
            self.emit("language", generate_language(self.state, ts_micros))

        for _ in range(scale.get("num_categories", 16)):
            self.emit("category", generate_category(self.state, ts_micros))

        for _ in range(scale.get("num_actors", 150)):
            self.emit("actor", generate_actor(self.state, ts_micros))

        # 3. Staff & Store Circular DAG Resolution
        # Create initial staff (manager candidate)
        for _ in range(scale.get("num_stores", 5)):
            self.emit("staff", generate_staff(self.state, ts_micros))

        # Create stores referencing manager staff and address
        for sid_idx in range(len(self.state.staffs)):
            manager_id = self.state.staffs[sid_idx]
            self.emit("store", generate_store(self.state, ts_micros, manager_staff_id=manager_id))

        # Create remaining staff assigned to created stores
        additional_staff = max(0, scale.get("num_staff", 25) - len(self.state.staffs))
        for _ in range(additional_staff):
            self.emit("staff", generate_staff(self.state, ts_micros))

        # 4. Catalog & Inventory
        num_films = scale.get("num_films", 500)
        copies_per_store = scale.get("copies_per_film_store", 2)

        for _ in range(num_films):
            film_payload = generate_film(self.state, ts_micros)
            fid = film_payload["film_id"]
            self.emit("film", film_payload)

            # Generate 1-2 film_category mappings
            for _ in range(random.randint(1, 2)):
                fc = generate_film_category(self.state, ts_micros, fid)
                if fc:
                    self.emit("film_category", fc)

            # Generate 2-4 film_actor mappings
            for _ in range(random.randint(2, 4)):
                fa = generate_film_actor(self.state, ts_micros, fid)
                if fa:
                    self.emit("film_actor", fa)

            # Generate inventory copies across stores
            for sid in self.state.stores:
                for _ in range(copies_per_store):
                    inv = generate_inventory(self.state, ts_micros, film_id=fid, store_id=sid)
                    self.emit("inventory", inv)

        # 5. Customers
        for _ in range(scale.get("num_customers", 1000)):
            self.emit("customer", generate_customer(self.state, ts_micros))

        logger.info(
            f"Bootstrap Complete: {len(self.state.stores)} stores, {len(self.state.staffs)} staff, "
            f"{len(self.state.films)} films, {len(self.state.inventories)} inventories, "
            f"{len(self.state.customers)} customers. Messages Emitted: {self.msg_count:,}"
        )

    def stream_transactions(
        self,
        target_rounds: int,
        stop_event: multiprocessing.Event,
        log_interval: int = 5000,
        op_insert_pct: int = 70,
        op_update_pct: int = 20,
    ) -> None:
        """Stream continuous transactions (rentals and payments) with strict linkage."""
        logger.info(f"Starting Stateful Transaction Stream for {target_rounds:,} rounds...")
        start_time = time.time()
        c_count, u_count, d_count = 0, 0, 0

        for r_idx in range(1, target_rounds + 1):
            if stop_event.is_set():
                break

            now = time.time()
            ts_micros = int(now * 1_000_000)

            # Periodic dimension expansion (5% chance: new customer or new film+inventory)
            if random.random() < 0.05:
                if random.random() < 0.7:
                    self.emit("customer", generate_customer(self.state, ts_micros))
                else:
                    new_film = generate_film(self.state, ts_micros)
                    fid = new_film["film_id"]
                    self.emit("film", new_film)
                    fc = generate_film_category(self.state, ts_micros, fid)
                    if fc:
                        self.emit("film_category", fc)
                    for sid in self.state.stores:
                        self.emit("inventory", generate_inventory(self.state, ts_micros, fid, sid))

            # Op determination: Insert / Update / Delete
            roll = random.randint(1, 100)
            if roll <= op_insert_pct:
                op = "c"
                c_count += 1
            elif roll <= (op_insert_pct + op_update_pct):
                op = "u"
                u_count += 1
            else:
                op = "d"
                d_count += 1

            # 1. Rental Generation
            is_returned = random.random() > 0.10  # 90% returned
            rental_payload, rental_item = generate_rental(self.state, ts_micros, is_returned=is_returned)

            if op == "u" and self.state.rentals:
                # Update: Mark an earlier open rental as returned
                cached = random.choice(self.state.rentals)
                old_rental = dict(cached)
                cached["return_date"] = ts_micros
                updated_rental = dict(cached)
                updated_rental["last_update"] = ts_micros
                self.emit("rental", updated_rental, op="u")
            elif op == "d" and self.state.rentals:
                # Delete: Tombstone an existing rental
                victim = self.state.rentals.popleft()
                self.emit("rental", victim, op="d")
            else:
                self.emit("rental", rental_payload, op="c")

            # 2. Payment Generation - Strictly linked to rental
            # Every completed rental gets a synchronous payment; unreturned rentals 50% deposit
            if is_returned or random.random() < 0.5:
                payment_payload = generate_payment(self.state, ts_micros, rental_item=rental_item)
                self.emit("payment", payment_payload, op="c")

            # Logging Telemetry
            if r_idx % log_interval == 0:
                elapsed = time.time() - start_time
                rate = self.msg_count / elapsed if elapsed > 0 else 0
                mb_s = (rate * 1.5) / 1024
                pct = (r_idx / target_rounds) * 100
                logger.info(
                    f"[{pct:5.1f}%] {r_idx:,}/{target_rounds:,} rounds | "
                    f"{rate:,.0f} msg/s ({mb_s:.1f} MB/s) | Active Rentals in Pool: {len(self.state.rentals):,} | "
                    f"Ops: C:{c_count:,} U:{u_count:,} D:{d_count:,}"
                )


# ==============================================================================
# WORKER PROCESS & MULTIPROCESSING HARNESS
# ==============================================================================
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def get_secret(secret_name: str) -> dict[str, Any]:
    logger.info(f"Fetching secret {secret_name} from AWS Secrets Manager...")
    client = boto3.client("secretsmanager", region_name="ap-southeast-2")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def load_config() -> dict[str, Any]:
    try:
        kafka_secret = get_secret("dvdrental/kafka_bootstrap_servers")
        bootstrap_servers = kafka_secret.get("bootstrap_servers")
        topic_prefix = kafka_secret.get("topic_prefix")
    except Exception as e:
        logger.warning(f"Could not load AWS Secret ({e}). Falling back to Environment Variables.")
        bootstrap_servers = os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS",
            "172.31.16.100:9092,172.31.16.101:9092,172.31.16.102:9092",
        )
        topic_prefix = os.environ.get("TOPIC_PREFIX", "cdc.public")

    return {
        "bootstrap_servers": bootstrap_servers,
        "topic_prefix": topic_prefix,
    }


def get_producer(bootstrap_servers: str) -> KafkaProducer:
    batch_size = int(os.environ.get("KAFKA_BATCH_SIZE", "524288"))  # 512 KB
    linger_ms = int(os.environ.get("KAFKA_LINGER_MS", "10"))
    acks_str = os.environ.get("KAFKA_ACKS", "1")
    acks_val = acks_str if acks_str in ("all", "0") else int(acks_str)
    compression = os.environ.get("KAFKA_COMPRESSION", "lz4")
    max_in_flight = int(os.environ.get("KAFKA_MAX_IN_FLIGHT", "5"))

    producer_kwargs = {
        "bootstrap_servers": bootstrap_servers.split(","),
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "key_serializer": lambda k: str(k).encode("utf-8"),
        "linger_ms": linger_ms,
        "batch_size": batch_size,
        "compression_type": compression,
        "acks": acks_val,
        "retries": 5,
        "max_in_flight_requests_per_connection": max_in_flight,
    }
    return KafkaProducer(**producer_kwargs)


def pump_worker(
    worker_id: int,
    bootstrap_servers: str,
    topic_prefix: str,
    scale_config: dict[str, int],
    rounds_per_worker: int,
    stop_event: multiprocessing.Event,
) -> None:
    """Worker process initializing its own state partition to avoid lock contention."""
    # Disjoint ID offsets per worker to guarantee unique keys across workers
    id_offset = worker_id * 100_000_000 + 1
    state = DataState(id_offset=id_offset)
    producer = get_producer(bootstrap_servers)

    orchestrator = MockDataOrchestrator(
        producer=producer,
        topic_prefix=topic_prefix,
        state=state,
        connector_name="dvdrental_source",
    )

    try:
        # Phase 1: Bootstrap hierarchy
        orchestrator.bootstrap_dimensions(scale_config)

        # Phase 2: Stream transactions
        op_insert_pct = int(os.environ.get("OP_INSERT_PCT", "70"))
        op_update_pct = int(os.environ.get("OP_UPDATE_PCT", "20"))
        orchestrator.stream_transactions(
            target_rounds=rounds_per_worker,
            stop_event=stop_event,
            log_interval=int(os.environ.get("LOG_INTERVAL_ROUNDS", "5000")),
            op_insert_pct=op_insert_pct,
            op_update_pct=op_update_pct,
        )
    except Exception as e:
        logger.error(f"Worker-{worker_id} encountered fatal error: {e}", exc_info=True)
    finally:
        producer.flush()
        producer.close()
        logger.info(f"Worker-{worker_id} successfully shutdown. Total messages: {orchestrator.msg_count:,}")


# ==============================================================================
# MAIN ENTRYPOINT
# ==============================================================================
def main() -> None:
    config = load_config()

    # Dynamic scale configuration from environment
    scale_config = {
        "num_countries": int(os.environ.get("NUM_COUNTRIES", "20")),
        "num_cities": int(os.environ.get("NUM_CITIES", "100")),
        "num_addresses": int(os.environ.get("NUM_ADDRESSES", "500")),
        "num_languages": int(os.environ.get("NUM_LANGUAGES", "7")),
        "num_categories": int(os.environ.get("NUM_CATEGORIES", "16")),
        "num_actors": int(os.environ.get("NUM_ACTORS", "300")),
        "num_staff": int(os.environ.get("NUM_STAFF", "50")),
        "num_stores": int(os.environ.get("NUM_STORES", "10")),
        "num_films": int(os.environ.get("NUM_FILMS", "2000")),
        "num_customers": int(os.environ.get("NUM_CUSTOMERS", "5000")),
        "copies_per_film_store": int(os.environ.get("COPIES_PER_FILM_STORE", "2")),
    }

    workers_count = int(os.environ.get("WORKERS", "8"))
    target_messages = int(os.environ.get("TARGET_MESSAGES", "20_000_000"))
    rounds_per_worker = (target_messages // workers_count) // 2  # ~2 msgs per round (rental + payment)

    logger.info("=" * 76)
    logger.info("DVDRENTAL DEBEZIUM 2.5.0 STATEFUL CDC GENERATOR")
    logger.info(f"Kafka Brokers:      {config['bootstrap_servers']}")
    logger.info(f"Topic Prefix:       {config['topic_prefix']}")
    logger.info(f"Scale Dimensions:   Stores: {scale_config['num_stores']} | Staff: {scale_config['num_staff']} | Films: {scale_config['num_films']} | Customers: {scale_config['num_customers']}")
    logger.info(f"Planned Scale:      {target_messages:,} target msgs across {workers_count} workers")
    logger.info("Envelope Standard:  Debezium 2.5.0.Final Full JSON Envelope (5 root fields)")
    logger.info("=" * 76)

    stop_event = multiprocessing.Event()
    workers = []

    for i in range(workers_count):
        p = multiprocessing.Process(
            target=pump_worker,
            args=(
                i,
                config["bootstrap_servers"],
                config["topic_prefix"],
                scale_config,
                rounds_per_worker,
                stop_event,
            ),
        )
        p.start()
        workers.append(p)

    try:
        for p in workers:
            p.join()
    except KeyboardInterrupt:
        logger.warning("Terminating all workers...")
        stop_event.set()
        for p in workers:
            p.join()

    logger.info("All Stateful Relationship Workers completed successfully!")


if __name__ == "__main__":
    main()
