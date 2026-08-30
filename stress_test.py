import sys
import psycopg2
import psycopg2.extras
import time
import random
import os
import multiprocessing
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "dvdrental")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "postgres")
TARGET_SIZE_GB = int(os.environ.get("TARGET_SIZE_GB", "100"))
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "4"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "2000"))
OP_INSERT_PCT = int(os.environ.get("OP_INSERT_PCT", "60"))
OP_UPDATE_PCT = int(os.environ.get("OP_UPDATE_PCT", "25"))
OP_DELETE_PCT = int(os.environ.get("OP_DELETE_PCT", "15"))
SLEEP_BETWEEN_BATCHES = float(os.environ.get("SLEEP_BETWEEN_BATCHES", "0.2"))


def get_connection():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    conn.autocommit = True
    return conn


def fetch_ids(cur, table, column, limit=100000):
    query = f"SELECT {column} FROM {table}"
    if limit:
        query += f" LIMIT {limit}"
    cur.execute(query)
    return [row[0] for row in cur.fetchall()]


def get_db_size_gb(cur):
    cur.execute(f"SELECT pg_database_size('{DB_NAME}') / (1024.0 * 1024 * 1024)")
    return cur.fetchone()[0]


def upscale_smallint_columns():
    print("=" * 60)
    print("PHASE 0: Upgrading smallint columns to integer (prevent overflow)")
    print("=" * 60)
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT table_name FROM information_schema.views
        WHERE table_schema = 'public'
    """)
    views = [row[0] for row in cur.fetchall()]
    if views:
        print(f"  Dropping {len(views)} views: {', '.join(views)}")
        for view in views:
            cur.execute(f'DROP VIEW IF EXISTS "{view}" CASCADE')

    cur.execute("""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND data_type = 'smallint'
          AND table_name NOT IN ('pg_stat_statements')
        ORDER BY table_name, column_name
    """)
    columns_to_alter = cur.fetchall()
    if not columns_to_alter:
        print("  No smallint columns found. Skipping.")
    else:
        for table, column in columns_to_alter:
            print(f"  ALTER {table}.{column}: smallint -> integer")
            cur.execute(f'ALTER TABLE "{table}" ALTER COLUMN "{column}" TYPE integer')
        print(f"  Upgraded {len(columns_to_alter)} columns.")
    print("Phase 0 complete!")


def seed_dimensions():
    print("=" * 60)
    print("PHASE 1: Seeding dimension tables")
    print("=" * 60)
    fake = Faker()
    conn = get_connection()
    cur = conn.cursor()

    print("Generating 100 countries...")
    country_data = [(fake.country()[:50],) for _ in range(100)]
    psycopg2.extras.execute_values(
        cur, "INSERT INTO country (country) VALUES %s", country_data
    )

    print("Generating 1,000 cities...")
    country_ids = fetch_ids(cur, "country", "country_id")
    city_data = [(fake.city()[:50], random.choice(country_ids)) for _ in range(1000)]
    psycopg2.extras.execute_values(
        cur, "INSERT INTO city (city, country_id) VALUES %s", city_data
    )

    print("Generating 5,000 addresses...")
    city_ids = fetch_ids(cur, "city", "city_id")
    address_data = []
    for _ in range(5000):
        address_data.append(
            (
                fake.street_address()[:50],
                fake.state()[:20],
                random.choice(city_ids),
                fake.postcode()[:10],
                fake.phone_number()[:20],
            )
        )
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO address (address, district, city_id, postal_code, phone) VALUES %s",
        address_data,
        page_size=2000,
    )

    print("Generating 5,000 actors...")
    actor_data = [(fake.first_name()[:45], fake.last_name()[:45]) for _ in range(5000)]
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO actor (first_name, last_name) VALUES %s",
        actor_data,
        page_size=2000,
    )

    print("Generating 5,000 films...")
    language_ids = fetch_ids(cur, "language", "language_id")
    film_data = []
    for _ in range(5000):
        film_data.append(
            (
                fake.catch_phrase()[:255],
                fake.text(max_nb_chars=200),
                random.randint(1990, 2026),
                random.choice(language_ids),
                random.randint(3, 7),
                round(random.uniform(0.99, 4.99), 2),
                random.randint(60, 185),
                round(random.uniform(9.99, 29.99), 2),
                random.choice(["G", "PG", "PG-13", "R", "NC-17"]),
            )
        )
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO film (title, description, release_year, language_id, rental_duration, rental_rate, length, replacement_cost, rating) VALUES %s",
        film_data,
        page_size=2000,
    )

    print("Generating 50,000 customers...")
    store_ids = fetch_ids(cur, "store", "store_id")
    address_ids = fetch_ids(cur, "address", "address_id")
    customer_data = []
    for _ in range(50000):
        customer_data.append(
            (
                random.choice(store_ids),
                fake.first_name()[:45],
                fake.last_name()[:45],
                fake.email()[:50],
                random.choice(address_ids),
                True,
            )
        )
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO customer (store_id, first_name, last_name, email, address_id, activebool) VALUES %s",
        customer_data,
        page_size=5000,
    )

    print("Phase 1 complete!")
    cur.close()
    conn.close()


def seed_bridge_tables():
    print("=" * 60)
    print("PHASE 2: Seeding bridge tables (film_actor, film_category)")
    print("=" * 60)
    conn = get_connection()
    cur = conn.cursor()

    actor_ids = fetch_ids(cur, "actor", "actor_id")
    film_ids = fetch_ids(cur, "film", "film_id")
    category_ids = fetch_ids(cur, "category", "category_id")

    print("Generating film_actor relationships (3-8 actors per film)...")
    film_actor_data = set()
    for film_id in film_ids:
        selected_actors = random.sample(
            actor_ids, min(random.randint(3, 8), len(actor_ids))
        )
        for actor_id in selected_actors:
            film_actor_data.add((actor_id, film_id))
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO film_actor (actor_id, film_id) VALUES %s ON CONFLICT DO NOTHING",
        list(film_actor_data),
        page_size=5000,
    )
    print(f"  -> Created {len(film_actor_data)} film_actor records")

    print("Generating film_category relationships (1-2 categories per film)...")
    film_category_data = set()
    for film_id in film_ids:
        selected_cats = random.sample(
            category_ids, min(random.randint(1, 2), len(category_ids))
        )
        for cat_id in selected_cats:
            film_category_data.add((film_id, cat_id))
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO film_category (film_id, category_id) VALUES %s ON CONFLICT DO NOTHING",
        list(film_category_data),
        page_size=5000,
    )
    print(f"  -> Created {len(film_category_data)} film_category records")

    print("Phase 2 complete!")
    cur.close()
    conn.close()


def seed_inventory():
    print("=" * 60)
    print("PHASE 3: Seeding inventory (2-5 copies per film per store)")
    print("=" * 60)
    conn = get_connection()
    cur = conn.cursor()

    film_ids = fetch_ids(cur, "film", "film_id")
    store_ids = fetch_ids(cur, "store", "store_id")

    inventory_data = []
    for film_id in film_ids:
        for store_id in store_ids:
            for _ in range(random.randint(2, 5)):
                inventory_data.append((film_id, store_id))
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO inventory (film_id, store_id) VALUES %s",
        inventory_data,
        page_size=5000,
    )

    print(f"  -> Created {len(inventory_data)} inventory items")
    print("Phase 3 complete!")
    cur.close()
    conn.close()


def random_date():
    # Fast random date in 2026
    t = random.randint(1767225600, 1798761600)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))


def blast_transactions(worker_id, batch_size, target_gb):
    conn = get_connection()
    cur = conn.cursor()

    customer_ids = fetch_ids(cur, "customer", "customer_id")
    staff_ids = fetch_ids(cur, "staff", "staff_id")
    inventory_ids = fetch_ids(cur, "inventory", "inventory_id")
    rental_ids = fetch_ids(cur, "rental", "rental_id")

    print(
        f"Worker {worker_id}: Started | FK pools -> customers={len(customer_ids)}, inventory={len(inventory_ids)}, rentals={len(rental_ids)}"
    )

    total_inserted = 0
    start_time = time.time()

    while True:
        try:
            if total_inserted % (batch_size * 20) == 0 and total_inserted > 0:
                current_gb = get_db_size_gb(cur)
                elapsed = time.time() - start_time
                rate = total_inserted / elapsed if elapsed > 0 else 0
                print(
                    f"Worker {worker_id}: {total_inserted:,} rows | DB: {current_gb:.2f} GB / {target_gb} GB | {rate:.0f} rows/sec"
                )
                if current_gb >= target_gb:
                    print(f"Worker {worker_id}: Target reached! Stopping.")
                    break

            op_roll = random.randint(1, 100)

            if op_roll <= OP_INSERT_PCT:
                rental_data = []
                for _ in range(batch_size):
                    rental_data.append(
                        (
                            random_date(),
                            random.choice(inventory_ids),
                            random.choice(customer_ids),
                            random.choice(staff_ids),
                        )
                    )
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id) VALUES %s ON CONFLICT DO NOTHING",
                    rental_data,
                    page_size=5000,
                )

                payment_data = []
                for _ in range(batch_size):
                    payment_data.append(
                        (
                            random.choice(customer_ids),
                            random.choice(staff_ids),
                            random.choice(rental_ids),
                            round(random.uniform(0.99, 9.99), 2),
                            random_date(),
                        )
                    )
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date) VALUES %s ON CONFLICT DO NOTHING",
                    payment_data,
                    page_size=5000,
                )
                total_inserted += batch_size * 2

            elif op_roll <= OP_INSERT_PCT + OP_UPDATE_PCT:
                update_rentals = random.sample(
                    rental_ids, min(50, batch_size, len(rental_ids))
                )
                cur.execute(
                    "UPDATE payment SET amount = ROUND((RANDOM() * 10)::numeric, 2) WHERE rental_id = ANY(%s)",
                    (update_rentals,),
                )
                total_inserted += cur.rowcount

            else:
                delete_rentals = random.sample(
                    rental_ids, min(50, batch_size, len(rental_ids))
                )
                cur.execute(
                    "DELETE FROM payment WHERE rental_id = ANY(%s)", (delete_rentals,)
                )
                total_inserted += cur.rowcount

            if SLEEP_BETWEEN_BATCHES > 0:
                time.sleep(SLEEP_BETWEEN_BATCHES)

        except Exception as e:
            print(f"Worker {worker_id} Warning: Batch failed (retrying): {e}")
            try:
                conn.rollback()
            except Exception:
                print(
                    f"Worker {worker_id} Warning: Rollback failed, connection might be dead. Reconnecting..."
                )
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(5)
                conn = get_connection()
                cur = conn.cursor()
            time.sleep(1)

    cur.close()
    conn.close()
    print(f"Worker {worker_id}: Done! Total: {total_inserted:,} rows")


if __name__ == "__main__":
    print("=" * 60)
    print(f"DVD Rental Stress Test | Target: {TARGET_SIZE_GB} GB")
    print(f"Workers: {NUM_WORKERS} | Batch Size: {BATCH_SIZE}")
    print("=" * 60)

    conn = get_connection()
    cur = conn.cursor()
    initial_size = get_db_size_gb(cur)
    print(f"Current DB size: {initial_size:.2f} GB")

    print("Checking/creating index on payment(rental_id)...")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_payment_rental_id ON payment(rental_id)")

    cur.execute("SELECT COUNT(*) FROM customer")
    customer_count = cur.fetchone()[0]
    cur.close()
    conn.close()

    if customer_count < 1000:
        upscale_smallint_columns()
        seed_dimensions()
        seed_bridge_tables()
        seed_inventory()
    else:
        print("Database already seeded. Skipping Phase 1-3 (Seeding) to prevent duplicate errors and speed up restart.")

    print("\n" + "=" * 60)
    print(f"PHASE 4: Blasting rental & payment with {NUM_WORKERS} workers")
    print("=" * 60)

    processes = []
    for i in range(NUM_WORKERS):
        p = multiprocessing.Process(
            target=blast_transactions, args=(i, BATCH_SIZE, TARGET_SIZE_GB)
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    any_failed = False
    for p in processes:
        if p.exitcode != 0:
            print(f"Worker {p.pid} crashed with exit code {p.exitcode}")
            any_failed = True

    if any_failed:
        print("One or more workers crashed (OOM?). Exiting main process with error to trigger bash restart.")
        sys.exit(1)

    conn = get_connection()
    cur = conn.cursor()
    final_size = get_db_size_gb(cur)
    print(f"\n{'=' * 60}")
    print(f"DONE! DB size: {initial_size:.2f} GB -> {final_size:.2f} GB")
    print(f"{'=' * 60}")
    cur.close()
    conn.close()
