"""
stress_test_v2.py — High-throughput stress test with full data integrity
=========================================================================
Core strategy:
  INSERT → Server-side SQL with generate_series() + random()
           Data is generated INSIDE PostgreSQL — no Python→network→DB overhead.
           Payment FK always references rentals just created in the SAME transaction.
  UPDATE → Batched SET on rental (return_date), customer (active), payment (amount)
  DELETE → FK-safe order: payment first → rental second (only orphaned returned ones)

Data integrity:
  ✓ All FK constraints preserved (never dropped)
  ✓ Rental unique constraint preserved (ON CONFLICT DO NOTHING)
  ✓ INSERT+payment in same transaction (no race condition on rental_id FK)
  ✓ DELETE uses FK-safe order (payment → rental)
  ✓ UPDATE/DELETE use LIMIT subqueries to avoid locking entire tables

Performance:
  ✓ generate_series → no Python data gen, no network transfer
  ✓ synchronous_commit=off per session
  ✓ work_mem=256MB per session
  ✓ Each iteration logs immediately — no more waiting 20 batches
"""

import os
import sys
import time
import random
import multiprocessing
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST        = os.environ.get("DB_HOST",        "localhost")
DB_PORT        = os.environ.get("DB_PORT",        "5432")
DB_NAME        = os.environ.get("DB_NAME",        "dvdrental")
DB_USER        = os.environ.get("DB_USER",        "postgres")
DB_PASS        = os.environ.get("DB_PASS",        "postgres")
TARGET_SIZE_GB = int(os.environ.get("TARGET_SIZE_GB", "100"))
NUM_WORKERS    = int(os.environ.get("NUM_WORKERS",    "4"))

# Rows per generate_series call — bigger = faster (fewer round-trips)
# 500K is a good sweet spot: ~50MB per batch, completes in seconds
BULK_BATCH     = int(os.environ.get("BULK_BATCH",     "500000"))

# Sub-batch size for UPDATE / DELETE (smaller to avoid long locks)
SUB_BATCH      = int(os.environ.get("SUB_BATCH",      "10000"))

# Operation mix — must sum to 100
OP_INSERT_PCT  = int(os.environ.get("OP_INSERT_PCT",  "60"))
OP_UPDATE_PCT  = int(os.environ.get("OP_UPDATE_PCT",  "25"))
OP_DELETE_PCT  = int(os.environ.get("OP_DELETE_PCT",  "15"))


# ─── Helpers ─────────────────────────────────────────────────────────────────

def get_connection():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, connect_timeout=30
    )
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit = off")
        cur.execute("SET work_mem = '256MB'")
    conn.commit()
    return conn


def get_connection_auto():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, connect_timeout=30
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit = off")
    return conn


def get_db_size_gb(cur) -> float:
    cur.execute(f"SELECT pg_database_size('{DB_NAME}') / (1024.0^3)")
    return cur.fetchone()[0]


def fetch_ids(cur, table, column):
    cur.execute(f"SELECT {column} FROM {table}")
    return [row[0] for row in cur.fetchall()]


def get_id_range(cur, table, column):
    """Return (min_id, max_id) for a column. Fast O(log n) with PK index."""
    cur.execute(f"SELECT MIN({column}), MAX({column}) FROM {table}")
    return cur.fetchone()


def table_count(cur, table):
    cur.execute(f"SELECT count(*) FROM {table}")
    return cur.fetchone()[0]


# ─── Phase 0: Upscale smallint → integer ─────────────────────────────────────

def upscale_smallint_columns():
    print("=" * 65)
    print("PHASE 0: Upgrade smallint → integer")
    print("=" * 65)
    conn = get_connection_auto()
    cur  = conn.cursor()

    # Drop views that depend on typed columns
    cur.execute("""
        SELECT table_name FROM information_schema.views
        WHERE table_schema = 'public'
    """)
    for (v,) in cur.fetchall():
        print(f"  DROP VIEW {v}")
        cur.execute(f'DROP VIEW IF EXISTS "{v}" CASCADE')

    cur.execute("""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND data_type = 'smallint'
          AND table_name NOT IN ('pg_stat_statements')
        ORDER BY table_name, column_name
    """)
    cols = cur.fetchall()
    if not cols:
        print("  No smallint columns. Skipping.")
    else:
        for tbl, col in cols:
            print(f"  ALTER {tbl}.{col}: smallint → integer")
            cur.execute(f'ALTER TABLE "{tbl}" ALTER COLUMN "{col}" TYPE integer')
        print(f"  Upgraded {len(cols)} columns.")

    print("Phase 0 complete!\n")
    cur.close()
    conn.close()


# ─── Phase 1: Seed dimensions ────────────────────────────────────────────────

def seed_dimensions():
    print("=" * 65)
    print("PHASE 1: Seeding dimension tables")
    print("=" * 65)

    conn = get_connection_auto()
    cur  = conn.cursor()

    # Skip if already seeded
    cust_count = table_count(cur, "customer")
    if cust_count > 100_000:
        print(f"  Already have {cust_count:,} customers. Skipping seed.")
        cur.close(); conn.close()
        return

    from faker import Faker
    fake = Faker()

    print("  100 countries …")
    psycopg2.extras.execute_values(cur,
        "INSERT INTO country (country) VALUES %s",
        [(fake.country()[:50],) for _ in range(100)])

    print("  1,000 cities …")
    country_ids = fetch_ids(cur, "country", "country_id")
    psycopg2.extras.execute_values(cur,
        "INSERT INTO city (city, country_id) VALUES %s",
        [(fake.city()[:50], random.choice(country_ids)) for _ in range(1000)])

    print("  5,000 addresses …")
    city_ids = fetch_ids(cur, "city", "city_id")
    psycopg2.extras.execute_values(cur,
        "INSERT INTO address (address, district, city_id, postal_code, phone) VALUES %s",
        [(fake.street_address()[:50], fake.state()[:20], random.choice(city_ids),
          fake.postcode()[:10], fake.phone_number()[:20]) for _ in range(5000)],
        page_size=2000)

    print("  5,000 actors …")
    psycopg2.extras.execute_values(cur,
        "INSERT INTO actor (first_name, last_name) VALUES %s",
        [(fake.first_name()[:45], fake.last_name()[:45]) for _ in range(5000)],
        page_size=2000)

    print("  5,000 films …")
    lang_ids = fetch_ids(cur, "language", "language_id")
    rows = []
    for _ in range(5000):
        rows.append((
            fake.catch_phrase()[:255],
            fake.text(max_nb_chars=200).replace("\n", " "),
            random.randint(1990, 2026),
            random.choice(lang_ids),
            random.randint(3, 7),
            round(random.uniform(0.99, 4.99), 2),
            random.randint(60, 185),
            round(random.uniform(9.99, 29.99), 2),
            random.choice(['G', 'PG', 'PG-13', 'R', 'NC-17'])
        ))
    psycopg2.extras.execute_values(cur,
        "INSERT INTO film (title, description, release_year, language_id, "
        "rental_duration, rental_rate, length, replacement_cost, rating) VALUES %s",
        rows, page_size=2000)

    print("  50,000 customers …")
    store_ids = fetch_ids(cur, "store", "store_id")
    addr_ids  = fetch_ids(cur, "address", "address_id")
    rows = []
    for _ in range(50000):
        rows.append((
            random.choice(store_ids),
            fake.first_name()[:45],
            fake.last_name()[:45],
            fake.email()[:50],
            random.choice(addr_ids),
            True
        ))
    psycopg2.extras.execute_values(cur,
        "INSERT INTO customer (store_id, first_name, last_name, email, address_id, activebool) VALUES %s",
        rows, page_size=5000)

    print("Phase 1 complete!\n")
    cur.close()
    conn.close()


# ─── Phase 2: Seed bridge tables ─────────────────────────────────────────────

def seed_bridge_tables():
    print("=" * 65)
    print("PHASE 2: Seeding film_actor + film_category")
    print("=" * 65)
    conn = get_connection_auto()
    cur  = conn.cursor()

    if table_count(cur, "film_actor") > 30_000:
        print("  Already seeded. Skipping.")
        cur.close(); conn.close()
        return

    actor_ids    = fetch_ids(cur, "actor",    "actor_id")
    film_ids     = fetch_ids(cur, "film",     "film_id")
    category_ids = fetch_ids(cur, "category", "category_id")

    fa = set()
    for fid in film_ids:
        for aid in random.sample(actor_ids, min(random.randint(3, 8), len(actor_ids))):
            fa.add((aid, fid))
    psycopg2.extras.execute_values(cur,
        "INSERT INTO film_actor (actor_id, film_id) VALUES %s ON CONFLICT DO NOTHING",
        list(fa), page_size=5000)
    print(f"  film_actor: {len(fa):,}")

    fc = set()
    for fid in film_ids:
        for cid in random.sample(category_ids, min(random.randint(1, 2), len(category_ids))):
            fc.add((fid, cid))
    psycopg2.extras.execute_values(cur,
        "INSERT INTO film_category (film_id, category_id) VALUES %s ON CONFLICT DO NOTHING",
        list(fc), page_size=5000)
    print(f"  film_category: {len(fc):,}")
    print("Phase 2 complete!\n")
    cur.close()
    conn.close()


# ─── Phase 3: Seed inventory ─────────────────────────────────────────────────

def seed_inventory():
    print("=" * 65)
    print("PHASE 3: Seeding inventory")
    print("=" * 65)
    conn = get_connection_auto()
    cur  = conn.cursor()

    if table_count(cur, "inventory") > 100_000:
        print("  Already seeded. Skipping.")
        cur.close(); conn.close()
        return

    film_ids  = fetch_ids(cur, "film",  "film_id")
    store_ids = fetch_ids(cur, "store", "store_id")
    rows = []
    for fid in film_ids:
        for sid in store_ids:
            for _ in range(random.randint(2, 5)):
                rows.append((fid, sid))
    psycopg2.extras.execute_values(cur,
        "INSERT INTO inventory (film_id, store_id) VALUES %s",
        rows, page_size=5000)
    print(f"  inventory: {len(rows):,}")
    print("Phase 3 complete!\n")
    cur.close()
    conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# Phase 4: Blast operations — SERVER-SIDE SQL
# ═════════════════════════════════════════════════════════════════════════════

def do_insert(conn, cur, bulk_batch, inv_min, inv_max, cust_min, cust_max,
              staff_min, staff_max):
    """
    INSERT rental + payment entirely in server-side SQL.
    Both inserts happen in the SAME transaction, so payment always
    references a rental_id that exists (just created above).
    """
    # 1. Capture current max rental_id
    cur.execute("SELECT COALESCE(MAX(rental_id), 0) FROM rental")
    prev_max = cur.fetchone()[0]

    # 2. Bulk-insert rentals using generate_series — all computation on Postgres
    cur.execute(f"""
        INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id)
        SELECT
            timestamp '2026-01-01' + random() * interval '365 days',
            floor(random() * ({inv_max} - {inv_min} + 1) + {inv_min})::int,
            floor(random() * ({cust_max} - {cust_min} + 1) + {cust_min})::int,
            floor(random() * ({staff_max} - {staff_min} + 1) + {staff_min})::int
        FROM generate_series(1, {bulk_batch})
        ON CONFLICT DO NOTHING
    """)
    rentals_ins = cur.rowcount

    # 3. Insert one payment per new rental — FK guaranteed by same transaction
    #    Payment uses the SAME customer/staff from the rental (realistic)
    cur.execute(f"""
        INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date)
        SELECT
            r.customer_id,
            r.staff_id,
            r.rental_id,
            round((random() * 9 + 0.99)::numeric, 2),
            r.rental_date + interval '1 hour' * floor(random() * 72)::int
        FROM rental r
        WHERE r.rental_id > {prev_max}
    """)
    payments_ins = cur.rowcount

    conn.commit()
    return rentals_ins, payments_ins


def do_update(conn, cur, sub_batch, cust_min, cust_max):
    """
    UPDATE operations:
      1. Return open rentals (set return_date)
      2. Toggle customer active status
      3. Adjust payment amounts
    All FK-safe — only modifies data columns, never FKs.
    """
    updated = 0

    # 1. Return open rentals ──────────────────────────────────────────────
    cur.execute(f"""
        UPDATE rental
        SET return_date = rental_date + interval '1 day' * floor(random() * 14 + 1)::int
        WHERE rental_id IN (
            SELECT rental_id FROM rental
            WHERE return_date IS NULL
            LIMIT {sub_batch}
        )
    """)
    updated += cur.rowcount

    # 2. Toggle customer activebool ───────────────────────────────────────
    toggle_count = min(sub_batch // 10, 500)
    cust_start   = random.randint(cust_min, max(cust_min, cust_max - toggle_count))
    cur.execute(f"""
        UPDATE customer
        SET activebool  = NOT activebool,
            last_update = NOW()
        WHERE customer_id BETWEEN {cust_start} AND {cust_start + toggle_count}
    """)
    updated += cur.rowcount

    # 3. Adjust payment amounts ───────────────────────────────────────────
    new_amount = round(random.uniform(0.99, 9.99), 2)
    cur.execute(f"""
        UPDATE payment
        SET amount = {new_amount}
        WHERE payment_id IN (
            SELECT payment_id FROM payment
            ORDER BY payment_date DESC
            LIMIT {sub_batch // 5}
        )
    """)
    updated += cur.rowcount

    conn.commit()
    return updated


def do_delete(conn, cur, sub_batch):
    """
    DELETE operations — FK-safe order:
      1. Delete payments for returned rentals
      2. Delete orphaned returned rentals (no remaining payments)
    """
    deleted = 0
    limit = max(1, sub_batch // 5)

    # 1. Delete payments for completed rentals ────────────────────────────
    cur.execute(f"""
        DELETE FROM payment
        WHERE payment_id IN (
            SELECT p.payment_id
            FROM payment p
            JOIN rental r ON r.rental_id = p.rental_id
            WHERE r.return_date IS NOT NULL
            LIMIT {limit}
        )
    """)
    deleted += cur.rowcount

    # 2. Delete returned rentals with no remaining payments ───────────────
    cur.execute(f"""
        DELETE FROM rental
        WHERE rental_id IN (
            SELECT r.rental_id
            FROM rental r
            LEFT JOIN payment p ON p.rental_id = r.rental_id
            WHERE r.return_date IS NOT NULL
              AND p.payment_id IS NULL
            LIMIT {limit}
        )
    """)
    deleted += cur.rowcount

    conn.commit()
    return deleted


# ─── Worker entry point ──────────────────────────────────────────────────────

def blast_transactions(worker_id, bulk_batch, sub_batch, target_gb,
                       insert_pct, update_pct, delete_pct):
    conn = get_connection()
    cur  = conn.cursor()

    # Get stable dimension ID ranges (these tables are NEVER modified)
    inv_min,   inv_max   = get_id_range(cur, "inventory", "inventory_id")
    cust_min,  cust_max  = get_id_range(cur, "customer",  "customer_id")
    staff_min, staff_max = get_id_range(cur, "staff",     "staff_id")
    conn.commit()

    print(f"Worker {worker_id}: Started | "
          f"inventory=[{inv_min}..{inv_max}]  "
          f"customer=[{cust_min}..{cust_max}]  "
          f"staff=[{staff_min}..{staff_max}]")

    # Build weighted op list
    op_pool = ["INSERT"] * insert_pct + ["UPDATE"] * update_pct + ["DELETE"] * delete_pct

    total_ins = 0
    total_upd = 0
    total_del = 0
    start_time = time.time()
    iteration  = 0

    while True:
        try:
            op = random.choice(op_pool)

            if op == "INSERT":
                r, p = do_insert(conn, cur, bulk_batch,
                                 inv_min, inv_max, cust_min, cust_max,
                                 staff_min, staff_max)
                total_ins += r + p

            elif op == "UPDATE":
                n = do_update(conn, cur, sub_batch, cust_min, cust_max)
                total_upd += n

            else:  # DELETE
                n = do_delete(conn, cur, sub_batch)
                total_del += n

            iteration += 1

            # ── Log every iteration ─────────────────────────────────────
            elapsed = time.time() - start_time
            total   = total_ins + total_upd + total_del
            rate    = total / elapsed if elapsed > 0 else 0

            # Check DB size every 5 iterations (pg_database_size is lightweight)
            if iteration % 5 == 0:
                current_gb = get_db_size_gb(cur)
                conn.commit()
                print(
                    f"Worker {worker_id} [{iteration}]: "
                    f"+{total_ins:,} INS  ~{total_upd:,} UPD  -{total_del:,} DEL  | "
                    f"DB: {current_gb:.2f}/{target_gb} GB  | "
                    f"{rate:,.0f} rows/sec  | "
                    f"op={op}",
                    flush=True
                )
                if current_gb >= target_gb:
                    print(f"Worker {worker_id}: ✓ Target {target_gb} GB reached!")
                    break
            else:
                print(
                    f"Worker {worker_id} [{iteration}]: "
                    f"{rate:,.0f} rows/sec  | op={op}  | "
                    f"+{total_ins:,} ~{total_upd:,} -{total_del:,}",
                    flush=True
                )

        except Exception as e:
            print(f"Worker {worker_id} ERR: {e}", flush=True)
            try:
                conn.rollback()
            except Exception:
                # Connection is dead, reconnect
                try: conn.close()
                except Exception: pass
                conn = get_connection()
                cur  = conn.cursor()
            time.sleep(0.5)

    cur.close()
    conn.close()
    elapsed = time.time() - start_time
    print(
        f"Worker {worker_id}: DONE in {elapsed:.0f}s | "
        f"INS={total_ins:,}  UPD={total_upd:,}  DEL={total_del:,}"
    )


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    assert OP_INSERT_PCT + OP_UPDATE_PCT + OP_DELETE_PCT == 100, \
        f"OP mix must sum to 100 (got {OP_INSERT_PCT}+{OP_UPDATE_PCT}+{OP_DELETE_PCT})"

    print("=" * 65)
    print(f"DVD Rental Stress Test v2  |  Target: {TARGET_SIZE_GB} GB")
    print(f"Workers: {NUM_WORKERS}  |  BULK_BATCH: {BULK_BATCH:,}  |  SUB_BATCH: {SUB_BATCH:,}")
    print(f"Mix: INSERT {OP_INSERT_PCT}% / UPDATE {OP_UPDATE_PCT}% / DELETE {OP_DELETE_PCT}%")
    print("=" * 65)

    conn = get_connection_auto()
    cur  = conn.cursor()
    initial_size = get_db_size_gb(cur)
    print(f"Current DB size: {initial_size:.2f} GB\n")
    cur.close()
    conn.close()

    upscale_smallint_columns()
    seed_dimensions()
    seed_bridge_tables()
    seed_inventory()

    print("\n" + "=" * 65)
    print(f"PHASE 4: Blasting with {NUM_WORKERS} workers")
    print("Strategy: Server-side SQL (generate_series) — no Python data transfer")
    print("=" * 65)

    processes = []
    for i in range(NUM_WORKERS):
        p = multiprocessing.Process(
            target=blast_transactions,
            args=(i, BULK_BATCH, SUB_BATCH, TARGET_SIZE_GB,
                  OP_INSERT_PCT, OP_UPDATE_PCT, OP_DELETE_PCT)
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    conn = get_connection_auto()
    cur  = conn.cursor()
    final_size = get_db_size_gb(cur)
    print(f"\n{'=' * 65}")
    print(f"DONE!  {initial_size:.2f} GB  →  {final_size:.2f} GB")
    print(f"{'=' * 65}")
    cur.close()
    conn.close()
