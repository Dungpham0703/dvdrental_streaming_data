"""
stress_test_v2.py — High-throughput stress test with data integrity
====================================================================
Operations per worker (configurable ratio):
  INSERT  60% — new rentals + payments via COPY
  UPDATE  25% — return rentals, update customer status/email
  DELETE  15% — purge old returned rentals & orphaned payments

Data integrity guarantees:
  ✓ All FK constraints kept (customer, staff, inventory, rental)
  ✓ Rental unique constraint (inventory_id, rental_date) kept — ON CONFLICT DO NOTHING
  ✓ Every DELETE cascades through FK-safe order (payment → rental)
  ✓ All mutations wrapped in explicit transactions (not autocommit)

Performance improvements over v1:
  ✓ COPY FROM STDIN for INSERTs         (5–10× faster than execute_values)
  ✓ numpy random sampling               (10–50× faster on large FK pools)
  ✓ synchronous_commit=off per session  (async WAL flush, safe for bulk load)
  ✓ BATCH_SIZE 50 000 default           (fewer network round-trips)
  ✓ Periodic FK-pool refresh            (UPDATE/DELETE always target real rows)
"""

import io
import os
import time
import random
import multiprocessing
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False
    print("[WARN] numpy not installed — install it for maximum speed: pip install numpy")

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST        = os.environ.get("DB_HOST",        "localhost")
DB_PORT        = os.environ.get("DB_PORT",        "5432")
DB_NAME        = os.environ.get("DB_NAME",        "dvdrental")
DB_USER        = os.environ.get("DB_USER",        "postgres")
DB_PASS        = os.environ.get("DB_PASS",        "postgres")
TARGET_SIZE_GB = int(os.environ.get("TARGET_SIZE_GB", "100"))
NUM_WORKERS    = int(os.environ.get("NUM_WORKERS",    "4"))
BATCH_SIZE     = int(os.environ.get("BATCH_SIZE",    "50000"))

# Operation mix weights — must sum to 100
OP_INSERT_PCT = int(os.environ.get("OP_INSERT_PCT", "60"))
OP_UPDATE_PCT = int(os.environ.get("OP_UPDATE_PCT", "25"))
OP_DELETE_PCT = int(os.environ.get("OP_DELETE_PCT", "15"))

# Date range for generated timestamps: 2026-01-01 → 2026-12-31 (UTC unix)
DATE_START = 1767225600
DATE_END   = 1798761600


# ─── Helpers ─────────────────────────────────────────────────────────────────

def get_connection(autocommit: bool = False):
    """Open a connection. autocommit=False for transactional workers."""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        connect_timeout=10
    )
    conn.autocommit = autocommit
    with conn.cursor() as cur:
        # Async WAL flush — huge write speedup; safe because we handle our own commits
        cur.execute("SET synchronous_commit = off")
    return conn


def fetch_ids(cur, table: str, column: str):
    """Fetch a column of IDs and return as numpy array (or list)."""
    cur.execute(f"SELECT {column} FROM {table}")
    data = [row[0] for row in cur.fetchall()]
    return np.array(data, dtype=np.int64) if HAS_NUMPY else data


def get_db_size_gb(cur) -> float:
    cur.execute(f"SELECT pg_database_size('{DB_NAME}') / (1024.0 * 1024 * 1024)")
    return cur.fetchone()[0]


def rand_choice(arr, size: int):
    """Vectorised random sampling."""
    if HAS_NUMPY:
        return arr[np.random.randint(0, len(arr), size=size)]
    return [random.choice(arr) for _ in range(size)]


def rand_scalar(arr):
    """Pick one random element."""
    if HAS_NUMPY:
        return int(arr[np.random.randint(0, len(arr))])
    return random.choice(arr)


def rand_dates(size: int) -> list[str]:
    """Generate random timestamp strings efficiently."""
    if HAS_NUMPY:
        ts = np.random.randint(DATE_START, DATE_END, size=size)
        return [time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(t))) for t in ts]
    return [
        time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(random.randint(DATE_START, DATE_END)))
        for _ in range(size)
    ]


def copy_insert(conn, table: str, columns: list[str], rows: list[str]):
    """Bulk-insert rows via COPY FROM STDIN (CSV). `rows` is list of CSV line strings."""
    buf = io.StringIO("\n".join(rows))
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv)",
            buf
        )


# ─── Phase 0 ─────────────────────────────────────────────────────────────────

def upscale_smallint_columns():
    print("=" * 60)
    print("PHASE 0: Upgrade smallint → integer (overflow prevention)")
    print("=" * 60)
    conn = get_connection(autocommit=True)
    cur  = conn.cursor()

    # Drop views that depend on typed columns
    cur.execute("""
        SELECT table_name FROM information_schema.views
        WHERE table_schema = 'public'
    """)
    views = [r[0] for r in cur.fetchall()]
    for v in views:
        print(f"  DROP VIEW {v}")
        cur.execute(f'DROP VIEW IF EXISTS "{v}" CASCADE')

    # Upscale smallint columns
    cur.execute("""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND data_type    = 'smallint'
          AND table_name NOT IN ('pg_stat_statements')
        ORDER BY table_name, column_name
    """)
    cols = cur.fetchall()
    for tbl, col in cols:
        print(f"  ALTER {tbl}.{col}: smallint → integer")
        cur.execute(f'ALTER TABLE "{tbl}" ALTER COLUMN "{col}" TYPE integer')
    if not cols:
        print("  No smallint columns found.")

    # ── Important: we keep the rental unique constraint intact ────────────────
    # INSERT worker will use ON CONFLICT DO NOTHING so integrity is never broken.
    print("  Rental unique constraint: KEPT (data integrity)")
    print("Phase 0 complete!\n")
    cur.close()
    conn.close()


# ─── Phase 1 ─────────────────────────────────────────────────────────────────

def seed_dimensions():
    from faker import Faker
    print("=" * 60)
    print("PHASE 1: Seeding dimension tables")
    print("=" * 60)
    fake = Faker()
    conn = get_connection(autocommit=True)
    cur  = conn.cursor()

    def _ids(table, col):
        return fetch_ids(cur, table, col)

    def _lst(arr):
        return arr.tolist() if HAS_NUMPY else arr

    print("  100 countries …")
    psycopg2.extras.execute_values(cur,
        "INSERT INTO country (country) VALUES %s",
        [(fake.country()[:50],) for _ in range(100)])

    print("  1 000 cities …")
    cids = _lst(_ids("country", "country_id"))
    psycopg2.extras.execute_values(cur,
        "INSERT INTO city (city, country_id) VALUES %s",
        [(fake.city()[:50], random.choice(cids)) for _ in range(1000)])

    print("  5 000 addresses …")
    city_ids = _lst(_ids("city", "city_id"))
    psycopg2.extras.execute_values(cur,
        "INSERT INTO address (address, district, city_id, postal_code, phone) VALUES %s",
        [(fake.street_address()[:50], fake.state()[:20], random.choice(city_ids),
          fake.postcode()[:10], fake.phone_number()[:20]) for _ in range(5000)],
        page_size=2000)

    print("  5 000 actors …")
    psycopg2.extras.execute_values(cur,
        "INSERT INTO actor (first_name, last_name) VALUES %s",
        [(fake.first_name()[:45], fake.last_name()[:45]) for _ in range(5000)],
        page_size=2000)

    print("  5 000 films …")
    lang_ids = _lst(_ids("language", "language_id"))
    film_rows = []
    for _ in range(5000):
        film_rows.append((
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
        film_rows, page_size=2000)

    print("  50 000 customers …")
    store_ids   = _lst(_ids("store",   "store_id"))
    addr_ids    = _lst(_ids("address", "address_id"))
    cust_rows   = []
    for _ in range(50000):
        cust_rows.append((
            random.choice(store_ids),
            fake.first_name()[:45],
            fake.last_name()[:45],
            fake.email()[:50],
            random.choice(addr_ids),
            True
        ))
    psycopg2.extras.execute_values(cur,
        "INSERT INTO customer (store_id, first_name, last_name, email, address_id, activebool) VALUES %s",
        cust_rows, page_size=5000)

    print("Phase 1 complete!\n")
    cur.close()
    conn.close()


# ─── Phase 2 ─────────────────────────────────────────────────────────────────

def seed_bridge_tables():
    print("=" * 60)
    print("PHASE 2: Seeding film_actor + film_category")
    print("=" * 60)
    conn = get_connection(autocommit=True)
    cur  = conn.cursor()

    actor_ids    = fetch_ids(cur, "actor",    "actor_id")
    film_ids     = fetch_ids(cur, "film",     "film_id")
    category_ids = fetch_ids(cur, "category", "category_id")
    a_lst = actor_ids.tolist()    if HAS_NUMPY else actor_ids
    f_lst = film_ids.tolist()     if HAS_NUMPY else film_ids
    c_lst = category_ids.tolist() if HAS_NUMPY else category_ids

    fa = set()
    for fid in f_lst:
        for aid in random.sample(a_lst, min(random.randint(3, 8), len(a_lst))):
            fa.add((aid, fid))
    psycopg2.extras.execute_values(cur,
        "INSERT INTO film_actor (actor_id, film_id) VALUES %s ON CONFLICT DO NOTHING",
        list(fa), page_size=5000)
    print(f"  film_actor: {len(fa):,} rows")

    fc = set()
    for fid in f_lst:
        for cid in random.sample(c_lst, min(random.randint(1, 2), len(c_lst))):
            fc.add((fid, cid))
    psycopg2.extras.execute_values(cur,
        "INSERT INTO film_category (film_id, category_id) VALUES %s ON CONFLICT DO NOTHING",
        list(fc), page_size=5000)
    print(f"  film_category: {len(fc):,} rows")
    print("Phase 2 complete!\n")
    cur.close()
    conn.close()


# ─── Phase 3 ─────────────────────────────────────────────────────────────────

def seed_inventory():
    print("=" * 60)
    print("PHASE 3: Seeding inventory")
    print("=" * 60)
    conn = get_connection(autocommit=True)
    cur  = conn.cursor()

    film_ids  = fetch_ids(cur, "film",  "film_id")
    store_ids = fetch_ids(cur, "store", "store_id")
    f_lst = film_ids.tolist()  if HAS_NUMPY else film_ids
    s_lst = store_ids.tolist() if HAS_NUMPY else store_ids

    rows = []
    for fid in f_lst:
        for sid in s_lst:
            for _ in range(random.randint(2, 5)):
                rows.append((fid, sid))
    psycopg2.extras.execute_values(cur,
        "INSERT INTO inventory (film_id, store_id) VALUES %s",
        rows, page_size=5000)
    print(f"  inventory: {len(rows):,} rows")
    print("Phase 3 complete!\n")
    cur.close()
    conn.close()


# ─── Phase 4: INSERT worker ───────────────────────────────────────────────────

def _do_insert(conn, cur, batch_size, customer_ids, staff_ids, inventory_ids, rental_ids):
    """
    INSERT batch_size rentals + batch_size payments.
    Uses COPY for max throughput.
    Returns number of rows attempted.
    """
    # ── Rental ──────────────────────────────────────────────────────────────
    r_inv  = rand_choice(inventory_ids, batch_size)
    r_cust = rand_choice(customer_ids,  batch_size)
    r_stf  = rand_choice(staff_ids,     batch_size)
    r_dts  = rand_dates(batch_size)

    rental_lines = [
        f"{r_dts[i]},{int(r_inv[i])},{int(r_cust[i])},{int(r_stf[i])}"
        for i in range(batch_size)
    ]
    # COPY then handle conflicts via try/except — revert to execute_values if COPY conflicts
    try:
        copy_insert(conn, "rental",
                    ["rental_date", "inventory_id", "customer_id", "staff_id"],
                    rental_lines)
    except psycopg2.errors.UniqueViolation:
        # Rare collision — fall back to execute_values with ON CONFLICT DO NOTHING
        conn.rollback()
        rental_data = [
            (r_dts[i], int(r_inv[i]), int(r_cust[i]), int(r_stf[i]))
            for i in range(batch_size)
        ]
        psycopg2.extras.execute_values(cur,
            "INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id) "
            "VALUES %s ON CONFLICT DO NOTHING",
            rental_data, page_size=5000)

    # ── Payment (linked to existing rental_ids pool) ──────────────────────
    p_cust = rand_choice(customer_ids, batch_size)
    p_stf  = rand_choice(staff_ids,   batch_size)
    p_rent = rand_choice(rental_ids,  batch_size)
    if HAS_NUMPY:
        p_amt = np.round(np.random.uniform(0.99, 9.99, batch_size), 2)
    else:
        p_amt = [round(random.uniform(0.99, 9.99), 2) for _ in range(batch_size)]
    p_dts  = rand_dates(batch_size)

    payment_lines = [
        f"{int(p_cust[i])},{int(p_stf[i])},{int(p_rent[i])},{float(p_amt[i])},{p_dts[i]}"
        for i in range(batch_size)
    ]
    try:
        copy_insert(conn, "payment",
                    ["customer_id", "staff_id", "rental_id", "amount", "payment_date"],
                    payment_lines)
    except Exception:
        conn.rollback()
        payment_data = [
            (int(p_cust[i]), int(p_stf[i]), int(p_rent[i]), float(p_amt[i]), p_dts[i])
            for i in range(batch_size)
        ]
        psycopg2.extras.execute_values(cur,
            "INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date) "
            "VALUES %s ON CONFLICT DO NOTHING",
            payment_data, page_size=5000)

    conn.commit()
    return batch_size * 2


def _do_update(conn, cur, batch_size, customer_ids, staff_ids):
    """
    UPDATE operations (25% of batches):
      • Set return_date on open rentals  (simulates customer returning a film)
      • Update customer email / active status
      • Adjust payment amounts for recent payments
    """
    updated = 0
    sub     = max(1, batch_size // 10)   # smaller sub-batch for updates

    # 1. Return open rentals ──────────────────────────────────────────────────
    #    Pick `sub` rentals with no return_date and set a return_date
    return_date = time.strftime('%Y-%m-%d %H:%M:%S',
                                time.gmtime(random.randint(DATE_START, DATE_END)))
    cur.execute("""
        UPDATE rental
        SET    return_date = %s
        WHERE  rental_id IN (
            SELECT rental_id FROM rental
            WHERE  return_date IS NULL
            ORDER  BY RANDOM()
            LIMIT  %s
        )
    """, (return_date, sub))
    updated += cur.rowcount

    # 2. Toggle customer active status ────────────────────────────────────────
    cust_sample = rand_choice(customer_ids, min(sub, 500))
    cur.execute("""
        UPDATE customer
        SET    activebool = NOT activebool,
               last_update = NOW()
        WHERE  customer_id = ANY(%s)
    """, (cust_sample.tolist() if HAS_NUMPY else list(cust_sample),))
    updated += cur.rowcount

    # 3. Adjust recent payment amounts ────────────────────────────────────────
    new_amount = round(random.uniform(0.99, 9.99), 2)
    cur.execute("""
        UPDATE payment
        SET    amount = %s
        WHERE  payment_id IN (
            SELECT payment_id FROM payment
            ORDER  BY payment_date DESC
            LIMIT  %s
        )
    """, (new_amount, sub))
    updated += cur.rowcount

    conn.commit()
    return updated


def _do_delete(conn, cur, batch_size):
    """
    DELETE operations (15% of batches) — FK-safe order:
      1. Delete payments whose rental has a return_date (completed rentals)
      2. Delete those completed rentals
    Never deletes rentals that still have open payments.
    """
    deleted = 0
    limit   = max(1, batch_size // 20)   # conservative delete size

    # 1. Delete payments linked to completed rentals ───────────────────────────
    cur.execute("""
        DELETE FROM payment
        WHERE payment_id IN (
            SELECT p.payment_id
            FROM   payment p
            JOIN   rental  r ON r.rental_id = p.rental_id
            WHERE  r.return_date IS NOT NULL
            ORDER  BY r.return_date ASC
            LIMIT  %s
        )
    """, (limit,))
    deleted += cur.rowcount

    # 2. Delete the completed rentals (no FK violation: payments already gone)
    cur.execute("""
        DELETE FROM rental
        WHERE rental_id IN (
            SELECT rental_id FROM rental
            WHERE  return_date IS NOT NULL
              AND  rental_id NOT IN (SELECT rental_id FROM payment)
            ORDER  BY return_date ASC
            LIMIT  %s
        )
    """, (limit,))
    deleted += cur.rowcount

    conn.commit()
    return deleted


# ─── Phase 4: Worker entry point ─────────────────────────────────────────────

def blast_transactions(worker_id: int, batch_size: int, target_gb: float,
                       insert_pct: int, update_pct: int, delete_pct: int):
    conn = get_connection(autocommit=False)   # transactional
    cur  = conn.cursor()

    # Load FK pools
    customer_ids  = fetch_ids(cur, "customer",  "customer_id")
    staff_ids     = fetch_ids(cur, "staff",     "staff_id")
    inventory_ids = fetch_ids(cur, "inventory", "inventory_id")
    rental_ids    = fetch_ids(cur, "rental",    "rental_id")

    print(f"Worker {worker_id}: Started | "
          f"customers={len(customer_ids):,}  inventory={len(inventory_ids):,}  "
          f"rentals={len(rental_ids):,}")

    # Build weighted operation list
    op_pool = (
        ["INSERT"] * insert_pct +
        ["UPDATE"] * update_pct +
        ["DELETE"] * delete_pct
    )

    total_inserted = 0
    total_updated  = 0
    total_deleted  = 0
    start_time     = time.time()
    iter_count     = 0
    LOG_EVERY      = 20   # log every 20 iterations

    while True:
        try:
            # ── Progress check ──────────────────────────────────────────────
            if iter_count > 0 and iter_count % LOG_EVERY == 0:
                current_gb = get_db_size_gb(cur)
                elapsed    = time.time() - start_time
                total_rows = total_inserted + total_updated + total_deleted
                rate       = total_rows / elapsed if elapsed > 0 else 0
                print(
                    f"Worker {worker_id}: "
                    f"+{total_inserted:,} INS  "
                    f"~{total_updated:,} UPD  "
                    f"-{total_deleted:,} DEL  | "
                    f"DB: {current_gb:.2f}/{target_gb} GB | "
                    f"{rate:,.0f} rows/sec"
                )
                if current_gb >= target_gb:
                    print(f"Worker {worker_id}: Target reached! Stopping.")
                    break

                # Refresh rental_ids pool so UPDATE/DELETE operate on real rows
                rental_ids = fetch_ids(cur, "rental", "rental_id")

            # ── Pick operation ──────────────────────────────────────────────
            op = random.choice(op_pool)

            if op == "INSERT":
                n = _do_insert(conn, cur, batch_size,
                               customer_ids, staff_ids, inventory_ids, rental_ids)
                total_inserted += n

            elif op == "UPDATE":
                n = _do_update(conn, cur, batch_size, customer_ids, staff_ids)
                total_updated += n

            else:  # DELETE
                n = _do_delete(conn, cur, batch_size)
                total_deleted += n

            iter_count += 1

        except Exception as e:
            print(f"Worker {worker_id} Warning: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            time.sleep(1)

    cur.close()
    conn.close()
    print(
        f"Worker {worker_id}: Done! "
        f"INS={total_inserted:,}  UPD={total_updated:,}  DEL={total_deleted:,}"
    )


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    assert OP_INSERT_PCT + OP_UPDATE_PCT + OP_DELETE_PCT == 100, \
        "OP_INSERT_PCT + OP_UPDATE_PCT + OP_DELETE_PCT must equal 100"

    print("=" * 65)
    print(f"DVD Rental Stress Test v2  |  Target: {TARGET_SIZE_GB} GB")
    print(f"Workers: {NUM_WORKERS}  |  Batch: {BATCH_SIZE:,}")
    print(f"Mix — INSERT:{OP_INSERT_PCT}%  UPDATE:{OP_UPDATE_PCT}%  DELETE:{OP_DELETE_PCT}%")
    print(f"numpy: {'✓ enabled' if HAS_NUMPY else '✗ disabled (pip install numpy)'}")
    print("=" * 65)

    conn = get_connection(autocommit=True)
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
    print("=" * 65)

    processes = []
    for i in range(NUM_WORKERS):
        p = multiprocessing.Process(
            target=blast_transactions,
            args=(i, BATCH_SIZE, TARGET_SIZE_GB,
                  OP_INSERT_PCT, OP_UPDATE_PCT, OP_DELETE_PCT)
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    conn = get_connection(autocommit=True)
    cur  = conn.cursor()
    final_size = get_db_size_gb(cur)
    print(f"\n{'=' * 65}")
    print(f"DONE!  {initial_size:.2f} GB  →  {final_size:.2f} GB")
    print(f"{'=' * 65}")
    cur.close()
    conn.close()
